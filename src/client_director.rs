//! The edge-choosing code of a UDP listener (B-listener.md 3.5, 3.8, 3.9),
//! shared by `needletail-udp-client` and `needletail-load`.
//!
//! A listener holds up to two legs (subscriptions at two edges of its
//! region) and a spare (a token at a third edge, nothing else there). The
//! [`Director`] decides which edges: it asks the entry points for an
//! assignment (`ASSIGN` over UDP from the listener's own media socket, HTTPS
//! as a hedge), adds and releases legs, holds the spare and hedges to it,
//! answers `MOVE` by its reason, keeps the skip set and fails over to another
//! region. It performs no I/O: it hands out [`Action`]s (datagrams to send,
//! HTTPS requests to run, host names to resolve) and takes their answers.
//!
//! The legs themselves (requests, tokens, parts, loss, the verdicts on a
//! silent leg) are the session's. The director drives the session through
//! the [`SessionLegs`] trait and learns what happened to each leg from its
//! [`LegEvent`]s. Its invariant: a leg that still delivers is released only
//! when its replacement is level with it (or steady for
//! [`DirectorConfig::level_wait`] after a `MOVE` that is not a rebalance), or
//! when another leg delivers and no replacement exists. Otherwise only the
//! edge's grace cuts a delivering leg.
//!
//! The caller's loop:
//! - each datagram first goes to [`Director::on_datagram`]; one that it does
//!   not take (it returns `false`) goes to the session;
//! - after each datagram and at each [`Director::next_wake`] (and the
//!   session's own wake), the session is polled and then
//!   [`Director::poll`], which reads the session's events;
//! - the actions in `out` are performed: [`Action::Send`] from the media
//!   socket, [`Action::Https`] answered with [`Director::on_https`],
//!   [`Action::Resolve`] with [`Director::on_resolved`].

use crate::client_udp::{
    AssignedCache, Control, RepairPolicy, ShortGrant, CONTROL_PREFIX, MAX_PATHS,
};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

/// Legs a session holds at most: two wanted, a replacement or a hedge each.
pub const MAX_LEGS: usize = 4;
/// A node whose leg failed by itself (dead beside another leg, `REFUSED`,
/// no answer) is not taken again for this long (B:57).
pub const SKIP_FAILED: Duration = Duration::from_secs(30);
/// A node whose leg got `MOVE` is skipped until this long after the leg's
/// release: one freshness window of the advertisements (M:19562).
pub const SKIP_AFTER_MOVE: Duration = Duration::from_secs(3);
/// A node that sent no `TOKEN` within the token wait is skipped this long.
pub const SKIP_NO_TOKEN: Duration = Duration::from_secs(3);
/// A node that refused a leg for a reason that passes in seconds (its
/// load: `edge-busy`, `edge-full`; or a rebalance's `MOVE` before the
/// `OK`) is skipped this long (the lead's decision on B Y1/X5; a drain or
/// lost parents keeps [`SKIP_FAILED`]).
pub const SKIP_BUSY: Duration = Duration::from_secs(3);
/// The spare's token is fetched again this often; the renewal is also the
/// spare's liveness probe.
pub const SPARE_RENEW: Duration = Duration::from_secs(15);
/// Renewals of the spare's token unanswered in a row before it is dropped.
pub const SPARE_MISSES: u32 = 2;
/// The confirmation of an assignment goes again after this long.
pub const CONFIRM_REPEAT: Duration = Duration::from_millis(30);
/// A request beside delivering legs with nothing leaving (a second leg
/// later, a new spare) waits up to this long, by a hash of the session.
pub const ASK_SPREAD: Duration = Duration::from_millis(250);
/// A hedge waits up to this long, by a hash of the session, so that a
/// stalled edge's listeners do not reach their spares in one instant.
pub const HEDGE_SPREAD: Duration = Duration::from_millis(20);
/// A one-leg join takes the spare's edge when its `TOKEN` comes this much
/// sooner than the leg's.
pub const SWAP_WINDOW: Duration = Duration::from_millis(10);
/// An entry point that failed is asked beside the next one for this long.
pub const ENTRY_FAILED_FOR: Duration = Duration::from_secs(10);
/// Assignment failures in a row, with no leg, before a failover region.
pub const FAILOVER_AFTER_FAILURES: u32 = 2;
/// Away from the home region this long, the home entry points are asked
/// every [`HOME_PROBE_EVERY`]; the listener goes back once they name a cache.
pub const HOME_HOLD: Duration = Duration::from_secs(30);
pub const HOME_PROBE_EVERY: Duration = Duration::from_secs(10);
/// A failed request is asked again after this long with a delivering leg,
/// and after [`RETRY_ALONE`] without one.
pub const RETRY_WITH_LEG: Duration = Duration::from_secs(1);
pub const RETRY_ALONE: Duration = Duration::from_millis(200);
/// An unanswered `ASSIGN` is repeated at most this far apart.
pub const REQUEST_REPEAT_MAX: Duration = Duration::from_millis(300);
/// Legs missing for want of an edge are asked for again after these waits,
/// then every last one.
const SHORT_BACKOFF: [Duration; 4] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(5),
];
/// The same for a two-path listener left on one path (fix 4, F6b): a
/// drain's movers, a rebalance or the entry's charges fading make room
/// within a second or two, and every second asked later is a second on one
/// path (lp2c6-g-edges: 3 to 4.1 s on one path, the backoff's 1 s and 2 s).
const ONE_PATH_BACKOFF: [Duration; 5] = [
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(5),
];
/// How often a handover, a hedge or an extra leg is looked at.
const CHECK_EVERY: Duration = Duration::from_millis(5);
/// An answered request is kept this long for late answers of other entries.
const ANSWERED_KEPT: Duration = Duration::from_secs(1);
/// The reason of a `MOVE` that sends the listener to a failover region.
pub const REGION_LOST: &str = "region-lost";
/// The reason of the legs left when the listener returns home.
pub const HOME_REGION: &str = "home-region";
const REBALANCE: &str = "rebalance";
const PARENTS_LOST: &str = "parents-lost";
/// The reason of a hedge leg's move to the edge the entry apportioned its
/// place to (the lead's decision after integration 2): the hedge's edge is
/// the spare again once the new leg is level.
pub const APPORTION: &str = "apportion";
/// Refusals for load, which passes in seconds.
const BUSY_REFUSALS: [&str; 2] = ["edge-busy", "edge-full"];
/// Place240: the reasons of a move the listener makes itself (rule 3) or
/// that its edge's distress asks for (rule 4).
pub const LATE_PARTS: &str = "late-parts";
pub const MISSING_PARTS: &str = "missing-parts";
/// Defaults of `--quality-hold-off-ms` (a node left for late or missing
/// parts is not taken again for this long), `--quality-moves-per-minute`,
/// `--quality-no-better-ms` and `--quality-giveup-ms`.
pub const DEFAULT_QUALITY_HOLD_OFF_MS: u64 = 30_000;
pub const DEFAULT_QUALITY_MOVES_PER_MINUTE: u32 = 2;
pub const DEFAULT_QUALITY_NO_BETTER_MS: u64 = 10_000;
pub const DEFAULT_QUALITY_GIVEUP_MS: u64 = 3_000;
/// A listener with two legs moves for distress only when every leg's edge
/// said so within this long.
pub const DISTRESS_TOGETHER: Duration = Duration::from_secs(2);

/// Whether a leaving reason is one of place240's own moves.
fn quality_reason(reason: &str) -> bool {
    reason == LATE_PARTS
        || reason == MISSING_PARTS
        || reason.starts_with(crate::client_udp::DISTRESS_MOVE_PREFIX)
}

pub const DEFAULT_PATHS: u8 = 2;
pub const DEFAULT_TOKEN_MS: u64 = 400;
pub const DEFAULT_REBALANCE_GIVEUP_MS: u64 = 3_000;
pub const DEFAULT_LEVEL_WAIT_MS: u64 = 1_000;
pub const DEFAULT_ASSIGNMENT_HTTPS_AFTER_MS: u64 = 50;
pub const DEFAULT_ASSIGNMENT_TIMEOUT_MS: u64 = 1_000;

/// A leg of the session, named by the director. Keys are never reused in
/// one director's life.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LegKey(pub u32);

/// Where a new leg asks its edge to start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartFrom {
    /// The newest part delivered + 1 (another leg delivers: the holes stay
    /// with the legs that sent the parts). With no part yet, the edge's head.
    AfterNewest,
    /// The oldest open hole at most 4,096 parts back, else the newest part
    /// + 1 (no leg delivers). With no part yet, the edge's head.
    OldestHole,
}

/// How a leg starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LegStart {
    pub from: StartFrom,
    /// A token the edge gave this listener already (the spare's): the first
    /// `SUB` carries it. `None`: a tokenless padded `SUB` first.
    pub token: Option<u64>,
    /// A leg with no `TOKEN` after this long is passed over
    /// ([`LegEvent::NoToken`]): `--token-ms`, or four round trips to the
    /// entry point when that is longer.
    pub token_wait: Duration,
}

/// What the session knows of one leg now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LegView {
    /// The leg brought a part (a first arrival or a duplicate) within the
    /// last max(20 ms, 8 part intervals): a leg a fraction of a millisecond
    /// behind the other delivers too.
    pub delivering: bool,
    /// The leg brought at least 28 of the session's last 32 delivered parts.
    pub steady: bool,
    /// The leg's first part (the part its `OK` named, else its first part).
    pub first_part: Option<u64>,
    /// When the leg's first media datagram came.
    pub first_media: Option<Instant>,
    /// When the leg's last media datagram came.
    pub last_media: Option<Instant>,
    /// When the leg's last datagram of any kind came.
    pub last_datagram: Option<Instant>,
    /// The leg's median lag behind the first arrival over the last 32
    /// delivered parts (zero when it was first).
    pub lag: Option<Duration>,
}

/// Why the session gave a leg up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegFailure {
    /// No datagram for `--silence-ms` while another leg sent one, or no
    /// media for that long while another leg delivered.
    Dead,
    /// No `OK` within `--answer-ms` of the leg's first request.
    NoAnswer,
    /// The leg's `OK` named a part more than 4,096 behind the session's
    /// newest, or its parts were shown that far behind (`stale_start`).
    Stale,
    /// The receiver refused every datagram of the leg for a second, at the
    /// stream's rate (an old generation, parts before the session's start),
    /// or, on the RaptorQ lane, took no part of them (stuck4k).
    Refusing,
    /// No media for a second while the leg's edge said that the stream went
    /// on (its `WAIT`s' heads moved), and no other leg delivered (stuck4k).
    Stalled,
}

impl LegFailure {
    pub fn reason(self) -> &'static str {
        match self {
            Self::Dead => "silence",
            Self::NoAnswer => "no-answer",
            Self::Stale => "stale-start",
            Self::Refusing => "refused-1s",
            Self::Stalled => "stalled-1s",
        }
    }
}

/// What happened to a leg (or to all of them), as the session reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LegEvent {
    /// The leg's edge gave its token.
    Tokened(LegKey),
    /// The leg's `OK` and its first media came; `compact`: the `OK` said `C`.
    Live { leg: LegKey, compact: bool },
    /// The edge sent `MOVE` on this leg: reported on the first `MOVE`, and
    /// again when the reason changes (repeats of one reason need not be).
    Moved { leg: LegKey, reason: String },
    /// The edge refused the leg (`REFUSED`).
    Refused { leg: LegKey, reason: String },
    /// No `TOKEN` within the leg's token wait. The leg goes on asking.
    NoToken(LegKey),
    /// The leg is silent: no media for `--hedge-after-ms` while another leg
    /// delivered a first arrival in the last 20 ms, or it is the only leg and
    /// no datagram came for `--lone-hedge-after-ms`. Once per silence.
    HedgeDue(LegKey),
    /// The session gave the leg up. It stays until the director releases it.
    Failed { leg: LegKey, cause: LegFailure },
    /// The edge answered a take-back ([`SessionLegs::takeback_leg`]) with `OK`.
    TakenBack(LegKey),
    /// No leg sent a datagram for `--silence-ms`; repeated every
    /// `--silence-repeat-ms` while that lasts. No leg is failed for it.
    AllSilent,
    /// 8 of the last 16 parts came as RaptorQ objects (a stream over the
    /// two-path bit rate): once.
    ObjectLane,
    /// Place240 rule 3: the lone leg delivers parts late or misses them
    /// (`late-parts`, `missing-parts`): the leg still delivers, and the
    /// director moves it make-before-break.
    Degraded { leg: LegKey, reason: String },
    /// Place240 rule 4: the leg's edge says it is in distress (`MOVE
    /// distress-<why>`); it keeps serving the leg.
    Distress { leg: LegKey, reason: String },
}

/// The surface of the session that the director drives (B 3.1, 3.2). Every
/// method that may send takes the caller's `send`, as `UdpSession` does.
/// The session judges its legs (tokens, answers, silence, staleness) and
/// reports with [`LegEvent`]s; it never adds, releases or replaces a leg of
/// its own accord.
pub trait SessionLegs {
    /// Open leg `leg` at the edge `node` (client UDP address `addr`) and send
    /// its first request at once: a tokenless padded `SUB` (doubled after
    /// `--first-double-ms`), or with `start.token`, the `SUB` with that token
    /// (doubled likewise). Every `SUB` of the leg, refreshes included,
    /// carries `o=` with the nodes of the session's other delivering legs.
    /// The director never holds more than [`MAX_LEGS`] legs. A leg added at
    /// an address released less than 10 s ago (a hedge back at the spare,
    /// the swap of a one-leg join) ends that address's `UNSUB` answers: its
    /// datagrams are the new leg's.
    fn add_leg(
        &mut self,
        leg: LegKey,
        node: &str,
        addr: SocketAddr,
        start: LegStart,
        now: Instant,
        send: &mut dyn FnMut(SocketAddr, &[u8]),
    );

    /// Close the leg: `UNSUB` twice 20 ms apart when it holds a token, and
    /// nothing more is delivered from it. For 10 s a datagram from its
    /// address is answered with `UNSUB` (at most one per 100 ms). Its holes
    /// stay the session's. The key is never used again.
    fn release_leg(&mut self, leg: LegKey, now: Instant, send: &mut dyn FnMut(SocketAddr, &[u8]));

    /// Ask the leg's edge to keep the listener after a `MOVE`: `SUB <newest
    /// + 1> <token>` (edge rule 5). A `MOVE` within one round trip of it and
    /// before its `OK` is ignored; the `OK` is reported as
    /// [`LegEvent::TakenBack`]. A `MOVE` after that is a new `MOVE`.
    fn takeback_leg(&mut self, leg: LegKey, now: Instant, send: &mut dyn FnMut(SocketAddr, &[u8]));

    /// Every leg was silent: ask the leg's edge again from the oldest hole
    /// (`SUB <part> <token> r=1`, edge rule 8; the newest + 1 when there is
    /// no hole).
    fn reask_leg(&mut self, leg: LegKey, now: Instant, send: &mut dyn FnMut(SocketAddr, &[u8]));

    /// The leg as the session knows it now; `None` for a key it does not
    /// hold.
    fn leg(&self, leg: LegKey, now: Instant) -> Option<LegView>;

    /// `new` is level with `old`: `new` is steady and its median lag over
    /// the last 32 delivered parts is at most `old`'s + 250 µs (steady is
    /// enough when `old` brought fewer than 16 of them).
    fn level(&self, new: LegKey, old: LegKey) -> bool;

    /// The oldest part still missing that the session would ask for.
    fn oldest_hole(&self) -> Option<u64>;

    /// The next event, oldest first.
    fn next_event(&mut self) -> Option<LegEvent>;
}

/// What the caller must do for the director.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Send this datagram from the listener's media socket.
    Send { to: SocketAddr, datagram: Vec<u8> },
    /// `GET <entry><path>` with the CMCD session header; give the body (or
    /// `None` on any failure) to [`Director::on_https`] with `id`.
    Https {
        id: u64,
        entry: String,
        path: String,
    },
    /// Resolve `host` to an address; give it to [`Director::on_resolved`].
    Resolve { host: String },
}

/// The director's settings: the flags of B 2.4 that are its own.
#[derive(Debug, Clone)]
pub struct DirectorConfig {
    pub stream_id: u64,
    /// The session id (CMCD `sid`).
    pub session: String,
    /// The home region's entry points, `https://host:port`.
    pub entries: Vec<String>,
    /// `--paths`: legs held on a stream the entry grants two for.
    pub paths: u8,
    /// `--spare`: hold a spare and hedge to it.
    pub spare: bool,
    /// `--token-ms`.
    pub token_wait: Duration,
    /// `--rebalance-giveup-ms`.
    pub rebalance_giveup: Duration,
    /// `--level-wait-ms`.
    pub level_wait: Duration,
    /// `--first-retry-ms`: the first repeat of an unanswered `ASSIGN` (three
    /// round trips to the entry once measured), and when the next entry is
    /// asked too. 0: every 300 ms, and the next entry never early.
    pub first_retry: Duration,
    /// `--assignment-udp-port`: the entry points' client UDP port. 0: HTTPS
    /// alone.
    pub assignment_udp_port: u16,
    /// `--assignment-https-after-ms`.
    pub assignment_https_after: Duration,
    /// `--assignment-timeout-ms`: the whole budget of one request.
    pub assignment_timeout: Duration,
    /// `--udp-port`: the caches' client UDP port, over what an answer names.
    pub udp_port: Option<u16>,
    /// Seed of the requests' nonces (the caller's randomness).
    pub nonce_seed: u64,
    /// Place240 rule 3 (`--quality-*`): a node left for late or missing
    /// parts, or for its distress, is skipped this long; such moves are
    /// made at most this many a minute; a degradation within `no_better`
    /// of the last such move is not moved for (the cause is not the edge:
    /// upstream, or the client's own path), and holds such moves off for
    /// `hold_off`; a move whose replacement brought nothing within
    /// `giveup` is given up and the old leg kept.
    pub quality_hold_off: Duration,
    pub quality_moves_per_minute: u32,
    pub quality_no_better: Duration,
    pub quality_giveup: Duration,
}

impl DirectorConfig {
    /// The defaults of the clients' flags.
    pub fn new(stream_id: u64, session: impl Into<String>, entries: Vec<String>) -> Self {
        Self {
            stream_id,
            session: session.into(),
            entries,
            paths: DEFAULT_PATHS,
            spare: true,
            token_wait: Duration::from_millis(DEFAULT_TOKEN_MS),
            rebalance_giveup: Duration::from_millis(DEFAULT_REBALANCE_GIVEUP_MS),
            level_wait: Duration::from_millis(DEFAULT_LEVEL_WAIT_MS),
            first_retry: Duration::from_millis(crate::client_session::DEFAULT_FIRST_RETRY_MS),
            assignment_udp_port: crate::client_udp::DEFAULT_CLIENT_UDP_PORT,
            assignment_https_after: Duration::from_millis(DEFAULT_ASSIGNMENT_HTTPS_AFTER_MS),
            assignment_timeout: Duration::from_millis(DEFAULT_ASSIGNMENT_TIMEOUT_MS),
            udp_port: None,
            nonce_seed: 0,
            quality_hold_off: Duration::from_millis(DEFAULT_QUALITY_HOLD_OFF_MS),
            quality_moves_per_minute: DEFAULT_QUALITY_MOVES_PER_MINUTE,
            quality_no_better: Duration::from_millis(DEFAULT_QUALITY_NO_BETTER_MS),
            quality_giveup: Duration::from_millis(DEFAULT_QUALITY_GIVEUP_MS),
        }
    }
}

/// How a leg came to replace another (B 2.8 `replacements`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplacementKind {
    /// A planned move, released when the new leg was level.
    Handover,
    /// The spare, hedged beside a live leg, took a dead leg's place.
    Hedge,
    /// A new leg through the entry beside a live leg.
    Replace,
    /// A new leg when no leg delivered.
    Interrupt,
    /// The edge was asked to keep the listener after all.
    Takeback,
}

impl ReplacementKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Handover => "handover",
            Self::Hedge => "hedge",
            Self::Replace => "replace",
            Self::Interrupt => "interrupt",
            Self::Takeback => "takeback",
        }
    }
}

/// One leg replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replacement {
    pub at: Instant,
    pub from: String,
    pub to: String,
    pub reason: String,
    pub kind: ReplacementKind,
    /// `MOVE` to the release of the old leg.
    pub handover: Option<Duration>,
    /// The old leg's last media to the new leg's first.
    pub exposure: Option<Duration>,
}

/// How an assignment came.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Via {
    Udp,
    Https,
}

impl Via {
    pub fn name(self) -> &'static str {
        match self {
            Self::Udp => "udp",
            Self::Https => "https",
        }
    }
}

/// Answers that granted fewer legs than asked, by the entry's reason
/// (`why=`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShortGrants {
    pub one_edge: u64,
    pub bitrate: u64,
    pub gate: u64,
    /// No reason given (an entry that names none).
    pub unexplained: u64,
}

impl ShortGrants {
    fn count(&mut self, why: Option<ShortGrant>) {
        let counter = match why {
            Some(ShortGrant::OneEdge) => &mut self.one_edge,
            Some(ShortGrant::Bitrate) => &mut self.bitrate,
            Some(ShortGrant::Gate) => &mut self.gate,
            None => &mut self.unexplained,
        };
        *counter += 1;
    }
}

/// Place240: the listener's own moves by reason, and those it did not make.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QualityMoves {
    pub late_parts: u64,
    pub missing_parts: u64,
    /// Its edge's distress (a one-leg listener, or every leg's edge).
    pub distress: u64,
    /// A degradation soon after such a move (the new edge no better), or
    /// in the hold-off that followed one: not moved for.
    pub no_better: u64,
    /// Over `--quality-moves-per-minute`: not moved for.
    pub over_budget: u64,
    /// The replacement brought nothing within `--quality-giveup-ms`: the
    /// old leg kept.
    pub given_up: u64,
    /// Distress hints of one leg beside another that delivers: ignored.
    pub distress_ignored: u64,
}

impl QualityMoves {
    /// The moves made, by reason, for a report.
    pub fn moves(&self) -> u64 {
        self.late_parts + self.missing_parts + self.distress
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DirectorStats {
    /// Requests made, and answers taken by each way.
    pub requests: u64,
    pub answers_udp: u64,
    pub answers_https: u64,
    /// Requests that no entry answered within the budget.
    pub assignment_failures: u64,
    pub hedges: u64,
    pub hedges_released: u64,
    /// Spares dropped for unanswered renewals.
    pub spares_dropped: u64,
    /// Spares the entry replaced when asked with the one held (`k=`): its
    /// edge was over the redundant leg's gate.
    pub spares_rechosen: u64,
    /// Spares whose edge answered their token's renewal with `MOVE` (it
    /// drains): dropped, and another asked for.
    pub spares_moved: u64,
    /// Hedge legs moved to the edge the entry apportioned their place to.
    pub apportion_moves: u64,
    pub failovers: u64,
    pub home_returns: u64,
    pub takebacks: u64,
    pub short_grants: ShortGrants,
    /// Place240 rules 3 and 4.
    pub quality: QualityMoves,
}

/// A leg as the director holds it.
#[derive(Debug, Clone)]
pub struct DirectorLeg {
    pub key: LegKey,
    pub node: String,
    pub addr: SocketAddr,
    pub added_at: Instant,
    pub tokened: bool,
    pub live: bool,
    /// A hedge at the spare beside the leg it covers.
    pub hedge_for: Option<LegKey>,
    /// The `MOVE` it got: reason and when.
    pub leaving: Option<(String, Instant)>,
    /// Passed over for want of a token, since when.
    pub passed_over: Option<Instant>,
    /// The leg this one was added for: a leg leaving or passed over. Only
    /// it releases that leg (B 3.5 steps 5 and 6), and a rebalance's give-up
    /// releases only it.
    pub stands_for: Option<LegKey>,
    compact: bool,
    steady_since: Option<Instant>,
    /// Every leg went silent then: the leg is not steady again until it
    /// has brought media after it.
    quiet_since: Option<Instant>,
    /// The spare this hedge leg was, given back when it is released.
    spare: Option<Spare>,
    /// The leg this one replaces, booked at its first media.
    replaces: Option<Pending>,
    /// A hedge that took a dead leg's place by the listener's spare, not
    /// by the entry's apportionment: the entry is asked for the place as
    /// for a mover (the lead's decision after integration 2).
    unplaced: bool,
}

impl DirectorLeg {
    fn counted(&self) -> bool {
        self.hedge_for.is_none() && self.leaving.is_none() && self.passed_over.is_none()
    }

    fn leaving_for(&self, reason: &str) -> bool {
        self.leaving.as_ref().is_some_and(|(why, _)| why == reason)
    }

    fn rebalance(&self) -> bool {
        self.leaving_for(REBALANCE)
    }

    /// A leg whose place another leg is added for: leaving or passed over,
    /// and not a hedge (a hedge holds no place of its own).
    fn wants_replacement(&self) -> bool {
        self.hedge_for.is_none() && (self.leaving.is_some() || self.passed_over.is_some())
    }
}

#[derive(Debug, Clone)]
struct Pending {
    from: String,
    reason: String,
    kind: ReplacementKind,
    last_media: Option<Instant>,
}

#[derive(Debug, Clone)]
struct Spare {
    node: String,
    addr: SocketAddr,
    token: Option<u64>,
    /// A tokenless `SUB` is out since then.
    asked_at: Option<Instant>,
    misses: u32,
    next_renew: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SkipCause {
    Moved,
    Failed,
    NoToken,
    /// Refused for load, or sent away for a rebalance before its `OK`.
    Busy,
    /// Left for late or missing parts, or its distress (place240): for
    /// `--quality-hold-off-ms`.
    Quality,
}

#[derive(Debug, Clone, Copy)]
struct Skip {
    since: Instant,
    /// `None`: until the leg that got `MOVE` there is released.
    until: Option<Instant>,
    cause: SkipCause,
}

#[derive(Debug, Clone)]
struct Entry {
    url: String,
    host: String,
    ip: Option<IpAddr>,
    rtt: Option<Duration>,
    failed_until: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestKind {
    Normal,
    /// Every leg was silent: legs beside the silent ones.
    Silence,
    /// Away from home: does the home region name a cache?
    HomeProbe,
}

#[derive(Debug, Clone)]
struct Request {
    nonce: u64,
    kind: RequestKind,
    started: Instant,
    deadline: Instant,
    datagram: Vec<u8>,
    paths: u8,
    spare: bool,
    skip: Vec<String>,
    hold: Vec<String>,
    /// The entries it may ask, in order; the first `asked` were asked.
    pool: Vec<usize>,
    asked: usize,
    repeats: u32,
    next_repeat: Instant,
    next_entry_at: Option<Instant>,
    https_at: Option<Instant>,
    /// Tokens the entries gave for this socket, and the entries that
    /// answered over UDP.
    tokens: Vec<(usize, u64)>,
    answered_by: Vec<usize>,
    /// The spare it named as held (`k=`): an answer that names another
    /// spare replaces it.
    spare_held: Option<String>,
    /// The hedge leg whose place it asks for as a mover.
    place_of: Option<LegKey>,
}

#[derive(Debug, Clone)]
struct AnswerCache {
    node: String,
    /// Its playback base URL (for `--verify-http`).
    url: String,
    host: String,
    port: Option<u16>,
    failover: Option<String>,
}

#[derive(Debug, Clone)]
struct Answer {
    port: Option<u16>,
    caches: Vec<AnswerCache>,
    paths: u8,
    spare: bool,
    why: Option<ShortGrant>,
}

/// The end of one leaving leg's move.
#[derive(Debug, Clone, Copy)]
enum Handover {
    /// `new`, added for `old`, takes over: `old` is released.
    Release {
        old: LegKey,
        new: LegKey,
        moved_at: Instant,
    },
    /// A rebalance not level by the give-up: `old` is taken back.
    GiveUp { old: LegKey },
    /// Place240: a move of the listener's own whose replacement brought
    /// nothing by the give-up: the old leg is kept (its edge never sent it
    /// away), the replacements released.
    Keep { old: LegKey },
}

#[derive(Debug, Clone)]
struct Silence {
    at: Instant,
    check_at: Instant,
    legs: Vec<LegKey>,
    asked: bool,
    /// Legs that took a silent leg's place (one record each).
    placed: Vec<LegKey>,
    /// When a silent leg first brought media again (as `check_legs` saw
    /// it).
    resumed: Option<Instant>,
}

/// The edge-choosing state of one listener. See the module documentation.
#[derive(Debug)]
pub struct Director {
    pub config: DirectorConfig,
    pub stats: DirectorStats,
    /// Every leg replaced, in order.
    pub replacements: Vec<Replacement>,
    /// When the first assignment came, and how.
    pub first_answer: Option<(Instant, Via)>,
    legs: Vec<DirectorLeg>,
    next_key: u32,
    spare: Option<Spare>,
    skip: HashMap<String, Skip>,
    /// Every entry point known (home ones first), the ones asked now, and
    /// the home ones.
    entries: Vec<Entry>,
    current: Vec<usize>,
    home: Vec<usize>,
    /// The failover entry points of the last answer.
    failover: Vec<String>,
    away_since: Option<Instant>,
    next_home_probe: Instant,
    want: u8,
    /// The stream came as RaptorQ objects: one leg, released at the first
    /// object of its replacement.
    objects: bool,
    ever_live: bool,
    request: Option<Request>,
    answered: Option<(Request, Instant)>,
    pending_answer: Option<(Answer, Option<usize>, Via)>,
    nonces: u64,
    /// Assignment failures in a row with no leg held.
    failures: u32,
    retry_at: Option<Instant>,
    /// The last `MOVE`, failure or pass-over that left a place to fill: an
    /// answer to a request sent before it says nothing of that place.
    changed_at: Instant,
    short_answers: usize,
    /// A planned move released a leg for want of a cache: until the
    /// listener holds its legs again, the missing one is asked for on
    /// [`ONE_PATH_BACKOFF`] (fix 4, F6b).
    lost_place: bool,
    deferred_ask: Option<Instant>,
    delayed: Vec<(Instant, SocketAddr, Vec<u8>)>,
    hedge_due: Option<(Instant, LegKey)>,
    swap_at: Option<Instant>,
    silence: Option<Silence>,
    pending: Vec<Pending>,
    resolved: HashMap<String, Option<IpAddr>>,
    /// The playback base URL of each cache an answer named.
    urls: HashMap<String, String>,
    next_check: Instant,
    wake: Instant,
    /// Place240: the listener's own moves of the last minute, the last
    /// one, the hold-off after a move that was no better, and the distress
    /// hints of each leg.
    quality_moves: Vec<Instant>,
    last_quality_move: Option<Instant>,
    quality_quiet_until: Option<Instant>,
    distress_seen: Vec<(LegKey, Instant)>,
}

fn sender(out: &mut Vec<Action>) -> impl FnMut(SocketAddr, &[u8]) + '_ {
    |to, datagram| {
        out.push(Action::Send {
            to,
            datagram: datagram.to_vec(),
        })
    }
}

/// FNV-1a of the session id: the spreads are fixed per session.
fn session_hash(session: &str) -> u64 {
    session.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// A fixed share of `max` (0 to `max`, in µs) for the session.
fn spread(session: &str, salt: u64, max: Duration) -> Duration {
    let hash = session_hash(session) ^ salt.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let hash = (hash ^ (hash >> 29)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    Duration::from_micros((hash >> 11) % (max.as_micros() as u64 + 1))
}

/// The host of `https://host[:port][/path]` (or `http://`, which the mesh
/// takes too).
fn url_host(url: &str) -> Option<String> {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))?;
    let authority = rest.split('/').next()?;
    let host = if let Some(inner) = authority.strip_prefix('[') {
        inner.split(']').next()?
    } else {
        match authority.rsplit_once(':') {
            Some((host, port)) if port.parse::<u16>().is_ok() => host,
            _ => authority,
        }
    };
    (!host.is_empty()).then(|| host.to_owned())
}

/// The entry point URL without a trailing slash.
fn entry_url(url: &str) -> String {
    url.trim_end_matches('/').to_owned()
}

impl Director {
    pub fn new(config: DirectorConfig, now: Instant) -> Self {
        let mut director = Self {
            want: config.paths.clamp(1, MAX_PATHS),
            config,
            stats: DirectorStats::default(),
            replacements: Vec::new(),
            first_answer: None,
            legs: Vec::new(),
            next_key: 0,
            spare: None,
            skip: HashMap::new(),
            entries: Vec::new(),
            current: Vec::new(),
            home: Vec::new(),
            failover: Vec::new(),
            away_since: None,
            next_home_probe: now,
            objects: false,
            ever_live: false,
            request: None,
            answered: None,
            pending_answer: None,
            nonces: 0,
            failures: 0,
            retry_at: None,
            changed_at: now,
            short_answers: 0,
            lost_place: false,
            deferred_ask: None,
            delayed: Vec::new(),
            hedge_due: None,
            swap_at: None,
            silence: None,
            pending: Vec::new(),
            resolved: HashMap::new(),
            urls: HashMap::new(),
            next_check: now,
            wake: now,
            quality_moves: Vec::new(),
            last_quality_move: None,
            quality_quiet_until: None,
            distress_seen: Vec::new(),
        };
        let urls = director.config.entries.clone();
        director.home = urls.iter().map(|url| director.entry_index(url)).collect();
        director.current = director.home.clone();
        director
    }

    /// The legs held now.
    pub fn legs(&self) -> &[DirectorLeg] {
        &self.legs
    }

    /// The spare's node, if one is held.
    pub fn spare(&self) -> Option<&str> {
        self.spare.as_ref().map(|spare| spare.node.as_str())
    }

    /// Legs wanted now.
    pub fn want(&self) -> u8 {
        self.want
    }

    /// The nodes skipped now.
    pub fn skipped(&self, now: Instant) -> Vec<String> {
        let mut nodes: Vec<String> = self
            .skip
            .iter()
            .filter(|(_, skip)| skip.until.is_none_or(|until| now < until))
            .map(|(node, _)| node.clone())
            .collect();
        nodes.sort();
        nodes
    }

    /// The skip set with each node's cause and time left (`None`: until
    /// its leg is released), for a report or a test's message.
    pub fn skip_detail(&self, now: Instant) -> Vec<(String, String, Option<Duration>)> {
        let mut nodes: Vec<(String, String, Option<Duration>)> = self
            .skip
            .iter()
            .filter(|(_, skip)| skip.until.is_none_or(|until| now < until))
            .map(|(node, skip)| {
                (
                    node.clone(),
                    format!("{:?}", skip.cause),
                    skip.until.map(|until| until.saturating_duration_since(now)),
                )
            })
            .collect();
        nodes.sort();
        nodes
    }

    /// Away from the home region, since when.
    pub fn away_since(&self) -> Option<Instant> {
        self.away_since
    }

    /// The playback base URL of a cache an answer named.
    pub fn cache_url(&self, node: &str) -> Option<&str> {
        self.urls.get(node).map(String::as_str)
    }

    /// When [`Self::poll`] must run next (at the latest).
    pub fn next_wake(&self) -> Instant {
        self.wake
    }

    fn entry_index(&mut self, url: &str) -> usize {
        let url = entry_url(url);
        if let Some(index) = self.entries.iter().position(|entry| entry.url == url) {
            return index;
        }
        let host = url_host(&url).unwrap_or_default();
        let ip = host.parse::<IpAddr>().ok();
        self.entries.push(Entry {
            url,
            host,
            ip,
            rtt: None,
            failed_until: None,
        });
        self.entries.len() - 1
    }

    fn entry_addr(&self, index: usize) -> Option<SocketAddr> {
        let entry = &self.entries[index];
        (self.config.assignment_udp_port != 0)
            .then_some(())
            .and(entry.ip)
            .map(|ip| SocketAddr::new(ip, self.config.assignment_udp_port))
    }

    fn entry_at(&self, peer: SocketAddr) -> Option<usize> {
        (0..self.entries.len()).find(|index| self.entry_addr(*index) == Some(peer))
    }

    fn leg_index(&self, key: LegKey) -> Option<usize> {
        self.legs.iter().position(|leg| leg.key == key)
    }

    fn view<S: SessionLegs>(legs: &S, key: LegKey, now: Instant) -> LegView {
        legs.leg(key, now).unwrap_or_default()
    }

    fn any_delivering<S: SessionLegs>(
        &self,
        legs: &S,
        now: Instant,
        except: Option<LegKey>,
    ) -> bool {
        self.legs
            .iter()
            .filter(|leg| Some(leg.key) != except)
            .any(|leg| Self::view(legs, leg.key, now).delivering)
    }

    fn held(&self) -> usize {
        self.legs.iter().filter(|leg| leg.counted()).count()
    }

    /// Legs asked for by a listener that holds none: `--paths`, or one once
    /// the stream came as objects. Its answer sets [`Self::want`] again.
    fn full_paths(&self) -> u8 {
        if self.objects {
            1
        } else {
            self.config.paths.clamp(1, MAX_PATHS)
        }
    }

    /// Legs to ask for now (B 3.5 steps 2 and 8).
    fn missing(&self) -> u8 {
        let room = MAX_LEGS.saturating_sub(self.legs.len());
        let held = self.held();
        let wanted = if held == 0 {
            self.full_paths() as usize
        } else {
            (self.want as usize).saturating_sub(held)
        };
        wanted.min(room) as u8
    }

    /// A place to fill appeared (a `MOVE`, a failure, a pass-over): an
    /// answer to a request sent before now does not hold its request back.
    fn place_opened(&mut self, now: Instant) {
        self.retry_at = None;
        self.changed_at = now;
    }

    /// Whether some leg was added for `key`.
    fn replaced(&self, key: LegKey) -> bool {
        self.legs.iter().any(|leg| leg.stands_for == Some(key))
    }

    /// The leg a new leg is added for: the leg leaving longest (then by
    /// key) that has no replacement yet, else a leg passed over that has
    /// none.
    fn open_place(&self) -> Option<LegKey> {
        let leaving = self
            .legs
            .iter()
            .filter(|leg| leg.wants_replacement() && !self.replaced(leg.key))
            .filter_map(|leg| leg.leaving.as_ref().map(|(_, at)| (*at, leg.key)))
            .min()
            .map(|(_, key)| key);
        leaving.or_else(|| {
            self.legs
                .iter()
                .find(|leg| {
                    leg.wants_replacement() && leg.leaving.is_none() && !self.replaced(leg.key)
                })
                .map(|leg| leg.key)
        })
    }

    /// Places with no leg of their own and no leg leaving or passed over
    /// for them: what the records of failed legs ([`Pending`]) wait for.
    fn empty_places(&self) -> usize {
        let open = self
            .legs
            .iter()
            .filter(|leg| leg.wants_replacement() && !self.replaced(leg.key))
            .count();
        (self.want as usize).saturating_sub(self.held() + open)
    }

    fn skipped_now(&self, node: &str, now: Instant) -> bool {
        self.skip
            .get(node)
            .is_some_and(|skip| skip.until.is_none_or(|until| now < until))
    }

    fn skip_node(&mut self, node: &str, cause: SkipCause, now: Instant) {
        let until = match cause {
            SkipCause::Moved => None,
            SkipCause::Failed => Some(now + SKIP_FAILED),
            SkipCause::NoToken => Some(now + SKIP_NO_TOKEN),
            SkipCause::Busy => Some(now + SKIP_BUSY),
            SkipCause::Quality => Some(now + self.config.quality_hold_off),
        };
        // A failure always takes its 30 s. A move's skip gives way only to
        // a failure; a token's or a busy edge's skip never shortens a
        // longer one.
        if let Some(old) = self.skip.get(node) {
            let active = old.until.is_none_or(|end| now < end);
            let keep = match cause {
                SkipCause::Failed => false,
                SkipCause::Moved => old.cause == SkipCause::Failed,
                SkipCause::NoToken | SkipCause::Busy | SkipCause::Quality => old
                    .until
                    .is_none_or(|end| until.is_some_and(|new_end| end >= new_end)),
            };
            if active && keep {
                return;
            }
        }
        self.skip.insert(
            node.to_owned(),
            Skip {
                since: now,
                until,
                cause,
            },
        );
    }

    fn token_wait(&self) -> Duration {
        let rtt = self
            .current
            .iter()
            .filter_map(|index| self.entries[*index].rtt)
            .min()
            .unwrap_or_default();
        self.config.token_wait.max(rtt * 4)
    }

    /// No spare, none hedged, and none coming back: a leg leaving its
    /// edge for the entry's apportionment leaves the spare there.
    fn spare_wanted(&self) -> bool {
        self.config.spare
            && self.spare.is_none()
            && !self
                .legs
                .iter()
                .any(|leg| leg.hedge_for.is_some() || leg.leaving_for(APPORTION))
    }

    /// The hedge leg whose place the entry is to apportion: it took a dead
    /// leg's place, holds it still (counted), and no request asks for it.
    fn unplaced(&self) -> Option<LegKey> {
        self.legs
            .iter()
            .find(|leg| leg.unplaced && leg.counted())
            .map(|leg| leg.key)
    }

    fn next_nonce(&mut self) -> u64 {
        self.nonces += 1;
        let mut z = self
            .config
            .nonce_seed
            .wrapping_add(self.nonces.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Take a datagram that is the director's: an `ASSIGNED`, or a `TOKEN`
    /// from an entry point or the spare. Returns `false` for any other
    /// datagram (the session's). Media datagrams cost one prefix compare.
    pub fn on_datagram<S: SessionLegs>(
        &mut self,
        legs: &mut S,
        peer: SocketAddr,
        datagram: &[u8],
        now: Instant,
        out: &mut Vec<Action>,
    ) -> bool {
        let Some(rest) = datagram.strip_prefix(CONTROL_PREFIX.as_bytes()) else {
            return false;
        };
        let assigned = rest.starts_with(b"ASSIGNED ");
        // The spare's edge drains: it answered the renewal of the token
        // with `MOVE` (av-mesh-fix-3). The spare is dropped, its node
        // skipped as a moved leg's is, and another asked for.
        if rest.starts_with(b"MOVE ")
            && self.spare.as_ref().is_some_and(|spare| spare.addr == peer)
            && !self.legs.iter().any(|leg| leg.addr == peer)
        {
            if let Some(Control::Move { stream_id, .. }) = Control::decode(datagram) {
                if stream_id == self.config.stream_id {
                    if let Some(spare) = self.spare.take() {
                        self.skip.insert(
                            spare.node,
                            Skip {
                                since: now,
                                until: Some(now + SKIP_AFTER_MOVE),
                                cause: SkipCause::Moved,
                            },
                        );
                        self.stats.spares_moved += 1;
                    }
                    self.run(legs, now, out);
                    return true;
                }
            }
        }
        if !assigned && !rest.starts_with(b"TOKEN ") {
            return false;
        }
        match Control::decode(datagram) {
            Some(Control::Token { stream_id, token }) if stream_id == self.config.stream_id => {
                let mut taken = false;
                if self.spare.as_ref().is_some_and(|spare| spare.addr == peer) {
                    self.on_spare_token(token, now);
                    taken = true;
                }
                if let Some(entry) = self.entry_at(peer) {
                    for request in [
                        self.request.as_mut(),
                        self.answered.as_mut().map(|(request, _)| request),
                    ]
                    .into_iter()
                    .flatten()
                    {
                        if request.pool[..request.asked].contains(&entry) {
                            request.tokens.retain(|(index, _)| *index != entry);
                            request.tokens.push((entry, token));
                            taken = true;
                        }
                    }
                }
                // A leg's own edge may also be an entry: its session needs
                // the token too.
                let leg_peer = self.legs.iter().any(|leg| leg.addr == peer);
                let taken = taken && !leg_peer;
                if taken {
                    self.run(legs, now, out);
                }
                taken
            }
            Some(Control::Assigned {
                stream_id,
                nonce,
                port,
                caches,
                paths,
                spare,
                why,
            }) if stream_id == self.config.stream_id => {
                let entry = self.entry_at(peer);
                let answer = Answer {
                    port,
                    caches: caches.into_iter().filter_map(answer_cache).collect(),
                    paths,
                    spare,
                    why,
                };
                self.on_udp_answer(legs, entry, nonce, answer, now, out);
                self.run(legs, now, out);
                true
            }
            _ => assigned,
        }
    }

    fn on_udp_answer<S: SessionLegs>(
        &mut self,
        legs: &mut S,
        entry: Option<usize>,
        nonce: u64,
        answer: Answer,
        now: Instant,
        out: &mut Vec<Action>,
    ) {
        let Some(entry) = entry else {
            return;
        };
        if self
            .request
            .as_ref()
            .is_some_and(|request| request.nonce == nonce)
        {
            let request = self.request.as_mut().expect("checked");
            if !request.pool[..request.asked].contains(&entry) {
                return;
            }
            // A round trip from an unrepeated request.
            if request.repeats == 0 {
                self.entries[entry].rtt = Some(now.saturating_duration_since(request.started));
            }
            request.answered_by.push(entry);
            self.entries[entry].failed_until = None;
            self.stats.answers_udp += 1;
            self.take_answer(legs, answer, Some(entry), Via::Udp, now, out);
            return;
        }
        // A later answer to a request answered already: confirmed too.
        let confirm = match self.answered.as_mut() {
            Some((request, _))
                if request.nonce == nonce && !request.answered_by.contains(&entry) =>
            {
                request.answered_by.push(entry);
                request
                    .tokens
                    .iter()
                    .find(|(index, _)| *index == entry)
                    .map(|(_, token)| (*token, request.clone()))
            }
            _ => None,
        };
        if let Some((token, request)) = confirm {
            self.confirm_to(&request, entry, token, now, out);
        }
    }

    /// The answer to an [`Action::Https`]: the body, or `None` on any
    /// failure.
    pub fn on_https<S: SessionLegs>(
        &mut self,
        legs: &mut S,
        id: u64,
        body: Option<&[u8]>,
        now: Instant,
        out: &mut Vec<Action>,
    ) {
        let Some(request) = self.request.as_ref().filter(|request| request.nonce == id) else {
            return;
        };
        let first = request.pool.first().copied();
        match body.and_then(parse_https_answer) {
            Some(answer) => {
                self.stats.answers_https += 1;
                self.take_answer(legs, answer, None, Via::Https, now, out);
            }
            // HTTPS alone: the request has failed.
            None if self.config.assignment_udp_port == 0 => {
                if let Some(first) = first {
                    self.entries[first].failed_until = Some(now + ENTRY_FAILED_FOR);
                }
                self.fail_request(now);
            }
            None => {}
        }
        self.run(legs, now, out);
    }

    /// The address of a host that an [`Action::Resolve`] named (`None`: it
    /// has none).
    pub fn on_resolved<S: SessionLegs>(
        &mut self,
        legs: &mut S,
        host: &str,
        ip: Option<IpAddr>,
        now: Instant,
        out: &mut Vec<Action>,
    ) {
        self.resolved.insert(host.to_owned(), ip);
        for index in 0..self.entries.len() {
            if self.entries[index].host == host && self.entries[index].ip.is_none() {
                self.entries[index].ip = ip;
                // A request waiting for this entry's address goes now.
                let send = self.request.as_ref().and_then(|request| {
                    request.pool[..request.asked]
                        .contains(&index)
                        .then(|| request.datagram.clone())
                });
                if let (Some(datagram), Some(addr)) = (send, self.entry_addr(index)) {
                    out.push(Action::Send { to: addr, datagram });
                }
            }
        }
        if let Some((answer, entry, via)) = self.pending_answer.take() {
            self.take_answer(legs, answer, entry, via, now, out);
        }
        self.run(legs, now, out);
    }

    /// The end of the run: every leg released, the spare forgotten.
    pub fn leave<S: SessionLegs>(&mut self, legs: &mut S, now: Instant, out: &mut Vec<Action>) {
        for leg in std::mem::take(&mut self.legs) {
            legs.release_leg(leg.key, now, &mut sender(out));
        }
        self.spare = None;
        self.request = None;
        self.hedge_due = None;
    }

    /// Read the session's events and run the timers. Cheap when nothing is
    /// due: call it after every datagram.
    pub fn poll<S: SessionLegs>(&mut self, legs: &mut S, now: Instant, out: &mut Vec<Action>) {
        let mut any = false;
        while let Some(event) = legs.next_event() {
            any = true;
            self.on_event(legs, event, now, out);
        }
        if any || now >= self.wake {
            self.run(legs, now, out);
        }
    }

    fn on_event<S: SessionLegs>(
        &mut self,
        legs: &mut S,
        event: LegEvent,
        now: Instant,
        out: &mut Vec<Action>,
    ) {
        match event {
            LegEvent::Tokened(key) => self.on_tokened(legs, key, now, out),
            LegEvent::Live { leg, compact } => self.on_live(legs, leg, compact, now, out),
            LegEvent::Moved { leg, reason } => self.on_moved(leg, reason, now),
            // Skipped by cause: a refusal for load or a rebalance passes in
            // seconds; a drain, lost parents or anything else is the
            // edge's for longer.
            LegEvent::Refused { leg, reason } => {
                let busy = BUSY_REFUSALS.contains(&reason.as_str());
                let cause = if reason == REBALANCE || busy {
                    SkipCause::Busy
                } else {
                    SkipCause::Failed
                };
                self.fail(legs, leg, &reason, cause, now, out);
                // Place240: an edge at its heavy cap refuses `edge-busy`; with
                // every edge of the region at it the entry names the least
                // full again, which refuses again. A listener with no leg
                // asks again after the retry wait, not at once (an honest
                // busy answer, not a loop of refusals); one with a leg
                // that delivers asks at once, as before.
                if busy && !self.any_delivering(legs, now, None) {
                    self.retry_at = Some(now + RETRY_ALONE);
                }
            }
            LegEvent::NoToken(key) => {
                if let Some(index) = self.leg_index(key) {
                    let leg = &mut self.legs[index];
                    if leg.passed_over.is_none() && !leg.tokened {
                        leg.passed_over = Some(now);
                        let node = leg.node.clone();
                        self.skip_node(&node, SkipCause::NoToken, now);
                        self.place_opened(now);
                    }
                }
            }
            LegEvent::HedgeDue(key) => self.on_hedge_due(key, now),
            LegEvent::Failed { leg, cause } => {
                self.fail(legs, leg, cause.reason(), SkipCause::Failed, now, out)
            }
            LegEvent::TakenBack(_) => {}
            LegEvent::AllSilent => self.on_all_silent(legs, now, out),
            LegEvent::ObjectLane => self.on_object_lane(legs, now, out),
            LegEvent::Degraded { leg, reason } => self.quality_move(leg, &reason, now),
            LegEvent::Distress { leg, reason } => self.on_distress(legs, leg, reason, now),
        }
    }

    /// Place240 rule 4: the edge of `key` says it is in distress. A
    /// listener whose other legs deliver ignores it unless every leg's edge
    /// said so lately; one with a lone leg moves (as rule 3).
    fn on_distress<S: SessionLegs>(&mut self, legs: &S, key: LegKey, reason: String, now: Instant) {
        self.distress_seen
            .retain(|(leg, at)| *leg != key && now.saturating_duration_since(*at) < DISTRESS_TOGETHER);
        self.distress_seen.push((key, now));
        let others: Vec<LegKey> = self
            .legs
            .iter()
            .filter(|leg| leg.key != key && leg.counted() && Self::view(legs, leg.key, now).delivering)
            .map(|leg| leg.key)
            .collect();
        let together = others
            .iter()
            .all(|other| self.distress_seen.iter().any(|(leg, _)| leg == other));
        if !together {
            self.stats.quality.distress_ignored += 1;
            return;
        }
        self.quality_move(key, &reason, now);
    }

    /// Place240 rule 3: leave the leg `key` make-before-break for `reason`
    /// (late or missing parts, or its edge's distress): it is leaving (the
    /// session keeps taking its parts), its node skipped for the hold-off,
    /// and the entry asked at once for another edge; the old leg goes at
    /// its replacement's first object (one leg) or once level. Guarded
    /// against flapping: at most `quality_moves_per_minute`, and none for a
    /// degradation within `quality_no_better` of the last such move (the
    /// new edge is no better: the cause is upstream or the client's own),
    /// which also holds such moves off for `quality_hold_off`.
    fn quality_move(&mut self, key: LegKey, reason: &str, now: Instant) {
        let Some(index) = self.leg_index(key) else {
            return;
        };
        if !self.legs[index].counted() || !self.legs[index].live {
            return;
        }
        if self.quality_quiet_until.is_some_and(|until| now < until) {
            self.stats.quality.no_better += 1;
            return;
        }
        if self
            .last_quality_move
            .is_some_and(|at| now.saturating_duration_since(at) < self.config.quality_no_better)
        {
            self.stats.quality.no_better += 1;
            self.quality_quiet_until = Some(now + self.config.quality_hold_off);
            return;
        }
        self.quality_moves
            .retain(|at| now.saturating_duration_since(*at) < Duration::from_secs(60));
        if self.quality_moves.len() >= self.config.quality_moves_per_minute as usize {
            self.stats.quality.over_budget += 1;
            return;
        }
        self.quality_moves.push(now);
        self.last_quality_move = Some(now);
        match reason {
            LATE_PARTS => self.stats.quality.late_parts += 1,
            MISSING_PARTS => self.stats.quality.missing_parts += 1,
            _ => self.stats.quality.distress += 1,
        }
        let leg = &mut self.legs[index];
        leg.leaving = Some((reason.to_owned(), now));
        leg.passed_over = None;
        let node = leg.node.clone();
        self.skip_node(&node, SkipCause::Quality, now);
        self.place_opened(now);
        self.deferred_ask = None;
    }

    fn on_tokened<S: SessionLegs>(
        &mut self,
        legs: &mut S,
        key: LegKey,
        now: Instant,
        out: &mut Vec<Action>,
    ) {
        let Some(index) = self.leg_index(key) else {
            return;
        };
        self.legs[index].tokened = true;
        // The join's leg answered in time: no swap.
        if self.swap_at.is_some() {
            self.swap_at = None;
        }
        // A leg passed over answered: used if it answered before the leg
        // added for it.
        if self.legs[index].passed_over.is_some() {
            let beaten = self
                .legs
                .iter()
                .any(|leg| leg.stands_for == Some(key) && leg.tokened);
            if beaten {
                self.release(legs, key, now, out);
            } else {
                self.legs[index].passed_over = None;
                while let Some(newer) = self
                    .legs
                    .iter()
                    .find(|leg| leg.stands_for == Some(key))
                    .map(|leg| leg.key)
                {
                    self.release(legs, newer, now, out);
                }
            }
        }
    }

    fn on_live<S: SessionLegs>(
        &mut self,
        legs: &mut S,
        key: LegKey,
        compact: bool,
        now: Instant,
        out: &mut Vec<Action>,
    ) {
        let Some(index) = self.leg_index(key) else {
            return;
        };
        self.ever_live = true;
        let leg = &mut self.legs[index];
        leg.live = true;
        leg.tokened = true;
        leg.compact = compact;
        let node = leg.node.clone();
        if let Some(pending) = leg.replaces.take() {
            let first = Self::view(legs, key, now).first_media.unwrap_or(now);
            self.replacements.push(Replacement {
                at: now,
                from: pending.from,
                to: node.clone(),
                reason: pending.reason,
                kind: pending.kind,
                handover: None,
                exposure: pending
                    .last_media
                    .map(|last| first.saturating_duration_since(last)),
            });
        }
        // An edge without the compact lane is left while another leg has it.
        if !compact
            && self
                .legs
                .iter()
                .any(|other| other.key != key && other.live && other.compact)
        {
            self.fail(legs, key, "no-compact", SkipCause::Failed, now, out);
        }
    }

    fn on_moved(&mut self, key: LegKey, reason: String, now: Instant) {
        let Some(index) = self.leg_index(key) else {
            return;
        };
        let leg = &mut self.legs[index];
        match &mut leg.leaving {
            // A drain re-marks a rebalanced leg; a repeat changes nothing.
            // A leg leaving already is not in the region the listener is
            // in, or is going already: a region-lost `MOVE` from it (every
            // edge of a lost region sends one, each at its own tick) does
            // not run the failover again.
            Some((old, at)) => {
                if *old != reason {
                    *old = reason;
                    *at = now;
                }
                return;
            }
            // A leg of the current region: the whole region is left.
            None if reason == REGION_LOST && !self.failover.is_empty() => {
                let url = self.failover[0].clone();
                self.go_to_region(&url, false, REGION_LOST, now);
                return;
            }
            None => leg.leaving = Some((reason, now)),
        }
        leg.passed_over = None;
        let node = leg.node.clone();
        self.skip_node(&node, SkipCause::Moved, now);
        self.place_opened(now);
        self.deferred_ask = None;
    }

    fn on_hedge_due(&mut self, key: LegKey, now: Instant) {
        if self.hedge_due.is_some()
            || self.legs.iter().any(|leg| leg.hedge_for.is_some())
            || self.leg_index(key).is_none()
            || !self
                .spare
                .as_ref()
                .is_some_and(|spare| spare.token.is_some())
        {
            return;
        }
        let delay = spread(&self.config.session, 1, HEDGE_SPREAD);
        self.hedge_due = Some((now + delay, key));
    }

    fn hedge<S: SessionLegs>(
        &mut self,
        legs: &mut S,
        cover: LegKey,
        now: Instant,
        out: &mut Vec<Action>,
    ) {
        if self.leg_index(cover).is_none() || self.legs.len() >= MAX_LEGS {
            return;
        }
        let Some(spare) = self.spare.take_if(|spare| spare.token.is_some()) else {
            return;
        };
        let from = if self.any_delivering(legs, now, Some(cover)) {
            StartFrom::AfterNewest
        } else {
            StartFrom::OldestHole
        };
        let start = LegStart {
            from,
            token: spare.token,
            token_wait: self.token_wait(),
        };
        let key = self.add(legs, &spare.node.clone(), spare.addr, start, now, out);
        let leg = self.legs.last_mut().expect("added");
        debug_assert_eq!(leg.key, key);
        leg.hedge_for = Some(cover);
        leg.spare = Some(spare);
        self.stats.hedges += 1;
        // The covered leg must be steady again for a whole level wait.
        if let Some(index) = self.leg_index(cover) {
            self.legs[index].steady_since = None;
        }
    }

    fn on_all_silent<S: SessionLegs>(&mut self, legs: &mut S, now: Instant, out: &mut Vec<Action>) {
        let keys: Vec<LegKey> = self.legs.iter().map(|leg| leg.key).collect();
        for key in &keys {
            legs.reask_leg(*key, now, &mut sender(out));
        }
        // A leg's steadiness counts parts, and none came: it says nothing
        // until the leg has delivered again.
        for leg in self.legs.iter_mut() {
            leg.quiet_since = Some(now);
            leg.steady_since = None;
        }
        let hedged = self.legs.iter().any(|leg| leg.hedge_for.is_some());
        let spare_ready = self
            .spare
            .as_ref()
            .is_some_and(|spare| spare.token.is_some());
        let mut silence = Silence {
            at: now,
            check_at: now + self.token_wait(),
            legs: keys.clone(),
            asked: false,
            placed: Vec::new(),
            resumed: None,
        };
        if !hedged && spare_ready {
            if let Some(cover) = keys.first() {
                self.hedge_due
                    .get_or_insert((now + spread(&self.config.session, 1, HEDGE_SPREAD), *cover));
            }
        } else if !hedged {
            // No spare: the entry is asked at once.
            silence.asked = self.ask_in_silence(&keys, now, out);
        }
        self.silence = Some(silence);
    }

    fn ask_in_silence(&mut self, silent: &[LegKey], now: Instant, out: &mut Vec<Action>) -> bool {
        if self.request.is_some() {
            return false;
        }
        let room = MAX_LEGS.saturating_sub(self.legs.len());
        let paths = (self.want as usize).min(room) as u8;
        let spare = self.spare_wanted();
        if paths == 0 && !spare {
            return false;
        }
        // The silent legs are skipped on the wire only: no edge is judged
        // by a silence of every leg.
        let mut skip = self.skipped(now);
        for key in silent {
            if let Some(index) = self.leg_index(*key) {
                let node = self.legs[index].node.clone();
                if !skip.contains(&node) {
                    skip.push(node);
                }
            }
        }
        self.send_request(
            RequestKind::Silence,
            paths,
            spare,
            skip,
            Vec::new(),
            None,
            now,
            out,
        );
        true
    }

    fn on_object_lane<S: SessionLegs>(
        &mut self,
        legs: &mut S,
        now: Instant,
        out: &mut Vec<Action>,
    ) {
        self.objects = true;
        self.want = 1;
        // The leg that is not leading is released.
        let counted: Vec<(LegKey, Duration)> = self
            .legs
            .iter()
            .filter(|leg| leg.counted() && leg.live)
            .map(|leg| {
                (
                    leg.key,
                    Self::view(legs, leg.key, now).lag.unwrap_or(Duration::MAX),
                )
            })
            .collect();
        if counted.len() > 1 {
            let leader = counted
                .iter()
                .min_by_key(|(_, lag)| *lag)
                .map(|(key, _)| *key)
                .expect("not empty");
            for (key, _) in counted {
                if key != leader {
                    self.release(legs, key, now, out);
                }
            }
        }
    }

    /// The session gave a leg up, or the director does.
    fn fail<S: SessionLegs>(
        &mut self,
        legs: &mut S,
        key: LegKey,
        reason: &str,
        cause: SkipCause,
        now: Instant,
        out: &mut Vec<Action>,
    ) {
        let Some(index) = self.leg_index(key) else {
            return;
        };
        let view = Self::view(legs, key, now);
        let mut leg = self.remove_at(index);
        legs.release_leg(key, now, &mut sender(out));
        self.skip_node(&leg.node, cause, now);
        self.place_opened(now);
        self.deferred_ask = None;
        if self.hedge_due.is_some_and(|(_, cover)| cover == key) {
            self.hedge_due = None;
        }
        if leg.hedge_for.is_some() {
            // The spare itself failed: a new one is asked for.
            return;
        }
        // A leg added to take an apportioned place failed: the hedge leg it
        // was to replace keeps that place.
        if let Some(stood) = leg.stands_for {
            if let Some(old) = self
                .legs
                .iter_mut()
                .find(|other| other.key == stood && other.leaving_for(APPORTION))
            {
                old.leaving = None;
                return;
            }
        }
        // A hedge beside it takes its place, and the entry is asked for that
        // place as for a mover.
        if let Some(hedge) = self
            .legs
            .iter_mut()
            .find(|other| other.hedge_for == Some(key))
        {
            hedge.hedge_for = None;
            hedge.spare = None;
            hedge.unplaced = self.config.spare;
            let first = Self::view(legs, hedge.key, now).first_media;
            self.replacements.push(Replacement {
                at: now,
                from: leg.node.clone(),
                to: hedge.node.clone(),
                reason: reason.to_owned(),
                kind: ReplacementKind::Hedge,
                handover: None,
                exposure: first
                    .zip(view.last_media)
                    .map(|(first, last)| first.saturating_duration_since(last)),
            });
            return;
        }
        // A leg that never went live was itself filling a failed leg's
        // place: that record still waits, not a second one.
        if let Some(record) = leg.replaces.take() {
            self.pending.insert(0, record);
            return;
        }
        if leg.leaving.is_some() || leg.passed_over.is_some() {
            return;
        }
        let kind = if self.any_delivering(legs, now, None) {
            ReplacementKind::Replace
        } else {
            ReplacementKind::Interrupt
        };
        let record = Pending {
            from: leg.node,
            reason: reason.to_owned(),
            kind,
            last_media: view.last_media,
        };
        match self.silence.take_if(|silence| silence.legs.contains(&key)) {
            Some(mut silence) => {
                self.place_after_silence(legs, record, &mut silence, now);
                self.silence = Some(silence);
            }
            None => self.pending.push(record),
        }
    }

    /// A leg that went silent with every other was given up: a leg asked
    /// for in the silence (not one of the silent legs, not a hedge, standing
    /// for nothing, holding no record, and not given a record already in
    /// this silence) took its place. Its record goes to that leg (booked now
    /// if it is live) rather than wait for a place that no answer will
    /// bring. The kind is `interrupt` when the taker brought media before
    /// any silent leg came back (it ended the silence), else the one `fail`
    /// gave.
    fn place_after_silence<S: SessionLegs>(
        &mut self,
        legs: &S,
        record: Pending,
        silence: &mut Silence,
        now: Instant,
    ) {
        let Some(index) = self.legs.iter().position(|leg| {
            !silence.legs.contains(&leg.key)
                && !silence.placed.contains(&leg.key)
                && leg.hedge_for.is_none()
                && leg.stands_for.is_none()
                && leg.replaces.is_none()
        }) else {
            self.pending.push(record);
            return;
        };
        let key = self.legs[index].key;
        silence.placed.push(key);
        let first = Self::view(legs, key, now).first_media;
        let interrupt = silence
            .resumed
            .is_none_or(|back| first.is_some_and(|first| first <= back));
        let record = Pending {
            kind: if interrupt {
                ReplacementKind::Interrupt
            } else {
                record.kind
            },
            ..record
        };
        let leg = &mut self.legs[index];
        if leg.live {
            self.replacements.push(Replacement {
                at: now,
                from: record.from,
                to: leg.node.clone(),
                reason: record.reason,
                kind: record.kind,
                handover: None,
                exposure: first
                    .zip(record.last_media)
                    .map(|(first, last)| first.saturating_duration_since(last)),
            });
        } else {
            leg.replaces = Some(record);
        }
    }

    /// Release a leg for good (no skip of its own).
    fn release<S: SessionLegs>(
        &mut self,
        legs: &mut S,
        key: LegKey,
        now: Instant,
        out: &mut Vec<Action>,
    ) -> Option<DirectorLeg> {
        let index = self.leg_index(key)?;
        let mut leg = self.remove_at(index);
        legs.release_leg(key, now, &mut sender(out));
        if let Some(record) = leg.replaces.take() {
            self.pending.insert(0, record);
        }
        if leg.leaving.is_some() {
            if let Some(skip) = self.skip.get_mut(&leg.node) {
                if skip.cause == SkipCause::Moved && skip.until.is_none() {
                    skip.until = Some(now + SKIP_AFTER_MOVE);
                }
            }
        }
        if self.hedge_due.is_some_and(|(_, cover)| cover == key) {
            self.hedge_due = None;
        }
        // A hedge whose leg is gone without a verdict keeps its place.
        for other in self.legs.iter_mut() {
            if other.hedge_for == Some(key) {
                other.hedge_for = None;
                other.spare = None;
            }
        }
        Some(leg)
    }

    /// Take a leg out of the list. A leg added for it stands for what it
    /// stood for, and takes over its record of a failed leg if it never went
    /// live (the record stays in the returned leg when no leg was added for
    /// it).
    fn remove_at(&mut self, index: usize) -> DirectorLeg {
        let mut leg = self.legs.remove(index);
        for other in self.legs.iter_mut() {
            if other.stands_for == Some(leg.key) {
                other.stands_for = leg.stands_for;
                if other.replaces.is_none() {
                    other.replaces = leg.replaces.take();
                }
            }
        }
        leg
    }

    fn add<S: SessionLegs>(
        &mut self,
        legs: &mut S,
        node: &str,
        addr: SocketAddr,
        start: LegStart,
        now: Instant,
        out: &mut Vec<Action>,
    ) -> LegKey {
        let key = LegKey(self.next_key);
        self.next_key += 1;
        legs.add_leg(key, node, addr, start, now, &mut sender(out));
        self.legs.push(DirectorLeg {
            key,
            node: node.to_owned(),
            addr,
            added_at: now,
            tokened: start.token.is_some(),
            live: false,
            hedge_for: None,
            leaving: None,
            passed_over: None,
            stands_for: None,
            compact: false,
            steady_since: None,
            quiet_since: None,
            spare: None,
            replaces: None,
            unplaced: false,
        });
        key
    }

    fn take_back<S: SessionLegs>(
        &mut self,
        legs: &mut S,
        key: LegKey,
        now: Instant,
        out: &mut Vec<Action>,
    ) {
        let Some(index) = self.leg_index(key) else {
            return;
        };
        let leg = &mut self.legs[index];
        let Some((reason, _)) = leg.leaving.take() else {
            return;
        };
        let node = leg.node.clone();
        legs.takeback_leg(key, now, &mut sender(out));
        self.skip.remove(&node);
        // It holds its place again: no leg stands for it.
        for other in self.legs.iter_mut() {
            if other.stands_for == Some(key) {
                other.stands_for = None;
            }
        }
        self.stats.takebacks += 1;
        self.replacements.push(Replacement {
            at: now,
            from: node.clone(),
            to: node,
            reason,
            kind: ReplacementKind::Takeback,
            handover: None,
            exposure: None,
        });
    }

    fn on_spare_token(&mut self, token: u64, now: Instant) {
        let Some(spare) = self.spare.as_mut() else {
            return;
        };
        let first = spare.token.is_none();
        spare.token = Some(token);
        spare.asked_at = None;
        spare.misses = 0;
        spare.next_renew = now + SPARE_RENEW;
        // A one-leg join whose leg has no token yet takes the spare's edge
        // if the leg's token does not follow within the window.
        if first
            && !self.ever_live
            && self.want == 1
            && self.legs.len() == 1
            && !self.legs[0].tokened
            && self.legs[0].counted()
        {
            self.swap_at = Some(now + SWAP_WINDOW);
        }
    }

    fn swap<S: SessionLegs>(&mut self, legs: &mut S, now: Instant, out: &mut Vec<Action>) {
        if self.legs.len() != 1 || self.legs[0].tokened || self.ever_live {
            return;
        }
        let Some(spare) = self.spare.take_if(|spare| spare.token.is_some()) else {
            return;
        };
        let old = self.legs[0].key;
        let start = LegStart {
            from: StartFrom::OldestHole,
            token: spare.token,
            token_wait: self.token_wait(),
        };
        self.add(legs, &spare.node, spare.addr, start, now, out);
        // The two swap roles: the leg's edge becomes the spare.
        if let Some(leg) = self.release(legs, old, now, out) {
            self.set_spare(leg.node, leg.addr, now, out);
        }
    }

    fn set_spare(&mut self, node: String, addr: SocketAddr, now: Instant, out: &mut Vec<Action>) {
        let mut spare = Spare {
            node,
            addr,
            token: None,
            asked_at: None,
            misses: 0,
            next_renew: now,
        };
        self.fetch_spare_token(&mut spare, now, out);
        self.spare = Some(spare);
    }

    /// A tokenless padded `SUB`: answered `TOKEN`, nothing kept at the edge.
    fn fetch_spare_token(&self, spare: &mut Spare, now: Instant, out: &mut Vec<Action>) {
        let datagram = Control::Subscribe {
            stream_id: self.config.stream_id,
            from_part: None,
            repair: RepairPolicy::default(),
            token: None,
            fast_lane: false,
            compact: None,
            others: Vec::new(),
            resume: false,
        }
        .encode();
        out.push(Action::Send {
            to: spare.addr,
            datagram,
        });
        spare.asked_at = Some(now);
    }

    fn go_to_region(&mut self, url: &str, home: bool, reason: &str, now: Instant) {
        let index = self.entry_index(url);
        self.current = if home { self.home.clone() } else { vec![index] };
        self.away_since = if home { None } else { Some(now) };
        self.next_home_probe = now + HOME_HOLD;
        if home {
            self.stats.home_returns += 1;
        } else {
            self.stats.failovers += 1;
        }
        let mut left = Vec::new();
        for leg in self.legs.iter_mut() {
            if leg.leaving.is_none() {
                leg.leaving = Some((reason.to_owned(), now));
                leg.passed_over = None;
                left.push(leg.node.clone());
            }
        }
        for node in left {
            self.skip_node(&node, SkipCause::Moved, now);
        }
        // The spare is in the region left.
        self.spare = None;
        self.hedge_due = None;
        self.request = None;
        self.failures = 0;
        self.place_opened(now);
        self.deferred_ask = None;
        self.short_answers = 0;
        // Only skips of legs still held matter in another region.
        let held: Vec<String> = self.legs.iter().map(|leg| leg.node.clone()).collect();
        self.skip.retain(|node, _| held.contains(node));
    }

    fn take_answer<S: SessionLegs>(
        &mut self,
        legs: &mut S,
        answer: Answer,
        entry: Option<usize>,
        via: Via,
        now: Instant,
        out: &mut Vec<Action>,
    ) {
        // Every host of the region's caches must have an address first.
        let mut waiting = false;
        for cache in answer
            .caches
            .iter()
            .filter(|cache| cache.failover.is_none())
        {
            if cache.host.parse::<IpAddr>().is_err() && !self.resolved.contains_key(&cache.host) {
                if !out
                    .iter()
                    .any(|action| matches!(action, Action::Resolve { host } if *host == cache.host))
                {
                    out.push(Action::Resolve {
                        host: cache.host.clone(),
                    });
                }
                waiting = true;
            }
        }
        if waiting {
            self.pending_answer = Some((answer, entry, via));
            return;
        }
        let Some(request) = self.request.take() else {
            return;
        };
        self.failures = 0;
        self.first_answer.get_or_insert((now, via));
        let failover: Vec<String> = answer
            .caches
            .iter()
            .filter_map(|cache| cache.failover.clone())
            .collect();
        if !failover.is_empty() {
            self.failover = failover;
        }
        for cache in answer
            .caches
            .iter()
            .filter(|cache| cache.failover.is_none())
        {
            if self.urls.get(&cache.node) != Some(&cache.url) {
                self.urls.insert(cache.node.clone(), cache.url.clone());
            }
        }
        let region: Vec<(String, SocketAddr)> = answer
            .caches
            .iter()
            .filter(|cache| cache.failover.is_none())
            .filter_map(|cache| {
                let ip = match cache.host.parse::<IpAddr>() {
                    Ok(ip) => ip,
                    Err(_) => (*self.resolved.get(&cache.host)?)?,
                };
                let port = self.config.udp_port.or(cache.port).or(answer.port)?;
                Some((cache.node.clone(), SocketAddr::new(ip, port)))
            })
            .collect();
        if request.kind == RequestKind::HomeProbe {
            if region.is_empty() {
                self.answered = Some((request, now + ANSWERED_KEPT));
                return;
            }
            let home = self.entries[self.home[0]].url.clone();
            self.go_to_region(&home, true, HOME_REGION, now);
        }
        let held_nodes: Vec<String> = self.legs.iter().map(|leg| leg.node.clone()).collect();
        let usable: Vec<&(String, SocketAddr)> = region
            .iter()
            .filter(|(node, _)| !held_nodes.contains(node))
            .collect();
        // No cache of the region for a listener with nothing left there:
        // a failover region (step 9; a lost region, B:1152-1161). A leg
        // leaving for a rebalance or lost parents is asked to stay instead
        // (steps 5 and 6), so the listener does not leave the region.
        let all_leaving = self
            .legs
            .iter()
            .filter(|leg| leg.hedge_for.is_none())
            .all(|leg| {
                leg.leaving.is_some()
                    && !leg.rebalance()
                    && !leg.leaving_for(PARENTS_LOST)
                    && !leg.leaving.as_ref().is_some_and(|(why, _)| quality_reason(why))
            });
        if usable.is_empty() && all_leaving && !self.failover.is_empty() {
            let url = self.failover[0].clone();
            self.answered = Some((request, now + ANSWERED_KEPT));
            self.go_to_region(&url, false, REGION_LOST, now);
            return;
        }
        let granted = answer.paths.min(request.paths) as usize;
        if granted < request.paths as usize {
            self.stats.short_grants.count(answer.why);
        }
        // Step 1: want is what the entry granted for --paths. A stream over
        // the two-path bit rate (`why=bitrate`) keeps the legs it holds and
        // the ones granted, and nothing is asked again until the listener
        // holds no leg. Any other short grant (no other edge, none through
        // the gate) is for want of an edge: want stays, and the missing leg
        // is asked again on the backoff.
        if request.kind != RequestKind::Silence {
            if answer.why == Some(ShortGrant::Bitrate) {
                self.want = (self.held() + granted).max(1) as u8;
            } else if request.hold.is_empty() && request.paths == self.full_paths() {
                self.want = self.full_paths();
            }
        }
        // The place of a hedge leg, apportioned: kept where the entry names
        // the hedge's own edge (or names none, or one the listener cannot
        // take); else the hedge leg leaves for the edge named, make before
        // break, and is the spare again once that leg is level.
        if let Some(place) = request.place_of {
            if let Some(index) = self.leg_index(place).filter(|index| {
                let leg = &self.legs[*index];
                leg.unplaced && leg.counted()
            }) {
                self.legs[index].unplaced = false;
                let named = answer
                    .caches
                    .iter()
                    .take(granted)
                    .find(|cache| cache.failover.is_none())
                    .map(|cache| cache.node.clone());
                let movable = named.as_ref().is_some_and(|named| {
                    *named != self.legs[index].node
                        && usable.iter().any(|(node, _)| node == named)
                        && !self.skipped_now(named, now)
                });
                if movable {
                    self.legs[index].leaving = Some((APPORTION.to_owned(), now));
                    self.stats.apportion_moves += 1;
                }
            }
        }
        let need = match request.kind {
            RequestKind::Silence => request.paths as usize,
            _ => (self.want as usize).saturating_sub(self.held()),
        };
        let need = need.min(MAX_LEGS.saturating_sub(self.legs.len()));
        let limit = need.min(granted);
        let mut chosen: Vec<(String, SocketAddr)> = Vec::new();
        let mut after = 0;
        for (position, (node, addr)) in usable.iter().map(|cache| (&cache.0, cache.1)).enumerate() {
            if chosen.len() == limit {
                break;
            }
            if !self.skipped_now(node, now) {
                chosen.push((node.clone(), addr));
                after = position + 1;
            }
        }
        // Every cache of the region skipped: the one skipped longest ago.
        if chosen.is_empty() && limit > 0 {
            if let Some((node, addr)) = usable
                .iter()
                .min_by_key(|(node, _)| self.skip.get(node).map(|skip| skip.since))
                .map(|cache| (cache.0.clone(), cache.1))
            {
                self.skip.remove(&node);
                after = usable.iter().position(|cache| cache.0 == node).unwrap_or(0) + 1;
                chosen.push((node, addr));
            }
        }
        // A leg left for late or missing parts (place240) delivers, but
        // late: its holes are the new leg's, which starts from the oldest.
        let delivering = self.legs.iter().any(|leg| {
            !leg.leaving.as_ref().is_some_and(|(why, _)| quality_reason(why))
                && Self::view(legs, leg.key, now).delivering
        });
        let from = if delivering {
            StartFrom::AfterNewest
        } else {
            StartFrom::OldestHole
        };
        let token_wait = self.token_wait();
        // Records of failed legs wait only for places that no leg leaving
        // or passed over holds: the oldest beyond those go.
        let places = self.empty_places();
        if self.pending.len() > places {
            let extra = self.pending.len() - places;
            self.pending.drain(..extra);
        }
        for (node, addr) in &chosen {
            // The spare named as a leg is promoted with its token.
            let token = match self.spare.take_if(|spare| spare.node == *node) {
                Some(spare) => spare.token,
                None => None,
            };
            let start = LegStart {
                from,
                token,
                token_wait,
            };
            // A new leg is for the leg leaving longest, else one passed
            // over, else the place of a leg that failed.
            let stands_for = self.open_place();
            let record =
                (stands_for.is_none() && !self.pending.is_empty()).then(|| self.pending.remove(0));
            let key = self.add(legs, node, *addr, start, now, out);
            let index = self.leg_index(key).expect("added");
            self.legs[index].stands_for = stands_for;
            self.legs[index].replaces = record;
        }
        // The cache after the legs is the spare (after a promoted spare's
        // node too). A request that named the spare it holds (`k=`) gets it
        // again while its edge passes the gate, else another: taken.
        let held_named = request.spare_held.is_some()
            && self
                .spare
                .as_ref()
                .is_some_and(|spare| Some(&spare.node) == request.spare_held.as_ref());
        if request.spare && answer.spare && (self.spare_wanted() || held_named) {
            let spare = usable[after.min(usable.len())..]
                .iter()
                .find(|(node, _)| {
                    !self.skipped_now(node, now) && !chosen.iter().any(|(taken, _)| taken == node)
                })
                .map(|cache| (cache.0.clone(), cache.1));
            if let Some((node, addr)) = spare {
                if self.spare.as_ref().is_none_or(|held| held.node != node) {
                    if held_named {
                        self.stats.spares_rechosen += 1;
                    }
                    self.set_spare(node, addr, now, out);
                }
            }
        }
        // Confirm to every entry that answered, so that it charges the legs.
        for entry in request.answered_by.clone() {
            if let Some(token) = request
                .tokens
                .iter()
                .find(|(index, _)| *index == entry)
                .map(|(_, token)| *token)
            {
                self.confirm_to(&request, entry, token, now, out);
            }
        }
        // No cache named as a leg: none of the region, or none granted
        // (the redundant gate refused every edge with room; a spare named
        // alone is no leg). A rebalanced leg stays where it was at once
        // (step 6). A drained leg goes at once beside a delivering leg only
        // when the region has no cache at all; with a cache that has no
        // room now it is kept, served by its draining edge, and the leg is
        // asked for again on ONE_PATH_BACKOFF (fix 4, F6a/F6b: released, the
        // listener sat on one path for 3 to 25 s in rolls; kept with the
        // old backoff, it stayed until its edge cut it). An answer empty only
        // for the edges the listener left lately keeps the drained leg and
        // is asked again at once without them.
        let no_leg = granted == 0 && chosen.is_empty();
        let no_cache = usable.is_empty() || no_leg;
        let again = no_cache && self.no_cache(legs, &request, usable.is_empty(), now, out);
        // An answer to a request that did not ask for every place missing
        // now (one sent before the last MOVE or failure, or for the spare
        // alone) holds nothing back: the loop asks at once for the rest.
        let asked_all = request.started >= self.changed_at && request.paths as usize >= need;
        // A request that named the spare it held asked for a spare only to
        // keep or replace it: one missing now (promoted to a leg) is asked
        // for at once.
        let short = chosen.len() < need
            || (request.spare && request.spare_held.is_none() && self.spare_wanted());
        if !asked_all || again {
            self.retry_at = None;
        } else if no_cache || short {
            let leaving = self.legs.iter().any(|leg| leg.leaving.is_some());
            // A drained (or lost-parents) leg kept for want of room waits
            // for its replacement: asked again soon, as a lost place is.
            let move_waits = self.legs.iter().any(|leg| {
                leg.leaving.is_some() && !leg.rebalance() && !self.replaced(leg.key)
            });
            let wait = if self.legs.is_empty() {
                RETRY_ALONE
            } else if usable.is_empty() && (leaving || self.held() == 0) {
                RETRY_WITH_LEG
            } else if move_waits {
                let wait =
                    ONE_PATH_BACKOFF[self.short_answers.min(ONE_PATH_BACKOFF.len() - 1)];
                self.short_answers += 1;
                wait
            } else if no_cache && (leaving || self.held() == 0) {
                RETRY_WITH_LEG
            } else {
                let one_path = self.held() > 0 && self.held() < usize::from(self.want);
                let backoff: &[Duration] = if one_path && self.lost_place {
                    &ONE_PATH_BACKOFF
                } else {
                    &SHORT_BACKOFF
                };
                let wait = backoff[self.short_answers.min(backoff.len() - 1)];
                self.short_answers += 1;
                wait
            };
            self.retry_at = Some(now + wait);
        } else {
            self.short_answers = 0;
            self.lost_place = false;
        }
        self.answered = Some((request, now + ANSWERED_KEPT));
    }

    /// The answer named no cache of the region that the listener could take.
    /// It speaks for a leaving leg only when the request asked for its
    /// replacement (it asked for a leg and was sent after the `MOVE`) and no
    /// leg has been added for it: a spare's request answered empty says
    /// nothing of a leg whose replacement is joining. Returns whether the
    /// leaving leg's place is to be asked for again at once: the request
    /// skipped edges the listener left lately ([`SKIP_AFTER_MOVE`]), so the
    /// empty answer says nothing of the region (integration 4,
    /// `roll_of_three_edges`: a drain within 3 s of two earlier moves
    /// released the drained leg, and the listener was on one path until
    /// the backoff's next answer).
    fn no_cache<S: SessionLegs>(
        &mut self,
        legs: &mut S,
        request: &Request,
        region_empty: bool,
        now: Instant,
        out: &mut Vec<Action>,
    ) -> bool {
        if request.paths == 0 {
            return false;
        }
        let left_lately = request.skip.iter().any(|node| {
            self.skip
                .get(node)
                .is_some_and(|skip| skip.cause == SkipCause::Moved && skip.until.is_some())
        });
        let mut again = false;
        let leaving: Vec<(LegKey, bool, bool)> = self
            .legs
            .iter()
            .filter(|leg| {
                leg.hedge_for.is_none()
                    && leg
                        .leaving
                        .as_ref()
                        .is_some_and(|(_, at)| request.started >= *at)
                    && !self.replaced(leg.key)
            })
            .map(|leg| (leg.key, leg.rebalance(), leg.leaving_for(PARENTS_LOST)))
            .collect();
        for (key, rebalance, parents_lost) in leaving {
            if rebalance {
                // A rebalance with nowhere to go returns at once.
                self.take_back(legs, key, now, out);
            } else if !region_empty {
                // A cache of the region without room now: kept (see the
                // caller), asked again soon.
                continue;
            } else if left_lately {
                // Kept, and asked for again at once without the edges left
                // lately.
                again = true;
            } else if self.any_delivering(legs, now, Some(key)) {
                self.release(legs, key, now, out);
                self.lost_place = true;
            } else if parents_lost {
                self.take_back(legs, key, now, out);
            }
        }
        if again {
            self.skip
                .retain(|_, skip| !(skip.cause == SkipCause::Moved && skip.until.is_some()));
        }
        again
    }

    fn confirm_to(
        &mut self,
        request: &Request,
        entry: usize,
        token: u64,
        now: Instant,
        out: &mut Vec<Action>,
    ) {
        let Some(addr) = self.entry_addr(entry) else {
            return;
        };
        let current: Vec<String> = self
            .legs
            .iter()
            .filter(|leg| leg.leaving.is_none() && leg.passed_over.is_none())
            .map(|leg| leg.node.clone())
            .collect();
        let datagram = Control::Assign {
            stream_id: self.config.stream_id,
            nonce: request.nonce,
            session: self.config.session.clone(),
            skip: request.skip.clone(),
            current,
            paths: request.paths,
            spare: request.spare,
            hold: request.hold.clone(),
            spare_held: None,
            place_at: None,
            token: Some(token),
        }
        .encode();
        out.push(Action::Send {
            to: addr,
            datagram: datagram.clone(),
        });
        self.delayed.push((now + CONFIRM_REPEAT, addr, datagram));
    }

    fn request_wait(&self, request: &Request) -> Duration {
        if self.config.first_retry.is_zero() {
            return REQUEST_REPEAT_MAX;
        }
        let rtt = request
            .pool
            .first()
            .and_then(|index| self.entries[*index].rtt)
            .unwrap_or_default();
        self.config
            .first_retry
            .max(rtt * 3)
            .saturating_mul(1 << request.repeats.min(8))
            .min(REQUEST_REPEAT_MAX)
    }

    #[allow(clippy::too_many_arguments)]
    fn send_request(
        &mut self,
        kind: RequestKind,
        paths: u8,
        spare: bool,
        skip: Vec<String>,
        hold: Vec<String>,
        held: Option<(Option<String>, Option<LegKey>)>,
        now: Instant,
        out: &mut Vec<Action>,
    ) {
        let (spare_held, place_of) = held.unwrap_or_default();
        let entries = if kind == RequestKind::HomeProbe {
            self.home.clone()
        } else {
            self.current.clone()
        };
        if entries.is_empty() {
            return;
        }
        let urls: Vec<String> = entries
            .iter()
            .map(|index| self.entries[*index].url.clone())
            .collect();
        let first = crate::load_assign::entry_for(&urls, &self.config.session);
        let pool: Vec<usize> = (0..entries.len())
            .map(|offset| entries[(first + offset) % entries.len()])
            .collect();
        let nonce = self.next_nonce();
        let datagram = Control::Assign {
            stream_id: self.config.stream_id,
            nonce,
            session: self.config.session.clone(),
            skip: skip.clone(),
            current: Vec::new(),
            paths,
            spare,
            hold: hold.clone(),
            spare_held: spare_held.clone(),
            place_at: place_of
                .and_then(|key| self.leg_index(key))
                .map(|index| self.legs[index].node.clone()),
            token: None,
        }
        .encode();
        let udp = self.config.assignment_udp_port != 0;
        let mut request = Request {
            nonce,
            kind,
            started: now,
            deadline: now + self.config.assignment_timeout.max(Duration::from_millis(1)),
            datagram,
            paths,
            spare,
            skip,
            hold,
            pool,
            asked: 0,
            repeats: 0,
            next_repeat: now,
            next_entry_at: None,
            https_at: udp.then(|| now + self.config.assignment_https_after),
            tokens: Vec::new(),
            answered_by: Vec::new(),
            spare_held,
            place_of,
        };
        if !udp {
            if let Some(first) = request.pool.first() {
                out.push(Action::Https {
                    id: nonce,
                    entry: self.entries[*first].url.clone(),
                    path: self.https_path(&request),
                });
            }
        } else {
            self.ask_entry(&mut request, out);
            // The first entry failed lately: the next one at once too.
            let failed = request.pool.first().is_some_and(|index| {
                self.entries[*index]
                    .failed_until
                    .is_some_and(|until| now < until)
            });
            if failed {
                self.ask_entry(&mut request, out);
            }
            if request.asked < request.pool.len() && !self.config.first_retry.is_zero() {
                request.next_entry_at = Some(now + self.config.first_retry);
            }
            request.next_repeat = now + self.request_wait(&request);
        }
        self.stats.requests += 1;
        self.request = Some(request);
        self.deferred_ask = None;
    }

    /// Send the request to the next entry of its pool.
    fn ask_entry(&mut self, request: &mut Request, out: &mut Vec<Action>) {
        let Some(&index) = request.pool.get(request.asked) else {
            return;
        };
        request.asked += 1;
        match self.entry_addr(index) {
            Some(to) => out.push(Action::Send {
                to,
                datagram: request.datagram.clone(),
            }),
            None => {
                let host = self.entries[index].host.clone();
                if !host.is_empty() && !self.resolved.contains_key(&host) {
                    out.push(Action::Resolve { host });
                }
            }
        }
    }

    fn https_path(&self, request: &Request) -> String {
        let mut path = format!(
            "/api/assignment?stream={}&sid={}",
            self.config.stream_id, self.config.session
        );
        if !request.skip.is_empty() {
            path.push_str("&skip=");
            path.push_str(&request.skip.join(","));
        }
        if !request.hold.is_empty() {
            path.push_str("&hold=");
            path.push_str(&request.hold.join(","));
        }
        if request.paths != 1 {
            path.push_str(&format!("&paths={}", request.paths));
        }
        if request.spare {
            path.push_str("&spare=1");
        }
        if let Some(spare) = &request.spare_held {
            path.push_str("&keep=");
            path.push_str(spare);
        }
        if let Some(place) = request
            .place_of
            .and_then(|key| self.leg_index(key))
            .map(|index| &self.legs[index].node)
        {
            path.push_str("&at=");
            path.push_str(place);
        }
        path
    }

    fn fail_request(&mut self, now: Instant) {
        let Some(request) = self.request.take() else {
            return;
        };
        self.pending_answer = None;
        self.stats.assignment_failures += 1;
        // Only failures in a row with no leg count towards a failover.
        if self.legs.is_empty() {
            self.failures += 1;
        } else {
            self.failures = 0;
        }
        for index in &request.pool[..request.asked.max(1).min(request.pool.len())] {
            self.entries[*index].failed_until = Some(now + ENTRY_FAILED_FOR);
        }
        if request.kind == RequestKind::HomeProbe {
            return;
        }
        // No leg at all, twice: a failover region an earlier answer named.
        if self.legs.is_empty()
            && self.failures >= FAILOVER_AFTER_FAILURES
            && !self.failover.is_empty()
        {
            let url = self.failover[0].clone();
            self.go_to_region(&url, false, REGION_LOST, now);
            return;
        }
        self.retry_at = Some(
            now + if self.legs.is_empty() {
                RETRY_ALONE
            } else {
                RETRY_WITH_LEG
            },
        );
    }

    /// The loop of B 3.5, after every event and timer.
    fn run<S: SessionLegs>(&mut self, legs: &mut S, now: Instant, out: &mut Vec<Action>) {
        // Skips that ended.
        self.skip
            .retain(|_, skip| skip.until.is_none_or(|until| now < until));
        if self
            .answered
            .as_ref()
            .is_some_and(|(_, until)| now >= *until)
        {
            self.answered = None;
        }
        if self.retry_at.is_some_and(|at| now >= at) {
            self.retry_at = None;
        }
        // Repeated confirmations.
        let mut index = 0;
        while index < self.delayed.len() {
            if now >= self.delayed[index].0 {
                let (_, to, datagram) = self.delayed.swap_remove(index);
                out.push(Action::Send { to, datagram });
            } else {
                index += 1;
            }
        }
        if let Some((at, cover)) = self.hedge_due {
            if now >= at {
                self.hedge_due = None;
                self.hedge(legs, cover, now, out);
            }
        }
        if self.swap_at.is_some_and(|at| now >= at) {
            self.swap_at = None;
            self.swap(legs, now, out);
        }
        self.spare_timers(now, out);
        self.silence_check(legs, now, out);
        if now >= self.next_check {
            self.next_check = now + CHECK_EVERY;
            self.check_legs(legs, now, out);
        }
        self.request_timers(now, out);
        self.maybe_ask(legs, now, out);
        self.wake = self.compute_wake(now);
    }

    fn spare_timers(&mut self, now: Instant, out: &mut Vec<Action>) {
        let token_wait = self.token_wait();
        let Some(mut spare) = self.spare.take() else {
            return;
        };
        match spare.asked_at {
            Some(asked) if now >= asked + token_wait => {
                spare.misses += 1;
                if spare.misses >= SPARE_MISSES {
                    // Dropped; another is asked for.
                    self.stats.spares_dropped += 1;
                    return;
                }
                self.fetch_spare_token(&mut spare, now, out);
            }
            None if now >= spare.next_renew => self.fetch_spare_token(&mut spare, now, out),
            _ => {}
        }
        self.spare = Some(spare);
    }

    fn silence_check<S: SessionLegs>(&mut self, legs: &mut S, now: Instant, out: &mut Vec<Action>) {
        let Some(mut silence) = self.silence.take_if(|silence| now >= silence.check_at) else {
            return;
        };
        // A leg that does not answer the question in the token wait failed.
        let unanswered: Vec<LegKey> = silence
            .legs
            .iter()
            .copied()
            .filter(|key| {
                self.leg_index(*key).is_some()
                    && Self::view(legs, *key, now)
                        .last_datagram
                        .is_none_or(|last| last <= silence.at)
            })
            .collect();
        // The hedge takes the place of a leg that failed, whichever it
        // stood beside.
        if let Some(failed) = unanswered.first() {
            if let Some(hedge) = self.legs.iter_mut().find(|leg| {
                leg.hedge_for
                    .is_some_and(|cover| !unanswered.contains(&cover))
            }) {
                hedge.hedge_for = Some(*failed);
            }
        }
        // A hedge beside an unanswered leg takes that leg's place (`fail`),
        // and no other.
        for leg in &self.legs {
            if leg
                .hedge_for
                .is_some_and(|cover| unanswered.contains(&cover))
            {
                silence.placed.push(leg.key);
            }
        }
        // The records that failing the unanswered legs makes (the earlier
        // ones wait as they did) go to the legs asked for in the silence.
        let earlier = std::mem::take(&mut self.pending);
        for key in unanswered {
            self.fail(legs, key, "unanswered", SkipCause::Failed, now, out);
        }
        for record in std::mem::replace(&mut self.pending, earlier) {
            self.place_after_silence(legs, record, &mut silence, now);
        }
        let delivered = self.legs.iter().any(|leg| {
            Self::view(legs, leg.key, now)
                .last_media
                .is_some_and(|last| last > silence.at)
        });
        if !silence.asked && !delivered {
            self.ask_in_silence(&silence.legs, now, out);
        }
    }

    /// Handovers, hedges, extra legs and rebalance give-ups. It runs every
    /// 5 ms while one is pending, for every listener of a load host at once
    /// when an edge drains: it allocates nothing.
    fn check_legs<S: SessionLegs>(&mut self, legs: &mut S, now: Instant, out: &mut Vec<Action>) {
        for leg in self.legs.iter_mut() {
            let view = legs.leg(leg.key, now);
            let last = view.and_then(|view| view.last_media);
            if leg
                .quiet_since
                .is_some_and(|quiet| last.is_some_and(|last| last > quiet))
            {
                leg.quiet_since = None;
                if let (Some(silence), Some(last)) = (self.silence.as_mut(), last) {
                    if silence.legs.contains(&leg.key) {
                        silence.resumed.get_or_insert(last);
                    }
                }
            }
            if leg.live && leg.quiet_since.is_none() && view.is_some_and(|view| view.steady) {
                leg.steady_since.get_or_insert(now);
            } else {
                leg.steady_since = None;
            }
        }
        // Each change releases a leaving leg or ends its move, so this ends.
        let hole = legs.oldest_hole();
        while let Some(change) = self.next_handover(legs, hole, now) {
            match change {
                Handover::Release { old, new, moved_at } => {
                    let to = self
                        .leg_index(new)
                        .map(|index| self.legs[index].node.clone());
                    if let Some(leg) = self.release(legs, old, now, out) {
                        let reason = leg.leaving.map(|(reason, _)| reason).unwrap_or_default();
                        // A hedge leg that left for its apportioned place:
                        // its edge is the spare again.
                        if reason == APPORTION && self.config.spare && self.spare.is_none() {
                            self.set_spare(leg.node.clone(), leg.addr, now, out);
                        }
                        self.replacements.push(Replacement {
                            at: now,
                            from: leg.node,
                            to: to.unwrap_or_default(),
                            reason,
                            kind: ReplacementKind::Handover,
                            handover: Some(now.saturating_duration_since(moved_at)),
                            exposure: None,
                        });
                    }
                }
                // A rebalance not level by the give-up: the legs added for
                // it go, and the old edge is asked to keep the listener.
                Handover::GiveUp { old } => {
                    while let Some(new) = self
                        .legs
                        .iter()
                        .find(|leg| {
                            leg.stands_for == Some(old)
                                && leg.leaving.is_none()
                                && leg.hedge_for.is_none()
                        })
                        .map(|leg| leg.key)
                    {
                        self.release(legs, new, now, out);
                    }
                    self.take_back(legs, old, now, out);
                }
                Handover::Keep { old } => {
                    while let Some(new) = self
                        .legs
                        .iter()
                        .find(|leg| leg.stands_for == Some(old) && leg.hedge_for.is_none())
                        .map(|leg| leg.key)
                    {
                        self.release(legs, new, now, out);
                    }
                    if let Some(index) = self.leg_index(old) {
                        self.legs[index].leaving = None;
                    }
                    self.stats.quality.given_up += 1;
                }
            }
        }
        // A hedge whose leg came back is released once that leg has been
        // steady for the level wait: it is the spare again, unless its edge
        // sent it away.
        while let Some(hedge) = self
            .legs
            .iter()
            .find(|leg| {
                leg.hedge_for.is_some_and(|cover| {
                    self.legs.iter().any(|covered| {
                        covered.key == cover
                            && covered
                                .steady_since
                                .is_some_and(|since| now >= since + self.config.level_wait)
                    })
                })
            })
            .map(|leg| leg.key)
        {
            let Some(leg) = self.release(legs, hedge, now, out) else {
                break;
            };
            self.stats.hedges_released += 1;
            if self.spare.is_none() && leg.leaving.is_none() {
                let mut spare = leg.spare.unwrap_or(Spare {
                    node: leg.node,
                    addr: leg.addr,
                    token: None,
                    asked_at: None,
                    misses: 0,
                    next_renew: now,
                });
                spare.asked_at = None;
                spare.misses = 0;
                spare.next_renew = spare.next_renew.max(now);
                self.spare = Some(spare);
            }
        }
        // More legs than wanted (legs asked for in a silence): the newest
        // goes once the others are steady for the level wait.
        let want = self.want as usize;
        while self.legs.iter().filter(|leg| leg.counted()).count() > want {
            let kept_steady = self
                .legs
                .iter()
                .filter(|leg| leg.counted())
                .take(want)
                .all(|leg| {
                    leg.steady_since
                        .is_some_and(|since| now >= since + self.config.level_wait)
                });
            let newest = self
                .legs
                .iter()
                .rev()
                .find(|leg| leg.counted())
                .map(|leg| leg.key);
            match newest {
                Some(newest) if kept_steady => {
                    self.release(legs, newest, now, out);
                }
                _ => break,
            }
        }
    }

    /// The first leaving leg whose move can end: a leg added for it is
    /// live and level with it (or steady for the level wait, not after a
    /// rebalance), with no hole older than its first part open; or, for a
    /// stream of objects, has brought its first object. Else a rebalance
    /// past its give-up.
    fn next_handover<S: SessionLegs>(
        &self,
        legs: &S,
        hole: Option<u64>,
        now: Instant,
    ) -> Option<Handover> {
        for old in &self.legs {
            let Some((_, moved_at)) = old.leaving.as_ref() else {
                continue;
            };
            let rebalance = old.rebalance();
            let taker = self.legs.iter().find(|new| {
                if new.stands_for != Some(old.key)
                    || new.leaving.is_some()
                    || new.hedge_for.is_some()
                    || !new.live
                {
                    return false;
                }
                if self.objects {
                    return true;
                }
                let first = Self::view(legs, new.key, now).first_part;
                let holes_ok = match (hole, first) {
                    (Some(hole), Some(first)) => hole >= first,
                    (Some(_), None) => false,
                    (None, _) => true,
                };
                holes_ok
                    && (legs.level(new.key, old.key)
                        || (!rebalance
                            && new
                                .steady_since
                                .is_some_and(|since| now >= since + self.config.level_wait)))
            });
            if let Some(new) = taker {
                return Some(Handover::Release {
                    old: old.key,
                    new: new.key,
                    moved_at: *moved_at,
                });
            }
            if rebalance && now >= *moved_at + self.config.rebalance_giveup {
                return Some(Handover::GiveUp { old: old.key });
            }
            if old.leaving.as_ref().is_some_and(|(why, _)| quality_reason(why))
                && now >= *moved_at + self.config.quality_giveup
            {
                return Some(Handover::Keep { old: old.key });
            }
        }
        None
    }

    fn request_timers(&mut self, now: Instant, out: &mut Vec<Action>) {
        let Some(mut request) = self.request.take() else {
            return;
        };
        if now >= request.deadline {
            self.request = Some(request);
            self.fail_request(now);
            return;
        }
        if self.config.assignment_udp_port != 0 && now >= request.next_repeat {
            for index in request.pool[..request.asked].iter().copied() {
                if let Some(to) = self.entry_addr(index) {
                    out.push(Action::Send {
                        to,
                        datagram: request.datagram.clone(),
                    });
                }
            }
            request.repeats += 1;
            request.next_repeat = now + self.request_wait(&request);
        }
        // The first entry has not answered: the next one is asked too.
        if request.next_entry_at.is_some_and(|at| now >= at) {
            request.next_entry_at = None;
            self.ask_entry(&mut request, out);
        }
        if request.https_at.is_some_and(|at| now >= at) {
            request.https_at = None;
            if let Some(first) = request.pool.first() {
                out.push(Action::Https {
                    id: request.nonce,
                    entry: self.entries[*first].url.clone(),
                    path: self.https_path(&request),
                });
            }
        }
        self.request = Some(request);
    }

    /// Steps 1 to 3: ask when legs or the spare are missing.
    fn maybe_ask<S: SessionLegs>(&mut self, legs: &mut S, now: Instant, out: &mut Vec<Action>) {
        if self.request.is_some() || self.pending_answer.is_some() {
            return;
        }
        let missing = self.missing();
        let spare = self.spare_wanted();
        let place_of = self.unplaced();
        if missing == 0 && !spare && place_of.is_none() {
            self.deferred_ask = None;
            // Away from home: is the home region back?
            if self
                .away_since
                .is_some_and(|since| now >= since + HOME_HOLD)
                && now >= self.next_home_probe
            {
                self.next_home_probe = now + HOME_PROBE_EVERY;
                let paths = self.full_paths();
                self.send_request(
                    RequestKind::HomeProbe,
                    paths,
                    self.config.spare,
                    Vec::new(),
                    Vec::new(),
                    None,
                    now,
                    out,
                );
            }
            return;
        }
        if self.retry_at.is_some_and(|at| now < at) {
            return;
        }
        let leaving = self.legs.iter().any(|leg| leg.leaving.is_some());
        let urgent = leaving || !self.any_delivering(legs, now, None);
        if !urgent {
            // Beside delivering legs with nothing leaving: spread by session.
            let at = *self
                .deferred_ask
                .get_or_insert(now + spread(&self.config.session, 0, ASK_SPREAD));
            if now < at {
                return;
            }
        }
        let skip = self.skipped(now);
        // The hedge leg whose place is asked for is in neither the skip
        // set nor the legs held: the entry may name its edge again.
        let hold: Vec<String> = self
            .legs
            .iter()
            .filter(|leg| leg.counted() && Some(leg.key) != place_of)
            .map(|leg| leg.node.clone())
            .collect();
        let paths = missing + u8::from(place_of.is_some());
        // A request that names the spare it holds asks for one: the entry
        // keeps it while its edge passes the gate.
        let spare_held = self
            .spare
            .as_ref()
            .filter(|_| self.config.spare)
            .map(|spare| spare.node.clone());
        let spare = spare || spare_held.is_some();
        self.send_request(
            RequestKind::Normal,
            paths,
            spare,
            skip,
            hold,
            Some((spare_held, place_of)),
            now,
            out,
        );
    }

    fn compute_wake(&self, now: Instant) -> Instant {
        let far = now + Duration::from_secs(3_600);
        let mut wake = far;
        let mut at = |instant: Option<Instant>| {
            if let Some(instant) = instant {
                wake = wake.min(instant);
            }
        };
        if let Some(request) = &self.request {
            at(Some(request.deadline));
            at(request.next_entry_at);
            at(request.https_at);
            if self.config.assignment_udp_port != 0 {
                at(Some(request.next_repeat));
            }
        }
        at(self.ask_wake());
        for (instant, _, _) in &self.delayed {
            at(Some(*instant));
        }
        at(self.hedge_due.map(|(instant, _)| instant));
        at(self.swap_at);
        if let Some(spare) = &self.spare {
            at(Some(match spare.asked_at {
                Some(asked) => asked + self.token_wait(),
                None => spare.next_renew,
            }));
        }
        at(self.silence.as_ref().map(|silence| silence.check_at));
        at(self.answered.as_ref().map(|(_, until)| *until));
        for skip in self.skip.values() {
            at(skip.until);
        }
        let checking = self
            .legs
            .iter()
            .any(|leg| leg.leaving.is_some() || leg.hedge_for.is_some())
            || self.held() > self.want as usize;
        if checking {
            at(Some(self.next_check.max(now)));
        }
        wake
    }

    /// When [`Self::maybe_ask`] will act next, as it decides.
    fn ask_wake(&self) -> Option<Instant> {
        if self.request.is_some() || self.pending_answer.is_some() {
            return None;
        }
        if self.missing() == 0 && !self.spare_wanted() && self.unplaced().is_none() {
            return self
                .away_since
                .map(|since| self.next_home_probe.max(since + HOME_HOLD));
        }
        match (self.retry_at, self.deferred_ask) {
            (Some(retry), Some(deferred)) => Some(retry.max(deferred)),
            (retry, deferred) => retry.or(deferred),
        }
    }
}

/// A two-path session's time on fewer than two delivering legs (B 2.8
/// `single_path_ms`, `single_path_spans`), for the clients' reports. A leg
/// delivers while its last media is at most `window` old (max(20 ms, 8
/// part intervals), as [`LegView::delivering`]). Time is counted only while
/// two legs are wanted, and from the session's first media plus a join
/// allowance (the token wait: a clean join's second leg brings its first
/// part within it), or from the moment two legs first deliver together if
/// that is sooner. A session that never gets its second leg (a short grant
/// for the gate or for one edge, a join whose second edge is dead in a
/// region with no third) is on one path from the end of the allowance
/// (review of B Y2, finding 5). A span never begins before the last moment
/// two legs were seen delivering: a leg released with no replacement yet (a
/// drained leg let go for want of a cache) leaves no second leg to date the
/// span by.
#[derive(Debug, Clone)]
pub struct SinglePath {
    allowance: Duration,
    /// When time on one path begins to count.
    counts_from: Option<Instant>,
    /// The last observation with two legs delivering.
    two_seen: Option<Instant>,
    open: Option<Instant>,
    /// Each span, closed.
    pub spans: Vec<(Instant, Instant)>,
    pub total: Duration,
}

impl SinglePath {
    /// `allowance`: the join's own time after the first media, `--token-ms`.
    pub fn new(allowance: Duration) -> Self {
        Self {
            allowance,
            counts_from: None,
            two_seen: None,
            open: None,
            spans: Vec::new(),
            total: Duration::ZERO,
        }
    }

    /// Judge the session now from the last media of each leg it holds
    /// (live or leaving); `two_paths`: two legs are wanted now. Call it at
    /// every media datagram and every wake: a span ends at the datagram
    /// that brings a second leg back, and begins when the second-freshest
    /// leg's media went stale (or, with no second leg, when two legs were
    /// last seen delivering, else when time begins to count).
    pub fn observe(
        &mut self,
        now: Instant,
        window: Duration,
        lasts: impl IntoIterator<Item = Instant>,
        two_paths: bool,
    ) {
        let (mut first, mut second): (Option<Instant>, Option<Instant>) = (None, None);
        let mut delivering = 0;
        for last in lasts {
            if now.saturating_duration_since(last) <= window {
                delivering += 1;
            }
            if first.is_none_or(|first| last > first) {
                second = first;
                first = Some(last);
            } else if second.is_none_or(|second| last > second) {
                second = Some(last);
            }
        }
        if delivering >= 2 {
            self.counts_from = Some(self.counts_from.map_or(now, |from| from.min(now)));
            self.two_seen = Some(now);
        } else if self.counts_from.is_none() {
            self.counts_from = first.map(|first| first + self.allowance);
        }
        if delivering >= 2 || !two_paths {
            self.close(now);
            return;
        }
        let Some(from) = self.counts_from.filter(|from| *from <= now) else {
            return;
        };
        if self.open.is_none() {
            // From the second leg's staleness, but not before two legs were
            // last seen delivering: a leg released since (its media is no
            // longer among `lasts`) did not take the time since the join
            // with it (roll_of_three_edges: spans of 13 to 14 s from the
            // join).
            let since = second
                .map_or(from, |second| (second + window).max(from))
                .max(self.two_seen.unwrap_or(from));
            self.open = Some(since.min(now));
        }
    }

    /// End an open span at `now` (the end of the run).
    pub fn close(&mut self, now: Instant) {
        if let Some(since) = self.open.take() {
            self.total += now.saturating_duration_since(since);
            self.spans.push((since, now));
        }
    }

    /// Whether the session is on fewer than two legs now.
    pub fn is_open(&self) -> bool {
        self.open.is_some()
    }
}

fn answer_cache(cache: AssignedCache) -> Option<AnswerCache> {
    if cache.failover {
        return Some(AnswerCache {
            node: cache.node_id,
            url: String::new(),
            host: String::new(),
            port: None,
            failover: Some(entry_url(&cache.playback_base_url)),
        });
    }
    Some(AnswerCache {
        host: url_host(&cache.playback_base_url)?,
        node: cache.node_id,
        url: cache.playback_base_url,
        port: None,
        failover: None,
    })
}

/// The JSON of `/api/assignment` (B 2.2).
fn parse_https_answer(body: &[u8]) -> Option<Answer> {
    let json: serde_json::Value = serde_json::from_slice(body).ok()?;
    let port = json["client_udp_port"].as_u64().map(|port| port as u16);
    let mut caches = Vec::new();
    for cache in json["assigned"].as_array()? {
        let Some(node) = cache["node_id"].as_str() else {
            continue;
        };
        let url = cache["playback_base_url"].as_str().unwrap_or_default();
        if cache["failover_entry"].as_bool() == Some(true) {
            caches.push(AnswerCache {
                node: node.to_owned(),
                url: String::new(),
                host: String::new(),
                port: None,
                failover: Some(entry_url(url)),
            });
            continue;
        }
        let Some(host) = url_host(url) else {
            continue;
        };
        caches.push(AnswerCache {
            node: node.to_owned(),
            url: url.to_owned(),
            host,
            port: cache["client_udp_port"].as_u64().map(|port| port as u16),
            failover: None,
        });
    }
    let paths = json["paths"]
        .as_u64()
        .map_or(1, |paths| paths.min(u64::from(MAX_PATHS)) as u8);
    // The spare is named apart; in the answer's order it follows the legs.
    let spare = json["spare"].as_str().map(str::to_owned);
    if let Some(spare) = &spare {
        if let Some(position) = caches.iter().position(|cache| cache.node == *spare) {
            let cache = caches.remove(position);
            let at = (paths as usize).min(caches.len());
            caches.insert(at, cache);
        }
    }
    Some(Answer {
        port,
        caches,
        paths,
        spare: spare.is_some(),
        why: json["why"].as_str().and_then(ShortGrant::from_word),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_udp::ASSIGN_REQUEST_BYTES;
    use std::collections::{BTreeMap, HashSet, VecDeque};

    const STREAM: u64 = 3001;
    const E1: &str = "https://10.0.0.1:19444";
    const E2: &str = "https://10.0.0.2:19444";
    const UDP: u16 = crate::client_udp::DEFAULT_CLIENT_UDP_PORT;

    fn entry_addr(url: &str) -> SocketAddr {
        SocketAddr::new(url_host(url).unwrap().parse().unwrap(), UDP)
    }

    fn edge_addr(n: u8) -> SocketAddr {
        SocketAddr::new(IpAddr::from([10, 0, 1, n]), UDP)
    }

    fn cache(n: u8) -> AssignedCache {
        AssignedCache {
            node_id: format!("e{n}"),
            playback_base_url: format!("https://10.0.1.{n}:19444/live"),
            failover: false,
        }
    }

    /// The failover entry a real entry appends to every answer
    /// (`--failover-entries`, M:21135).
    fn failover_entry() -> AssignedCache {
        AssignedCache {
            node_id: "new-dist-a".into(),
            playback_base_url: "https://10.0.9.1:19444".into(),
            failover: true,
        }
    }

    #[derive(Debug, Clone)]
    struct FakeLeg {
        node: String,
        addr: SocketAddr,
        start: LegStart,
        released: bool,
    }

    /// The session as a script: the test says what each leg does.
    #[derive(Default)]
    struct Fake {
        legs: BTreeMap<LegKey, FakeLeg>,
        views: HashMap<LegKey, LegView>,
        level: HashSet<(LegKey, LegKey)>,
        hole: Option<u64>,
        events: VecDeque<LegEvent>,
        log: Vec<String>,
    }

    impl SessionLegs for Fake {
        fn add_leg(
            &mut self,
            leg: LegKey,
            node: &str,
            addr: SocketAddr,
            start: LegStart,
            _now: Instant,
            send: &mut dyn FnMut(SocketAddr, &[u8]),
        ) {
            assert!(!self.legs.contains_key(&leg), "a key used twice");
            let open = self.legs.values().filter(|leg| !leg.released).count();
            assert!(open < MAX_LEGS, "more than {MAX_LEGS} legs");
            send(addr, format!("fake-sub {node}").as_bytes());
            self.log.push(format!("add {node}"));
            self.legs.insert(
                leg,
                FakeLeg {
                    node: node.to_owned(),
                    addr,
                    start,
                    released: false,
                },
            );
        }

        fn release_leg(
            &mut self,
            leg: LegKey,
            _now: Instant,
            send: &mut dyn FnMut(SocketAddr, &[u8]),
        ) {
            let fake = self.legs.get_mut(&leg).expect("a leg the session holds");
            assert!(!fake.released, "released twice");
            fake.released = true;
            send(fake.addr, b"fake-unsub");
            self.log.push(format!("release {}", fake.node));
        }

        fn takeback_leg(
            &mut self,
            leg: LegKey,
            _now: Instant,
            _send: &mut dyn FnMut(SocketAddr, &[u8]),
        ) {
            self.log.push(format!("takeback {}", self.legs[&leg].node));
        }

        fn reask_leg(
            &mut self,
            leg: LegKey,
            _now: Instant,
            _send: &mut dyn FnMut(SocketAddr, &[u8]),
        ) {
            self.log.push(format!("reask {}", self.legs[&leg].node));
        }

        fn leg(&self, leg: LegKey, _now: Instant) -> Option<LegView> {
            let fake = self.legs.get(&leg)?;
            (!fake.released).then(|| self.views.get(&leg).copied().unwrap_or_default())
        }

        fn level(&self, new: LegKey, old: LegKey) -> bool {
            self.level.contains(&(new, old))
        }

        fn oldest_hole(&self) -> Option<u64> {
            self.hole
        }

        fn next_event(&mut self) -> Option<LegEvent> {
            self.events.pop_front()
        }
    }

    struct World {
        d: Director,
        s: Fake,
        t0: Instant,
        now: Instant,
        out: Vec<Action>,
    }

    impl World {
        fn new(sid: &str, paths: u8, spare: bool) -> Self {
            Self::with(sid, paths, spare, vec![E1.into(), E2.into()])
        }

        fn with(sid: &str, paths: u8, spare: bool, entries: Vec<String>) -> Self {
            let t0 = Instant::now();
            let mut config = DirectorConfig::new(STREAM, sid, entries);
            config.paths = paths;
            config.spare = spare;
            config.nonce_seed = 7;
            Self {
                d: Director::new(config, t0),
                s: Fake::default(),
                t0,
                now: t0,
                out: Vec::new(),
            }
        }

        fn ms(&self) -> u64 {
            (self.now - self.t0).as_millis() as u64
        }

        fn first_entry(&self) -> SocketAddr {
            let index =
                crate::load_assign::entry_for(&self.d.config.entries, &self.d.config.session);
            entry_addr(&self.d.config.entries[index])
        }

        fn poll(&mut self) {
            self.d.poll(&mut self.s, self.now, &mut self.out);
        }

        /// Poll at every wake up to `ms`, as the caller's loop does.
        fn run_to(&mut self, ms: u64) {
            let target = self.t0 + Duration::from_millis(ms);
            let mut polls = 0;
            loop {
                let wake = self.d.next_wake();
                if wake > target {
                    break;
                }
                assert!(wake >= self.now || polls == 0, "a wake in the past");
                self.now = self.now.max(wake);
                self.poll();
                polls += 1;
                assert!(polls < 100_000, "the director spins at {} ms", self.ms());
            }
            self.now = target.max(self.now);
            self.poll();
        }

        fn event(&mut self, event: LegEvent) {
            self.s.events.push_back(event);
            self.poll();
        }

        fn take(&mut self) -> Vec<Action> {
            std::mem::take(&mut self.out)
        }

        /// The control datagrams sent (fake session markers left out).
        fn controls(&mut self) -> Vec<(SocketAddr, Control, usize)> {
            self.take()
                .into_iter()
                .filter_map(|action| match action {
                    Action::Send { to, datagram } => {
                        Control::decode(&datagram).map(|control| (to, control, datagram.len()))
                    }
                    _ => None,
                })
                .collect()
        }

        /// The requests sent (`ASSIGN` without a token): (to, nonce, control).
        fn requests(&mut self) -> Vec<(SocketAddr, u64, Control)> {
            self.controls()
                .into_iter()
                .filter_map(|(to, control, _)| match control {
                    Control::Assign {
                        nonce, token: None, ..
                    } => Some((to, nonce, control)),
                    _ => None,
                })
                .collect()
        }

        fn token_from(&mut self, from: SocketAddr, token: u64) -> bool {
            let datagram = Control::Token {
                stream_id: STREAM,
                token,
            }
            .encode();
            self.d
                .on_datagram(&mut self.s, from, &datagram, self.now, &mut self.out)
        }

        fn answer(
            &mut self,
            from: SocketAddr,
            nonce: u64,
            caches: Vec<AssignedCache>,
            paths: u8,
            spare: bool,
            token: Option<u64>,
        ) {
            self.answer_why(from, nonce, caches, paths, spare, token, None);
        }

        /// An answer that says why it grants fewer legs than asked.
        #[allow(clippy::too_many_arguments)]
        fn answer_why(
            &mut self,
            from: SocketAddr,
            nonce: u64,
            caches: Vec<AssignedCache>,
            paths: u8,
            spare: bool,
            token: Option<u64>,
            why: Option<ShortGrant>,
        ) {
            if let Some(token) = token {
                assert!(
                    self.token_from(from, token),
                    "the entry's token is the director's"
                );
            }
            let datagram = Control::Assigned {
                stream_id: STREAM,
                nonce,
                port: Some(UDP),
                caches,
                paths,
                spare,
                why,
            }
            .encode();
            assert!(self
                .d
                .on_datagram(&mut self.s, from, &datagram, self.now, &mut self.out));
        }

        /// The unreleased leg at `node`.
        fn key(&self, node: &str) -> LegKey {
            *self
                .s
                .legs
                .iter()
                .find(|(_, leg)| leg.node == node && !leg.released)
                .unwrap_or_else(|| panic!("no leg at {node}: {:?}", self.s.log))
                .0
        }

        fn fake(&self, node: &str) -> FakeLeg {
            self.s.legs[&self.key(node)].clone()
        }

        fn view(&mut self, node: &str, change: impl FnOnce(&mut LegView)) {
            let key = self.key(node);
            change(self.s.views.entry(key).or_default());
        }

        /// The leg answers and delivers, steadily.
        fn live(&mut self, node: &str) {
            let key = self.key(node);
            let now = self.now;
            self.view(node, |view| {
                view.delivering = true;
                view.steady = true;
                view.first_part = Some(1_000);
                view.first_media = Some(now);
                view.last_media = Some(now);
                view.last_datagram = Some(now);
                view.lag = Some(Duration::ZERO);
            });
            self.s.events.push_back(LegEvent::Tokened(key));
            self.event(LegEvent::Live {
                leg: key,
                compact: true,
            });
        }

        /// Run to `ms`, the spare answering each renewal of its token.
        fn run_with_spare(&mut self, ms: u64, spare: SocketAddr) {
            while self.ms() < ms {
                self.run_to((self.ms() + 10).min(ms));
                let renewal = self.controls().into_iter().any(|(to, control, _)| {
                    to == spare && matches!(control, Control::Subscribe { token: None, .. })
                });
                if renewal {
                    assert!(self.token_from(spare, 0x5a));
                }
            }
        }

        /// The director's legs by node.
        fn held(&self) -> Vec<String> {
            self.d.legs().iter().map(|leg| leg.node.clone()).collect()
        }

        fn leaving(&self, node: &str) -> bool {
            self.d
                .legs()
                .iter()
                .any(|leg| leg.node == node && leg.leaving.is_some())
        }

        /// A join: the first request answered with `legs` and `spare`, the
        /// legs live and the spare's token in.
        fn join(&mut self, legs: &[u8], spare: Option<u8>) {
            self.poll();
            let requests = self.requests();
            let (to, nonce, _) = requests[0].clone();
            let mut caches: Vec<AssignedCache> = legs.iter().map(|n| cache(*n)).collect();
            caches.extend(spare.map(cache));
            self.answer(
                to,
                nonce,
                caches,
                legs.len() as u8,
                spare.is_some(),
                Some(0xe0),
            );
            for n in legs {
                self.live(&format!("e{n}"));
            }
            if let Some(n) = spare {
                assert!(self.token_from(edge_addr(n), 0x5000 + u64::from(n)));
            }
            self.take();
        }
    }

    #[test]
    fn a_join_asks_one_entry_and_two_edges_and_plays_the_first() {
        let mut w = World::new("join-1", 2, true);
        w.poll();
        let controls = w.controls();
        assert_eq!(controls.len(), 1, "one entry is asked: {controls:?}");
        let (to, request, len) = controls[0].clone();
        assert_eq!(to, w.first_entry());
        assert!(len >= ASSIGN_REQUEST_BYTES, "padded: {len}");
        let Control::Assign {
            nonce,
            paths,
            spare,
            current,
            hold,
            skip,
            token: None,
            ..
        } = request
        else {
            panic!("{request:?}");
        };
        assert_eq!((paths, spare), (2, true));
        assert!(current.is_empty() && hold.is_empty() && skip.is_empty());
        // The answer at 2 ms: two legs and the spare, all asked at once.
        w.run_to(2);
        w.answer(
            to,
            nonce,
            vec![cache(1), cache(2), cache(3)],
            2,
            true,
            Some(0xabc),
        );
        assert_eq!(w.s.log, ["add e1", "add e2"]);
        for node in ["e1", "e2"] {
            let leg = w.fake(node);
            assert_eq!(leg.start.from, StartFrom::OldestHole);
            assert_eq!(leg.start.token, None);
            assert_eq!(
                leg.start.token_wait,
                Duration::from_millis(DEFAULT_TOKEN_MS)
            );
        }
        assert_eq!(w.fake("e2").addr, edge_addr(2));
        let sent = w.controls();
        assert!(sent.iter().any(|(to, control, _)| *to == edge_addr(3)
            && matches!(
                control,
                Control::Subscribe {
                    token: None,
                    from_part: None,
                    ..
                }
            )));
        assert_eq!(w.d.spare(), Some("e3"));
        // The legs and the spare's token: nothing else is asked.
        w.live("e2");
        w.live("e1");
        assert!(w.token_from(edge_addr(3), 0x33));
        w.run_with_spare(60_000, edge_addr(3));
        assert!(w.requests().is_empty());
        assert_eq!(w.held(), ["e1", "e2"]);
        assert_eq!(w.d.stats.requests, 1);
        assert_eq!(w.d.first_answer.map(|(_, via)| via), Some(Via::Udp));
    }

    #[test]
    fn the_second_entry_is_asked_after_the_first_retry() {
        let mut w = World::new("second-entry", 2, true);
        w.poll();
        let first = w.first_entry();
        let second = [E1, E2]
            .into_iter()
            .map(entry_addr)
            .find(|addr| *addr != first)
            .unwrap();
        let requests = w.requests();
        assert_eq!(requests.len(), 1);
        let nonce = requests[0].1;
        w.run_to(24);
        assert!(w.requests().is_empty());
        w.run_to(25);
        let mut to: Vec<SocketAddr> = w.requests().iter().map(|(to, _, _)| *to).collect();
        to.sort();
        let mut both = vec![first, second];
        both.sort();
        assert_eq!(to, both, "the first again and the second at 25 ms");
        // HTTPS to the first entry at 50 ms.
        w.run_to(49);
        assert!(!w
            .out
            .iter()
            .any(|action| matches!(action, Action::Https { .. })));
        w.run_to(50);
        let https: Vec<Action> = w
            .take()
            .into_iter()
            .filter(|action| matches!(action, Action::Https { .. }))
            .collect();
        let entry = w.d.config.entries
            [crate::load_assign::entry_for(&w.d.config.entries, "second-entry")]
        .clone();
        assert_eq!(
            https,
            [Action::Https {
                id: nonce,
                entry,
                path: "/api/assignment?stream=3001&sid=second-entry&paths=2&spare=1".into(),
            }]
        );
        // The second answers: its answer is taken and only it confirms.
        w.run_to(60);
        w.answer(
            second,
            nonce,
            vec![cache(1), cache(2), cache(3)],
            2,
            true,
            Some(0x22),
        );
        assert_eq!(w.held(), ["e1", "e2"]);
        let confirmations: Vec<(SocketAddr, Option<u64>)> = w
            .controls()
            .into_iter()
            .filter_map(|(to, control, _)| match control {
                Control::Assign {
                    token: Some(token), ..
                } => Some((to, Some(token))),
                _ => None,
            })
            .collect();
        assert_eq!(confirmations, [(second, Some(0x22))]);
        // The HTTPS answer that comes later changes nothing.
        let body = br#"{"assigned":[{"node_id":"e4","playback_base_url":"https://10.0.1.4:19444"}],"client_udp_port":19446}"#;
        w.d.on_https(&mut w.s, nonce, Some(body), w.now, &mut w.out);
        assert_eq!(w.held(), ["e1", "e2"]);

        // An entry that failed in the last 10 s: the next one at once too.
        let mut w = World::new("second-entry", 2, true);
        w.poll();
        w.run_to(1_000);
        assert_eq!(w.d.stats.assignment_failures, 1);
        w.take();
        w.run_to(1_200);
        let mut to: Vec<SocketAddr> = w.requests().iter().map(|(to, _, _)| *to).collect();
        to.sort();
        assert_eq!(to, both, "both at once after a failure");
    }

    #[test]
    fn the_udp_assignment_is_repeated_until_answered() {
        let mut w = World::with("s-1", 1, false, vec!["https://127.0.0.1:19444".into()]);
        let entry = entry_addr("https://127.0.0.1:19444");
        let mut seen = Vec::new();
        let mut nonce = 0;
        for ms in 0..=80 {
            w.run_to(ms);
            for (to, request_nonce, _) in w.requests() {
                assert_eq!(to, entry);
                seen.push(ms);
                nonce = request_nonce;
            }
        }
        // Repeated after --first-retry-ms, then twice as long each time.
        assert_eq!(seen, [0, 25, 75]);
        // The node's token, an answer to another request, then the answer.
        assert!(w.token_from(entry, 0xabc));
        let answer = |nonce| {
            Control::Assigned {
                stream_id: STREAM,
                nonce,
                port: Some(19_446),
                caches: vec![AssignedCache {
                    node_id: "lon-edge-2".into(),
                    playback_base_url: "https://127.0.0.1:19444/live".into(),
                    failover: false,
                }],
                paths: 1,
                spare: false,
                why: None,
            }
            .encode()
        };
        assert!(w
            .d
            .on_datagram(&mut w.s, entry, &answer(nonce ^ 1), w.now, &mut w.out));
        assert!(w.held().is_empty(), "another request's answer is not taken");
        assert!(w
            .d
            .on_datagram(&mut w.s, entry, &answer(nonce), w.now, &mut w.out));
        assert_eq!(w.held(), ["lon-edge-2"]);
        assert_eq!(
            w.fake("lon-edge-2").addr,
            "127.0.0.1:19446".parse().unwrap()
        );
        // The confirmation: the token, the cache taken, unpadded.
        let confirmation = w
            .controls()
            .into_iter()
            .find(|(_, control, _)| matches!(control, Control::Assign { token: Some(_), .. }))
            .unwrap();
        assert_eq!(confirmation.0, entry);
        assert!(confirmation.2 < 200, "{}", confirmation.2);
        let Control::Assign { token, current, .. } = confirmation.1 else {
            unreachable!()
        };
        assert_eq!(token, Some(0xabc));
        assert_eq!(current, ["lon-edge-2"]);
    }

    #[test]
    fn a_move_keeps_the_old_edge_until_the_new_one_is_level() {
        let mut w = World::new("move-1", 2, true);
        w.join(&[1, 2], Some(3));
        let e1 = w.key("e1");
        w.event(LegEvent::Moved {
            leg: e1,
            reason: "draining".into(),
        });
        // At once: one leg, the moved edge skipped, the other held.
        let requests = w.requests();
        assert_eq!(requests.len(), 1);
        let (to, nonce, request) = requests[0].clone();
        let Control::Assign {
            paths,
            spare,
            skip,
            hold,
            ..
        } = request
        else {
            unreachable!()
        };
        // A spare is held: the request names it (`k=`) and asks for one,
        // so that the entry keeps it or names another.
        assert_eq!((paths, spare), (1, true));
        assert_eq!((skip, hold), (vec!["e1".to_owned()], vec!["e2".to_owned()]));
        w.answer(to, nonce, vec![cache(4)], 1, false, None);
        assert_eq!(w.fake("e4").start.from, StartFrom::AfterNewest);
        w.live("e4");
        w.view("e4", |view| view.steady = false);
        // Not level: the old leg stays, whatever the time.
        w.run_to(w.ms() + 5_000);
        assert!(w.leaving("e1"));
        assert!(!w.s.legs[&e1].released);
        // Level, but a hole older than the new leg's first part is open.
        let e4 = w.key("e4");
        w.s.level.insert((e4, e1));
        w.s.hole = Some(999);
        w.run_to(w.ms() + 100);
        assert!(!w.s.legs[&e1].released);
        w.s.hole = Some(1_000);
        w.run_to(w.ms() + 5);
        assert!(w.s.legs[&e1].released);
        assert_eq!(w.held(), ["e2", "e4"]);
        let handover = w.d.replacements.last().unwrap();
        assert_eq!(handover.kind, ReplacementKind::Handover);
        assert_eq!((handover.from.as_str(), handover.to.as_str()), ("e1", "e4"));
        assert_eq!(handover.reason, "draining");
        // The moved edge is skipped 3 s more.
        let released = w.ms();
        assert!(w.d.skipped(w.now).contains(&"e1".to_owned()));
        w.run_to(released + 2_999);
        assert!(w.d.skipped(w.now).contains(&"e1".to_owned()));
        w.run_to(released + 3_000);
        assert!(!w.d.skipped(w.now).contains(&"e1".to_owned()));
    }

    #[test]
    fn a_drain_has_no_release_timer() {
        let mut w = World::new("drain-1", 1, false);
        w.join(&[1], None);
        let e1 = w.key("e1");
        w.event(LegEvent::Moved {
            leg: e1,
            reason: "draining".into(),
        });
        // No cache anywhere: the leg stays, and the entry is asked every
        // second.
        let mut asked = Vec::new();
        for ms in (w.ms()..w.ms() + 20_000).step_by(5) {
            w.run_to(ms);
            for (to, nonce, _) in w.requests() {
                asked.push(ms);
                w.answer(to, nonce, Vec::new(), 1, false, None);
            }
        }
        assert!(!w.s.legs[&e1].released);
        assert!(w.leaving("e1"));
        assert!(asked.len() >= 19, "{asked:?}");
        for pair in asked.windows(2) {
            assert!((1_000..=1_005).contains(&(pair[1] - pair[0])), "{asked:?}");
        }
        // A replacement that never levels: still no release by the client.
        let (to, nonce) = loop {
            w.run_to(w.ms() + 5);
            if let Some((to, nonce, _)) = w.requests().pop() {
                break (to, nonce);
            }
        };
        w.answer(to, nonce, vec![cache(2)], 1, false, None);
        w.live("e2");
        w.view("e2", |view| view.steady = false);
        w.run_to(w.ms() + 20_000);
        assert!(!w.s.legs[&e1].released);
    }

    #[test]
    fn a_leaving_leg_is_released_at_once_when_no_cache_is_named_and_another_delivers() {
        let mut w = World::new("no-cache-1", 2, true);
        w.join(&[1, 2], Some(3));
        let e1 = w.key("e1");
        w.event(LegEvent::Moved {
            leg: e1,
            reason: "draining".into(),
        });
        // A real entry appends its failover entries to every answer.
        let (to, nonce, _) = w.requests()[0].clone();
        w.answer(to, nonce, vec![failover_entry()], 1, false, None);
        assert!(w.s.legs[&e1].released);
        assert_eq!(w.held(), ["e2"]);
        assert_eq!(w.d.stats.failovers, 0);
        // A one-leg listener sent away for lost parents asks the edge to
        // take it back instead.
        let mut w = World::new("no-cache-2", 1, false);
        w.join(&[1], None);
        let e1 = w.key("e1");
        w.event(LegEvent::Moved {
            leg: e1,
            reason: "parents-lost".into(),
        });
        let (to, nonce, _) = w.requests()[0].clone();
        w.answer(to, nonce, vec![failover_entry()], 1, false, None);
        assert!(!w.s.legs[&e1].released);
        assert_eq!(w.s.log.last().unwrap(), "takeback e1");
        assert_eq!(w.d.stats.failovers, 0, "it stays in its region");
    }

    /// Fix 4, F6a/F6b: the redundant gate refused every edge with room, and
    /// the answer names only a spare (no gate on a spare). A rebalanced
    /// leg is taken back at once (step 6). A drained leg is kept, served by
    /// its draining edge (make before break: nobody on one path), and its
    /// replacement asked for at 250 ms, 500 ms, 1 s, 2 s, then 5 s, not on
    /// the 1 s, 2 s, 4 s backoff (lp2c10-a-drain: two such legs stayed until
    /// their edge cut them; released at once instead, rolls left listeners
    /// on one path 3 to 25 s). When room comes, the hand-over is as any.
    #[test]
    fn an_answer_that_grants_no_leg_keeps_a_drained_leg_and_asks_again_soon() {
        for reason in ["draining", "rebalance"] {
            let mut w = World::new(&format!("gate-{reason}"), 2, true);
            w.join(&[1, 2], Some(3));
            let e1 = w.key("e1");
            w.event(LegEvent::Moved {
                leg: e1,
                reason: reason.into(),
            });
            let (to, nonce, _) = w.requests()[0].clone();
            w.answer_why(
                to,
                nonce,
                vec![cache(4), failover_entry()],
                0,
                true,
                None,
                Some(ShortGrant::Gate),
            );
            assert!(!w.s.legs[&e1].released, "{reason}: {:?}", w.s.log);
            assert_eq!(w.d.stats.failovers, 0, "{reason}: it stays in its region");
            assert!(!w.held().iter().any(|node| node == "e4"), "{reason}: no leg at the spare's edge");
            if reason == "rebalance" {
                assert_eq!(w.s.log.last().unwrap(), "takeback e1");
                continue;
            }
            assert!(w.leaving("e1"));
            let answered = w.ms();
            let mut asked = Vec::new();
            for round in 0..4 {
                let at = loop {
                    w.run_to(w.ms() + 1);
                    if let Some((to, nonce, _)) = w.requests().first().cloned() {
                        if round < 3 {
                            w.answer_why(to, nonce, vec![cache(4)], 0, true, None, Some(ShortGrant::Gate));
                        } else {
                            // Room: the replacement is granted.
                            w.answer(to, nonce, vec![cache(4)], 1, false, None);
                        }
                        break w.ms();
                    }
                    assert!(w.ms() < answered + 10_000, "asked again");
                };
                asked.push(at);
                assert!(!w.s.legs[&e1].released, "kept while it waits: {:?}", w.s.log);
            }
            let waits: Vec<u64> = std::iter::once(answered)
                .chain(asked.iter().copied())
                .collect::<Vec<_>>()
                .windows(2)
                .map(|pair| pair[1] - pair[0])
                .collect();
            for (wait, backoff) in waits.iter().zip([250, 500, 1_000, 2_000]) {
                assert!(wait.abs_diff(backoff) <= 2, "{waits:?}");
            }
            // The replacement joins and is level: the drained leg goes.
            w.live("e4");
            let e4 = w.key("e4");
            w.s.level.insert((e4, e1));
            w.run_to(w.ms() + 50);
            assert!(w.s.legs[&e1].released, "{:?}", w.s.log);
        }
    }

    #[test]
    fn a_rebalance_with_no_cache_returns_at_once() {
        let mut w = World::new("rebalance-1", 2, true);
        w.join(&[1, 2], Some(3));
        let e1 = w.key("e1");
        w.event(LegEvent::Moved {
            leg: e1,
            reason: "rebalance".into(),
        });
        assert!(w.d.skipped(w.now).contains(&"e1".to_owned()));
        let (to, nonce, _) = w.requests()[0].clone();
        w.answer(to, nonce, vec![failover_entry()], 1, false, None);
        assert_eq!(w.s.log.last().unwrap(), "takeback e1");
        assert_eq!(w.d.stats.failovers, 0);
        assert!(!w.leaving("e1"));
        assert!(!w.d.skipped(w.now).contains(&"e1".to_owned()));
        assert_eq!(w.d.stats.takebacks, 1);
        assert_eq!(
            w.d.replacements.last().unwrap().kind,
            ReplacementKind::Takeback
        );
        // It holds its two legs again: nothing is asked.
        w.s.events.push_back(LegEvent::TakenBack(e1));
        w.run_to(w.ms() + 10_000);
        assert!(w.requests().is_empty());
        assert_eq!(w.held(), ["e1", "e2"]);
    }

    #[test]
    fn a_rebalance_that_is_not_level_returns_at_three_seconds() {
        let mut w = World::new("rebalance-2", 2, true);
        w.join(&[1, 2], Some(3));
        let e1 = w.key("e1");
        let moved = w.ms();
        w.event(LegEvent::Moved {
            leg: e1,
            reason: "rebalance".into(),
        });
        let (to, nonce, _) = w.requests()[0].clone();
        w.answer(to, nonce, vec![cache(4)], 1, false, None);
        // Steady, never level: the level wait does not apply to a rebalance.
        w.live("e4");
        let e4 = w.key("e4");
        w.run_to(moved + 2_995);
        assert!(!w.s.legs[&e1].released && !w.s.legs[&e4].released);
        w.run_to(moved + 3_005);
        assert!(w.s.legs[&e4].released, "{:?}", w.s.log);
        assert!(!w.s.legs[&e1].released);
        assert!(w.s.log.contains(&"takeback e1".to_owned()));
        assert_eq!(w.held(), ["e1", "e2"]);
        // A level replacement takes over before then.
        let mut w = World::new("rebalance-3", 2, true);
        w.join(&[1, 2], Some(3));
        let e1 = w.key("e1");
        w.event(LegEvent::Moved {
            leg: e1,
            reason: "rebalance".into(),
        });
        let (to, nonce, _) = w.requests()[0].clone();
        w.answer(to, nonce, vec![cache(4)], 1, false, None);
        w.live("e4");
        let e4 = w.key("e4");
        w.s.level.insert((e4, e1));
        w.run_to(w.ms() + 10);
        assert!(w.s.legs[&e1].released);
        assert_eq!(w.held(), ["e2", "e4"]);
    }

    #[test]
    fn a_dead_leg_is_replaced_from_the_spare_and_a_new_spare_asked() {
        let mut w = World::new("dead-1", 2, true);
        w.join(&[1, 2], Some(3));
        let e1 = w.key("e1");
        w.view("e1", |view| view.delivering = false);
        let due = w.ms();
        w.event(LegEvent::HedgeDue(e1));
        // Hedged at the spare within 20 ms, with its token, beside e2.
        let mut hedged_at = None;
        for ms in due..=due + 20 {
            w.run_to(ms);
            if w.s.log.contains(&"add e3".to_owned()) {
                hedged_at = Some(ms);
                break;
            }
        }
        let hedged_at = hedged_at.expect("hedged within 20 ms");
        let spread = spread("dead-1", 1, HEDGE_SPREAD).as_micros() as u64;
        assert_eq!(hedged_at - due, spread.div_ceil(1_000));
        let hedge = w.fake("e3");
        assert_eq!(hedge.start.token, Some(0x5003));
        assert_eq!(hedge.start.from, StartFrom::AfterNewest);
        assert_eq!(w.d.spare(), None);
        assert_eq!(w.d.stats.hedges, 1);
        // The spare is in use: no new one is asked while the hedge is out.
        w.live("e3");
        w.run_to(hedged_at + 150);
        assert!(w.requests().is_empty());
        // Dead at 200 ms: the hedge has its place; a new spare is asked.
        w.event(LegEvent::Failed {
            leg: e1,
            cause: LegFailure::Dead,
        });
        assert!(w.s.legs[&e1].released);
        assert_eq!(w.held(), ["e2", "e3"]);
        let replaced = w.d.replacements.last().unwrap().clone();
        assert_eq!(replaced.kind, ReplacementKind::Hedge);
        assert_eq!((replaced.from.as_str(), replaced.to.as_str()), ("e1", "e3"));
        let failed = w.ms();
        let (to, nonce, request) = loop {
            w.run_to(w.ms() + 1);
            if let Some(request) = w.requests().pop() {
                break request;
            }
            assert!(w.ms() <= failed + 250, "asked within 250 ms");
        };
        let Control::Assign {
            paths,
            spare,
            skip,
            hold,
            ..
        } = request
        else {
            unreachable!()
        };
        // The hedge's place is asked for as a mover's (skip the dead leg,
        // hold the other; the hedge's edge in neither), with a new spare.
        assert_eq!((paths, spare), (1, true));
        assert_eq!(skip, ["e1"]);
        assert_eq!(hold, ["e2"]);
        // The entry names the hedge's own edge: nothing moves.
        w.answer(to, nonce, vec![cache(3), cache(5)], 1, true, Some(0x77));
        assert_eq!(w.d.spare(), Some("e5"));
        assert_eq!(w.held(), ["e2", "e3"], "the hedge keeps its place");
        assert_eq!(w.d.stats.apportion_moves, 0);
        // The confirmation names the hedge leg, so the entry charges it.
        let current = w
            .controls()
            .into_iter()
            .find_map(|(_, control, _)| match control {
                Control::Assign {
                    token: Some(_),
                    current,
                    ..
                } => Some(current),
                _ => None,
            })
            .unwrap();
        assert_eq!(current, ["e2", "e3"]);
        // e1 failed by itself: skipped 30 s.
        w.run_to(failed + 29_000);
        assert!(w.d.skipped(w.now).contains(&"e1".to_owned()));
        w.run_to(failed + 30_001);
        assert!(!w.d.skipped(w.now).contains(&"e1".to_owned()));
    }

    /// The lead's decision after integration 2: a hedge that took a dead
    /// leg's place, apportioned by the entry to another edge, moves there
    /// make before break, and its edge is the spare again; no other spare is
    /// asked for meanwhile.
    #[test]
    fn a_hedge_s_place_apportioned_elsewhere_moves_and_its_edge_is_the_spare_again() {
        let mut w = World::new("apportion-1", 2, true);
        w.join(&[1, 2], Some(3));
        let e1 = w.key("e1");
        w.view("e1", |view| view.delivering = false);
        w.event(LegEvent::HedgeDue(e1));
        w.run_to(w.ms() + 25);
        w.live("e3");
        w.event(LegEvent::Failed {
            leg: e1,
            cause: LegFailure::Dead,
        });
        let failed = w.ms();
        let (to, nonce, request) = loop {
            w.run_to(w.ms() + 1);
            if let Some(request) = w.requests().pop() {
                break request;
            }
            assert!(w.ms() <= failed + 250, "asked within 250 ms");
        };
        // The hedge's edge is named as where the place is (`a=`).
        assert!(matches!(
            request,
            Control::Assign {
                paths: 1,
                spare: true,
                spare_held: None,
                place_at: Some(ref at),
                ..
            } if at == "e3"
        ));
        // The entry apportions the place to e4 (and names e5 as a spare).
        w.answer(to, nonce, vec![cache(4), cache(5)], 1, true, Some(0x77));
        assert_eq!(w.d.stats.apportion_moves, 1);
        assert!(w.leaving("e3"), "{:?}", w.held());
        assert_eq!(w.held(), ["e2", "e3", "e4"]);
        assert_eq!(w.d.spare(), None, "the hedge's edge is to be the spare");
        // Nothing more is asked while e3 is leaving for its place.
        w.run_to(w.ms() + 600);
        assert!(w.requests().is_empty());
        assert!(!w.s.legs[&w.key("e3")].released, "make before break");
        // e4 delivers and is level with e3: e3 goes, and is the spare again.
        w.live("e4");
        let (e3, e4) = (w.key("e3"), w.key("e4"));
        w.s.level.insert((e4, e3));
        w.run_to(w.ms() + 10);
        assert_eq!(w.held(), ["e2", "e4"]);
        assert_eq!(w.d.spare(), Some("e3"));
        let moved = w.d.replacements.last().unwrap().clone();
        assert_eq!(
            (moved.from.as_str(), moved.to.as_str(), moved.reason.as_str()),
            ("e3", "e4", APPORTION)
        );
        assert_eq!(moved.kind, ReplacementKind::Handover);
        // Its token is fetched again (a tokenless SUB to e3).
        assert!(w.s.legs[&e3].released);
        w.run_to(w.ms() + 300);
        assert!(w.requests().is_empty(), "no other spare asked");
    }

    /// A new leg for an apportioned place that fails leaves the hedge leg
    /// where it is, holding its place.
    #[test]
    fn a_failed_leg_for_an_apportioned_place_leaves_the_hedge_where_it_is() {
        let mut w = World::new("apportion-2", 2, true);
        w.join(&[1, 2], Some(3));
        let e1 = w.key("e1");
        w.view("e1", |view| view.delivering = false);
        w.event(LegEvent::HedgeDue(e1));
        w.run_to(w.ms() + 25);
        w.live("e3");
        w.event(LegEvent::Failed {
            leg: e1,
            cause: LegFailure::Dead,
        });
        let (to, nonce, _) = loop {
            w.run_to(w.ms() + 1);
            if let Some(request) = w.requests().pop() {
                break request;
            }
        };
        w.answer(to, nonce, vec![cache(4), cache(5)], 1, true, Some(0x77));
        assert!(w.leaving("e3"));
        let e4 = w.key("e4");
        w.event(LegEvent::Refused {
            leg: e4,
            reason: "edge-busy".into(),
        });
        assert!(!w.leaving("e3"), "{:?}", w.held());
        assert_eq!(w.held(), ["e2", "e3"]);
        // A spare is asked for again, with no place to apportion.
        let (_, _, request) = loop {
            w.run_to(w.ms() + 1);
            if let Some(request) = w.requests().pop() {
                break request;
            }
        };
        assert!(matches!(request, Control::Assign { paths: 0, spare: true, .. }), "{request:?}");
    }

    /// A request for a leg names the spare the listener holds (`k=`): an
    /// answer that names the same spare keeps it (its token too); one that
    /// names another replaces it.
    #[test]
    fn a_request_names_the_spare_held_and_the_answer_may_replace_it() {
        let mut w = World::new("keep-1", 2, true);
        w.join(&[1, 2], Some(3));
        assert_eq!(w.d.spare(), Some("e3"));
        let e1 = w.key("e1");
        w.event(LegEvent::Moved {
            leg: e1,
            reason: "draining".into(),
        });
        let (to, nonce, request) = loop {
            w.run_to(w.ms() + 1);
            if let Some(request) = w.requests().pop() {
                break request;
            }
        };
        let Control::Assign {
            paths,
            spare,
            spare_held,
            ..
        } = request.clone()
        else {
            unreachable!()
        };
        assert_eq!((paths, spare, spare_held.as_deref()), (1, true, Some("e3")));
        assert!(Control::decode(&request.encode()) == Some(request));
        // The same spare: kept, no new token asked.
        w.answer(to, nonce, vec![cache(4), cache(3)], 1, true, Some(0x77));
        assert_eq!(w.d.spare(), Some("e3"));
        assert_eq!(w.d.stats.spares_rechosen, 0);
        assert!(!w
            .controls()
            .iter()
            .any(|(to, control, _)| *to == edge_addr(3)
                && matches!(control, Control::Subscribe { token: None, .. })));
        // Another move: the entry names e5, under the gate where e3 is not.
        w.live("e4");
        let e4 = w.key("e4");
        w.s.level.insert((e4, e1));
        w.run_to(w.ms() + 10);
        let e2 = w.key("e2");
        w.event(LegEvent::Moved {
            leg: e2,
            reason: "draining".into(),
        });
        let (to, nonce, _) = loop {
            w.run_to(w.ms() + 1);
            if let Some(request) = w.requests().pop() {
                break request;
            }
        };
        w.answer(to, nonce, vec![cache(6), cache(5)], 1, true, Some(0x78));
        assert_eq!(w.d.spare(), Some("e5"));
        assert_eq!(w.d.stats.spares_rechosen, 1);
    }

    #[test]
    fn a_lone_leg_is_hedged_after_two_keepalives() {
        // The session says when (--lone-hedge-after-ms, 170 ms with no
        // datagram); the director hedges from the oldest hole, with no
        // other leg to stand beside.
        let mut w = World::new("lone-1", 1, true);
        w.join(&[1], Some(2));
        let e1 = w.key("e1");
        w.view("e1", |view| view.delivering = false);
        w.s.hole = Some(1_020);
        let due = w.ms();
        w.event(LegEvent::HedgeDue(e1));
        w.run_to(due + 20);
        let hedge = w.fake("e2");
        assert_eq!(hedge.start.from, StartFrom::OldestHole);
        assert_eq!(hedge.start.token, Some(0x5002));
        assert_eq!(
            w.d.legs()
                .iter()
                .find(|leg| leg.node == "e2")
                .unwrap()
                .hedge_for,
            Some(e1)
        );
        // A second verdict on the same silence hedges nothing more.
        w.event(LegEvent::HedgeDue(e1));
        w.run_to(due + 60);
        assert_eq!(
            w.s.log
                .iter()
                .filter(|line| line.starts_with("add"))
                .count(),
            2
        );
        // Without a spare, a lone leg is not hedged.
        let mut w = World::new("lone-2", 1, false);
        w.join(&[1], None);
        let e1 = w.key("e1");
        w.event(LegEvent::HedgeDue(e1));
        w.run_to(w.ms() + 50);
        assert_eq!(w.s.log, ["add e1"]);
    }

    #[test]
    fn a_hedge_whose_leg_comes_back_is_released() {
        let mut w = World::new("back-1", 2, true);
        w.join(&[1, 2], Some(3));
        let e1 = w.key("e1");
        w.view("e1", |view| {
            view.delivering = false;
            view.steady = false;
        });
        w.event(LegEvent::HedgeDue(e1));
        w.run_to(w.ms() + 20);
        w.live("e3");
        // e1 comes back before 200 ms: three legs until it is steady for
        // the level wait.
        w.view("e1", |view| {
            view.delivering = true;
            view.steady = true;
        });
        let back = w.ms();
        w.run_to(back + 990);
        assert_eq!(w.held(), ["e1", "e2", "e3"]);
        w.run_to(back + 1_010);
        assert_eq!(w.held(), ["e1", "e2"]);
        assert_eq!(w.d.spare(), Some("e3"), "the spare again");
        assert_eq!(w.d.stats.hedges_released, 1);
        assert!(w.requests().is_empty(), "no new spare is asked for");
        // Its token is kept for the next hedge.
        w.view("e2", |view| {
            view.delivering = false;
            view.steady = false;
        });
        let e2 = w.key("e2");
        w.event(LegEvent::HedgeDue(e2));
        w.run_to(w.ms() + 20);
        assert_eq!(w.fake("e3").start.token, Some(0x5003));
    }

    #[test]
    fn skips_end_by_cause_and_the_longest_skipped_is_taken_when_all_are() {
        let mut w = World::new("skips-1", 2, false);
        w.join(&[1, 2], None);
        let start = w.ms();
        // e2 dies beside e1: skipped 30 s.
        let e2 = w.key("e2");
        w.event(LegEvent::Failed {
            leg: e2,
            cause: LegFailure::Dead,
        });
        let (to, nonce, request) = loop {
            w.run_to(w.ms() + 1);
            if let Some(request) = w.requests().pop() {
                break request;
            }
        };
        assert!(matches!(&request, Control::Assign { skip, .. } if skip == &["e2"]));
        // The replacement e3 sends no token: skipped 3 s, passed over.
        w.answer(to, nonce, vec![cache(3)], 1, false, None);
        let e3 = w.key("e3");
        w.event(LegEvent::NoToken(e3));
        let passed = w.ms();
        assert!(w
            .d
            .legs()
            .iter()
            .any(|leg| leg.node == "e3" && leg.passed_over.is_some()));
        // Another edge is asked, beside the delivering leg within 250 ms.
        let (to, nonce, request) = loop {
            w.run_to(w.ms() + 1);
            if let Some(request) = w.requests().pop() {
                break request;
            }
            assert!(w.ms() <= passed + 250);
        };
        let Control::Assign { skip, hold, .. } = request else {
            unreachable!()
        };
        assert_eq!(
            (skip, hold),
            (
                vec!["e2".to_owned(), "e3".to_owned()],
                vec!["e1".to_owned()]
            )
        );
        // Every cache named is skipped: the one skipped longest ago (e2).
        w.answer(to, nonce, vec![cache(3), cache(2)], 1, false, None);
        assert!(w.held().contains(&"e2".to_owned()), "{:?}", w.held());
        assert!(!w.d.skipped(w.now).contains(&"e2".to_owned()));
        // The passed-over leg is still asked; the new leg answers first.
        let new_e2 = w.key("e2");
        w.event(LegEvent::Tokened(new_e2));
        w.event(LegEvent::Tokened(e3));
        assert!(w.s.legs[&e3].released, "used only if it answers first");
        // No-token skip: 3 s.
        w.run_to(passed + 2_999);
        assert!(w.d.skipped(w.now).contains(&"e3".to_owned()));
        w.run_to(passed + 3_000);
        assert!(!w.d.skipped(w.now).contains(&"e3".to_owned()));
        // A moved leg: skipped until 3 s after its release.
        let e1 = w.key("e1");
        w.event(LegEvent::Moved {
            leg: e1,
            reason: "draining".into(),
        });
        let (to, nonce, _) = w.requests().pop().unwrap();
        w.answer(to, nonce, vec![cache(4)], 1, false, None);
        w.live("e4");
        w.view("e4", |view| view.steady = false);
        w.run_to(w.ms() + 10_000);
        assert!(
            w.d.skipped(w.now).contains(&"e1".to_owned()),
            "held: no end yet"
        );
        let e4 = w.key("e4");
        w.s.level.insert((e4, e1));
        w.run_to(w.ms() + 5);
        let released = w.ms();
        assert!(w.s.legs[&e1].released);
        w.run_to(released + 2_990);
        assert!(w.d.skipped(w.now).contains(&"e1".to_owned()));
        w.run_to(released + 3_005);
        assert!(!w.d.skipped(w.now).contains(&"e1".to_owned()));
        assert!(w.ms() > start);
    }

    /// No moment with no leg that delivers and is not leaving.
    fn assert_served(w: &World) {
        assert!(
            w.d.legs()
                .iter()
                .any(|leg| leg.leaving.is_none() && leg.live && w.s.views[&leg.key].delivering),
            "no leg at {} ms: {:?}",
            w.ms(),
            w.s.log
        );
    }

    #[test]
    fn a_roll_of_two_edges_leaves_no_listener_without_an_edge() {
        let mut w = World::new("roll-1", 2, true);
        w.join(&[1, 2], Some(3));
        // e1 drained: the entry names e3, the spare, which is promoted.
        let e1 = w.key("e1");
        w.event(LegEvent::Moved {
            leg: e1,
            reason: "draining".into(),
        });
        let (to, nonce, _) = w.requests().pop().unwrap();
        w.answer(to, nonce, vec![cache(3)], 1, false, None);
        assert_eq!(
            w.fake("e3").start.token,
            Some(0x5003),
            "promoted with its token"
        );
        assert_eq!(w.d.spare(), None);
        assert_served(&w);
        w.live("e3");
        let e3 = w.key("e3");
        w.s.level.insert((e3, e1));
        w.run_to(w.ms() + 5);
        assert!(w.s.legs[&e1].released);
        assert_served(&w);
        // e1 is down: the spare's answer names nothing until it is back.
        let mut spare_answers = 0;
        let mut e1_back = false;
        let roll_start = w.ms();
        while w.d.spare().is_none() {
            w.run_to(w.ms() + 5);
            assert_served(&w);
            if w.ms() > roll_start + 3_000 {
                e1_back = true;
            }
            if let Some((to, nonce, request)) = w.requests().pop() {
                let Control::Assign { skip, hold, .. } = &request else {
                    unreachable!()
                };
                assert!(!hold.contains(&"e1".to_owned()));
                spare_answers += 1;
                let named = if e1_back && !skip.contains(&"e1".to_owned()) {
                    vec![cache(1)]
                } else {
                    Vec::new()
                };
                w.answer(to, nonce, named, 0, true, None);
            }
            assert!(w.ms() < roll_start + 20_000);
        }
        assert!(spare_answers >= 2);
        assert_eq!(w.d.spare(), Some("e1"));
        assert!(w.token_from(edge_addr(1), 0x6001));
        // Then e2 drained: e1, the spare, takes its place.
        let e2 = w.key("e2");
        w.event(LegEvent::Moved {
            leg: e2,
            reason: "draining".into(),
        });
        let (to, nonce, _) = w.requests().pop().unwrap();
        w.answer(to, nonce, vec![cache(1)], 1, false, None);
        w.live("e1");
        let new_e1 = w.key("e1");
        assert_served(&w);
        w.s.level.insert((new_e1, e2));
        w.run_to(w.ms() + 5);
        assert!(w.s.legs[&e2].released);
        assert_served(&w);
        let mut held = w.held();
        held.sort();
        assert_eq!(held, ["e1", "e3"]);
    }

    #[test]
    fn one_serving_edge_gives_one_leg_and_asks_again() {
        let mut w = World::new("one-edge", 2, true);
        w.poll();
        let (to, nonce, _) = w.requests().pop().unwrap();
        w.answer_why(
            to,
            nonce,
            vec![cache(1)],
            1,
            false,
            Some(0x1),
            Some(ShortGrant::OneEdge),
        );
        assert_eq!(w.held(), ["e1"]);
        assert_eq!(w.d.stats.short_grants.one_edge, 1);
        w.live("e1");
        let spread = spread("one-edge", 0, ASK_SPREAD).as_millis() as u64;
        let mut answered = w.ms();
        let mut waits = Vec::new();
        while waits.len() < 5 {
            w.run_to(w.ms() + 1);
            if let Some((to, nonce, request)) = w.requests().pop() {
                let Control::Assign {
                    paths, spare, hold, ..
                } = request
                else {
                    unreachable!()
                };
                assert_eq!((paths, spare, hold), (1, true, vec!["e1".to_owned()]));
                waits.push(w.ms() - answered);
                w.answer(to, nonce, Vec::new(), 0, false, None);
                answered = w.ms();
            }
        }
        let expected: Vec<u64> = [1_000, 2_000, 4_000, 5_000, 5_000]
            .iter()
            .map(|wait| wait + spread)
            .collect();
        for (wait, expected) in waits.iter().zip(&expected) {
            assert!(
                wait.abs_diff(*expected) <= 1,
                "{waits:?} against {expected:?}"
            );
        }
        assert_eq!(w.held(), ["e1"]);
    }

    #[test]
    fn region_lost_moves_make_before_break() {
        let failover = AssignedCache {
            node_id: "new-dist-a".into(),
            playback_base_url: "https://10.0.9.1:19444".into(),
            failover: true,
        };
        let away = |n: u8| AssignedCache {
            node_id: format!("n{n}"),
            playback_base_url: format!("https://10.0.2.{n}:19444"),
            failover: false,
        };
        let mut w = World::new("region-1", 2, true);
        w.poll();
        let (to, nonce, _) = w.requests().pop().unwrap();
        w.answer(
            to,
            nonce,
            vec![cache(1), cache(2), cache(3), failover],
            2,
            true,
            None,
        );
        w.live("e1");
        w.live("e2");
        assert!(w.token_from(edge_addr(3), 0x33));
        let e1 = w.key("e1");
        let e2 = w.key("e2");
        w.event(LegEvent::Moved {
            leg: e1,
            reason: REGION_LOST.into(),
        });
        // Both home legs leave; the failover entry is asked for two legs
        // and a spare.
        assert!(w.leaving("e1") && w.leaving("e2"));
        let failover_addr = entry_addr("https://10.0.9.1:19444");
        let (to, nonce, request) = w.requests().pop().unwrap();
        assert_eq!(to, failover_addr);
        assert!(matches!(
            request,
            Control::Assign {
                paths: 2,
                spare: true,
                ..
            }
        ));
        // The other home edge says so too, at its own tick: nothing more.
        w.run_to(w.ms() + 40);
        w.event(LegEvent::Moved {
            leg: e2,
            reason: REGION_LOST.into(),
        });
        assert_eq!(w.d.stats.failovers, 1);
        assert!(
            w.requests().iter().all(|(_, repeat, _)| *repeat == nonce),
            "the request out is kept"
        );
        w.answer(to, nonce, vec![away(1), away(2), away(3)], 2, true, None);
        assert!(
            !w.s.legs[&e1].released && !w.s.legs[&e2].released,
            "make before break"
        );
        w.live("n1");
        w.live("n2");
        let n1 = w.key("n1");
        let n2 = w.key("n2");
        // Home edges repeat their MOVE every 250 ms: the new region's legs
        // are not sent away, and the listener is not sent back home.
        w.run_to(w.ms() + 250);
        for leg in [e1, e2] {
            w.event(LegEvent::Moved {
                leg,
                reason: REGION_LOST.into(),
            });
        }
        assert!(!w.leaving("n1") && !w.leaving("n2"));
        assert_eq!(w.d.stats.failovers, 1);
        assert!(w.requests().iter().all(|(to, _, _)| *to == failover_addr));
        w.s.level.insert((n1, e1));
        w.s.level.insert((n2, e2));
        w.run_to(w.ms() + 5);
        assert_eq!(w.held(), ["n1", "n2"]);
        assert_eq!(w.d.stats.failovers, 1);
        let away_spare = SocketAddr::new(IpAddr::from([10, 0, 2, 3]), UDP);
        assert!(w.token_from(away_spare, 0x23));
        // Home again after 30 s, make before break as well.
        let left = w.d.away_since().unwrap();
        let left = (left - w.t0).as_millis() as u64;
        w.run_with_spare(left + 29_990, away_spare);
        assert!(w.requests().is_empty());
        w.run_to(left + 30_000);
        let (to, nonce, _) = w.requests().pop().expect("the home region is probed");
        assert_eq!(to, w.first_entry());
        w.answer(to, nonce, vec![cache(4), cache(5), cache(6)], 2, true, None);
        assert!(w.leaving("n1") && w.leaving("n2"));
        w.live("e4");
        w.live("e5");
        let (e4, e5) = (w.key("e4"), w.key("e5"));
        w.s.level.insert((e4, n1));
        w.s.level.insert((e5, n2));
        w.run_to(w.ms() + 5);
        assert_eq!(w.held(), ["e4", "e5"]);
        assert_eq!(w.d.stats.home_returns, 1);
        assert_eq!(w.d.away_since(), None);
        assert_eq!(w.d.spare(), Some("e6"));
    }

    #[test]
    fn a_stream_over_the_bit_rate_keeps_one_leg() {
        let mut w = World::new("4k-1", 2, true);
        w.poll();
        let (to, nonce, _) = w.requests().pop().unwrap();
        // One leg granted for the bit rate: the next cache is the spare.
        w.answer_why(
            to,
            nonce,
            vec![cache(1), cache(2), cache(3)],
            1,
            true,
            None,
            Some(ShortGrant::Bitrate),
        );
        assert_eq!(w.held(), ["e1"]);
        assert_eq!(w.d.spare(), Some("e2"));
        assert_eq!(w.d.want(), 1, "the entry's grant, for the bit rate");
        assert_eq!(w.d.stats.short_grants.bitrate, 1);
        w.live("e1");
        assert!(w.token_from(edge_addr(2), 0x22));
        // No second leg is asked for, though the objects have not shown yet
        // (a 4K stream's 16 parts take seconds).
        for _ in 0..1_000 {
            w.run_to(w.ms() + 10);
            for (to, control, _) in w.controls() {
                assert!(
                    !matches!(control, Control::Assign { token: None, .. }),
                    "asked at {} ms: {control:?}",
                    w.ms()
                );
                if to == edge_addr(2) && matches!(control, Control::Subscribe { token: None, .. }) {
                    assert!(w.token_from(edge_addr(2), 0x22));
                }
            }
        }
        assert_eq!(w.held(), ["e1"]);
        w.event(LegEvent::ObjectLane);
        assert_eq!(w.d.want(), 1);
        for _ in 0..30 {
            w.run_to(w.ms() + 1_000);
            assert!(w.requests().is_empty());
            assert!(w.token_from(edge_addr(2), 0x22));
        }
        // A leaving leg is released at its replacement's first object.
        let e1 = w.key("e1");
        w.event(LegEvent::Moved {
            leg: e1,
            reason: "draining".into(),
        });
        let (to, nonce, _) = w.requests().pop().unwrap();
        w.answer(to, nonce, vec![cache(4)], 1, false, None);
        w.live("e4");
        w.view("e4", |view| view.steady = false);
        w.run_to(w.ms() + 5);
        assert!(w.s.legs[&e1].released);
        // With two legs when the objects start: the one not leading goes.
        let mut w = World::new("4k-2", 2, true);
        w.join(&[1, 2], Some(3));
        w.view("e1", |view| view.lag = Some(Duration::from_micros(800)));
        w.event(LegEvent::ObjectLane);
        assert_eq!(w.held(), ["e2"]);
    }

    #[test]
    fn a_request_carries_no_current_and_the_confirmation_names_every_leg() {
        let mut w = World::new("current-1", 2, true);
        w.join(&[1, 2], Some(3));
        let e2 = w.key("e2");
        w.event(LegEvent::Moved {
            leg: e2,
            reason: "draining".into(),
        });
        let (to, nonce, request) = w.requests().pop().unwrap();
        let Control::Assign {
            current,
            skip,
            hold,
            ..
        } = &request
        else {
            unreachable!()
        };
        assert!(current.is_empty(), "a request names no current");
        assert_eq!(
            (skip.as_slice(), hold.as_slice()),
            (&["e2".to_owned()][..], &["e1".to_owned()][..])
        );
        w.answer(to, nonce, vec![cache(4)], 1, false, Some(0x44));
        let confirmations: Vec<(SocketAddr, Vec<String>)> = w
            .controls()
            .into_iter()
            .filter_map(|(to, control, _)| match control {
                Control::Assign {
                    token: Some(0x44),
                    current,
                    nonce: confirmed,
                    ..
                } if confirmed == nonce => Some((to, current)),
                _ => None,
            })
            .collect();
        assert_eq!(
            confirmations,
            [(to, vec!["e1".to_owned(), "e4".to_owned()])]
        );
        // Sent again 30 ms later.
        w.run_to(w.ms() + 30);
        let again = w
            .controls()
            .into_iter()
            .filter(|(_, control, _)| {
                matches!(
                    control,
                    Control::Assign {
                        token: Some(0x44),
                        ..
                    }
                )
            })
            .count();
        assert_eq!(again, 1);
        // The HTTPS form says the same.
        let request = Request {
            nonce: 1,
            kind: RequestKind::Normal,
            started: w.now,
            deadline: w.now,
            datagram: Vec::new(),
            paths: 1,
            spare: false,
            skip: vec!["e2".into()],
            hold: vec!["e1".into()],
            pool: vec![0],
            asked: 0,
            repeats: 0,
            next_repeat: w.now,
            next_entry_at: None,
            https_at: None,
            tokens: Vec::new(),
            answered_by: Vec::new(),
            spare_held: None,
            place_of: None,
        };
        assert_eq!(
            w.d.https_path(&request),
            "/api/assignment?stream=3001&sid=current-1&skip=e2&hold=e1"
        );
    }

    #[test]
    fn a_request_beside_a_live_leg_is_spread_over_250_ms() {
        let mut delays = Vec::new();
        for index in 0..200 {
            let sid = format!("spread-{index}");
            let mut w = World::new(&sid, 2, false);
            w.join(&[1, 2], None);
            let e2 = w.key("e2");
            w.event(LegEvent::Failed {
                leg: e2,
                cause: LegFailure::Dead,
            });
            let failed = w.ms();
            let (delay, to, nonce) = loop {
                if let Some((to, nonce, _)) = w.requests().pop() {
                    break (w.ms() - failed, to, nonce);
                }
                assert!(w.ms() <= failed + 250, "{sid}: not asked in 250 ms");
                w.run_to(w.ms() + 1);
            };
            delays.push(delay);
            // No edge for it; then a MOVE on the other leg is asked for at
            // once, whatever the backoff.
            w.answer(to, nonce, Vec::new(), 0, false, None);
            let e1 = w.key("e1");
            w.event(LegEvent::Moved {
                leg: e1,
                reason: "draining".into(),
            });
            assert_eq!(w.requests().len(), 1, "{sid}: a MOVE is asked for at once");
        }
        let low = delays.iter().filter(|delay| **delay < 50).count();
        let high = delays.iter().filter(|delay| **delay > 200).count();
        assert!(low >= 20 && high >= 20, "{delays:?}");
    }

    #[test]
    fn every_leg_silent_fails_only_the_legs_that_do_not_answer() {
        let mut w = World::new("silent-1", 2, true);
        w.join(&[1, 2], Some(3));
        w.s.hole = Some(1_010);
        for node in ["e1", "e2"] {
            w.view(node, |view| view.delivering = false);
        }
        w.run_to(w.ms() + 200);
        let silent = w.ms();
        w.event(LegEvent::AllSilent);
        assert!(
            w.s.log.contains(&"reask e1".to_owned()) && w.s.log.contains(&"reask e2".to_owned())
        );
        assert!(w.d.skipped(w.now).is_empty(), "nobody is skipped for it");
        w.run_to(silent + 20);
        assert_eq!(w.fake("e3").start.from, StartFrom::OldestHole);
        w.live("e3");
        // e1 answers the question, e2 does not.
        let now = w.now;
        w.view("e1", |view| {
            view.last_datagram = Some(now);
            view.last_media = Some(now);
            view.delivering = true;
        });
        let e2 = w.key("e2");
        w.run_to(silent + 400);
        assert!(w.s.legs[&e2].released);
        assert_eq!(w.d.skipped(w.now), ["e2"]);
        assert_eq!(w.held(), ["e1", "e3"]);
        assert!(
            w.d.legs().iter().all(|leg| leg.hedge_for.is_none()),
            "the hedge has its place"
        );

        // Without a spare the entry is asked at once, the silent legs in
        // its skip on the wire only.
        let mut w = World::new("silent-2", 2, false);
        w.join(&[1, 2], None);
        // The legs were steady well before the silence.
        w.run_to(w.ms() + 1_100);
        let silent = w.ms();
        w.event(LegEvent::AllSilent);
        let (to, nonce, request) = w.requests().pop().expect("asked at once");
        let Control::Assign {
            paths, skip, hold, ..
        } = request
        else {
            unreachable!()
        };
        assert_eq!(paths, 2);
        assert_eq!(skip, ["e1", "e2"]);
        assert!(hold.is_empty());
        assert!(w.d.skipped(w.now).is_empty());
        w.answer(to, nonce, vec![cache(4), cache(5)], 2, false, None);
        assert_eq!(w.held(), ["e1", "e2", "e4", "e5"]);
        w.live("e4");
        w.live("e5");
        // The old legs still count as steady by their last parts, but have
        // brought nothing since the silence: the new legs stay.
        w.run_to(silent + 50);
        assert_eq!(w.held(), ["e1", "e2", "e4", "e5"]);
        let now = w.now;
        w.view("e1", |view| {
            view.last_datagram = Some(now);
            view.last_media = Some(now);
        });
        w.run_to(silent + 400);
        assert_eq!(w.held(), ["e1", "e4", "e5"]);
        // e2's place went to a leg asked for in the silence: no leg
        // delivered meanwhile, an interruption (B 2.8).
        let replaced: Vec<_> =
            w.d.replacements
                .iter()
                .map(|r| (r.from.as_str(), r.to.as_str(), r.kind))
                .collect();
        assert_eq!(replaced, [("e2", "e4", ReplacementKind::Interrupt)]);
        let mut lone = World::new("silent-3", 1, false);
        lone.join(&[1], None);
        lone.run_to(lone.ms() + 1_100);
        lone.event(LegEvent::AllSilent);
        let (to, nonce, _) = lone.requests().pop().expect("asked at once");
        lone.answer(to, nonce, vec![cache(4)], 1, false, None);
        lone.live("e4");
        // The silent leg found dead beside the leg asked for in the silence
        // (before the question's wait ends): the same record.
        let e1 = lone.key("e1");
        lone.event(LegEvent::Failed {
            leg: e1,
            cause: LegFailure::Dead,
        });
        assert_eq!(lone.held(), ["e4"]);
        let replaced: Vec<_> = lone
            .d
            .replacements
            .iter()
            .map(|r| (r.from.as_str(), r.to.as_str(), r.kind, r.exposure.is_some()))
            .collect();
        assert_eq!(replaced, [("e1", "e4", ReplacementKind::Interrupt, true)]);
        // One leg more than wanted: the newest goes once the others are
        // steady for the level wait, counted from e1's media after the
        // silence.
        w.run_to(silent + 1_040);
        assert_eq!(w.held(), ["e1", "e4", "e5"]);
        w.run_to(silent + 1_100);
        assert_eq!(w.held(), ["e1", "e4"]);
    }

    /// The records of legs given up in a silence of every leg: one to a
    /// leg, and `interrupt` only when the leg that took the place ended the
    /// silence (B 2.8).
    #[test]
    fn a_silence_s_records_go_one_to_a_leg_and_name_who_ended_it() {
        let replaced = |w: &World| -> Vec<(String, String, ReplacementKind)> {
            w.d.replacements
                .iter()
                .map(|r| (r.from.clone(), r.to.clone(), r.kind))
                .collect()
        };
        let record = |from: &str, to: &str, kind| (from.to_owned(), to.to_owned(), kind);
        // Neither leg answers the question: the hedge takes the place of the
        // leg it stood beside and no other; the second record waits.
        let mut w = World::new("silent-4", 2, true);
        w.join(&[1, 2], Some(3));
        for node in ["e1", "e2"] {
            w.view(node, |view| view.delivering = false);
        }
        w.run_to(w.ms() + 200);
        let silent = w.ms();
        w.event(LegEvent::AllSilent);
        w.run_to(silent + 20);
        w.live("e3");
        w.run_to(silent + 400);
        assert_eq!(w.held(), ["e3"]);
        assert_eq!(replaced(&w), [record("e1", "e3", ReplacementKind::Hedge)]);
        // No spare: the entry gives e4 and e5, e1 comes back before either
        // brings media, and e2 is found dead beside it while the silence is
        // open. A leg delivered: e4 replaced e2, it ended nothing.
        let mut w = World::new("silent-5", 2, false);
        w.join(&[1, 2], None);
        w.run_to(w.ms() + 1_100);
        let silent = w.ms();
        w.event(LegEvent::AllSilent);
        let (to, nonce, _) = w.requests().pop().expect("asked at once");
        w.answer(to, nonce, vec![cache(4), cache(5)], 2, false, None);
        w.run_to(silent + 20);
        let now = w.now;
        w.view("e1", |view| {
            view.last_datagram = Some(now);
            view.last_media = Some(now);
            view.delivering = true;
        });
        w.run_to(silent + 30);
        w.live("e4");
        w.poll();
        let e2 = w.key("e2");
        w.event(LegEvent::Failed {
            leg: e2,
            cause: LegFailure::Dead,
        });
        assert_eq!(replaced(&w), [record("e2", "e4", ReplacementKind::Replace)]);
    }

    #[test]
    fn a_one_leg_join_takes_the_edge_whose_token_comes_first() {
        let mut w = World::new("swap-1", 1, true);
        w.poll();
        let (to, nonce, _) = w.requests().pop().unwrap();
        w.answer(to, nonce, vec![cache(1), cache(2)], 1, true, None);
        assert_eq!(w.held(), ["e1"]);
        assert_eq!(w.d.spare(), Some("e2"));
        w.take();
        // The spare's token first; the leg's does not follow in 10 ms.
        assert!(w.token_from(edge_addr(2), 0x22));
        let tokened = w.ms();
        w.run_to(tokened + 9);
        assert_eq!(w.held(), ["e1"]);
        w.run_to(tokened + 11);
        assert_eq!(w.held(), ["e2"]);
        assert_eq!(w.fake("e2").start.token, Some(0x22));
        assert_eq!(w.d.spare(), Some("e1"), "the two swap roles");
        assert!(w
            .controls()
            .iter()
            .any(|(to, control, _)| *to == edge_addr(1)
                && matches!(control, Control::Subscribe { token: None, .. })));
        assert!(w.token_from(edge_addr(1), 0x11));

        // With --paths 2, a stream the entry gives one leg for its bit rate
        // swaps the same.
        let mut w = World::new("swap-3", 2, true);
        w.poll();
        let (to, nonce, _) = w.requests().pop().unwrap();
        w.answer_why(
            to,
            nonce,
            vec![cache(1), cache(2), cache(3)],
            1,
            true,
            None,
            Some(ShortGrant::Bitrate),
        );
        assert_eq!((w.held(), w.d.spare()), (vec!["e1".to_owned()], Some("e2")));
        assert!(w.token_from(edge_addr(2), 0x22));
        w.run_to(w.ms() + 11);
        assert_eq!(w.held(), ["e2"]);
        assert_eq!(w.d.spare(), Some("e1"));

        // The leg's token within 10 ms: it stays the leg.
        let mut w = World::new("swap-2", 1, true);
        w.poll();
        let (to, nonce, _) = w.requests().pop().unwrap();
        w.answer(to, nonce, vec![cache(1), cache(2)], 1, true, None);
        assert!(w.token_from(edge_addr(2), 0x22));
        w.run_to(w.ms() + 5);
        let e1 = w.key("e1");
        w.event(LegEvent::Tokened(e1));
        w.run_to(w.ms() + 50);
        assert_eq!(w.held(), ["e1"]);
        assert_eq!(w.d.spare(), Some("e2"));
    }

    #[test]
    fn an_https_answer_names_the_legs_and_the_spare_by_name() {
        let mut w = World::new("https-1", 2, true);
        w.d.config.assignment_udp_port = 0;
        w.poll();
        let actions = w.take();
        assert!(!actions
            .iter()
            .any(|action| matches!(action, Action::Send { .. })));
        let Some(Action::Https { id, path, .. }) = actions.first().cloned() else {
            panic!("{actions:?}")
        };
        assert_eq!(
            path,
            "/api/assignment?stream=3001&sid=https-1&paths=2&spare=1"
        );
        let body = br#"{"client_udp_port":19446,"paths":2,"spare":"e3","assigned":[
            {"node_id":"e1","playback_base_url":"https://edge-1.lab:19444"},
            {"node_id":"e3","playback_base_url":"https://edge-3.lab:19444","reason":"spare"},
            {"node_id":"e2","playback_base_url":"https://10.0.1.2:19444","reason":"second_path"}]}"#;
        w.d.on_https(&mut w.s, id, Some(body), w.now, &mut w.out);
        let mut hosts: Vec<String> = w
            .take()
            .into_iter()
            .filter_map(|action| match action {
                Action::Resolve { host } => Some(host),
                _ => None,
            })
            .collect();
        hosts.sort();
        assert_eq!(hosts, ["edge-1.lab", "edge-3.lab"]);
        assert!(w.held().is_empty());
        w.d.on_resolved(
            &mut w.s,
            "edge-1.lab",
            Some(IpAddr::from([10, 0, 1, 1])),
            w.now,
            &mut w.out,
        );
        assert!(w.held().is_empty(), "waits for every host");
        w.d.on_resolved(
            &mut w.s,
            "edge-3.lab",
            Some(IpAddr::from([10, 0, 1, 3])),
            w.now,
            &mut w.out,
        );
        assert_eq!(w.held(), ["e1", "e2"]);
        assert_eq!(w.fake("e1").addr, edge_addr(1));
        assert_eq!(w.d.spare(), Some("e3"));
        assert_eq!(w.d.first_answer.map(|(_, via)| via), Some(Via::Https));
        assert_eq!(w.d.stats.answers_https, 1);
    }

    #[test]
    fn the_spare_named_as_the_new_leg_keeps_the_old_one_until_level_whatever_the_spare_answer() {
        for reason in ["draining", "rebalance"] {
            let mut w = World::new(&format!("promoted-{reason}"), 2, true);
            w.join(&[1, 2], Some(3));
            let e1 = w.key("e1");
            w.event(LegEvent::Moved {
                leg: e1,
                reason: reason.into(),
            });
            let (to, nonce, request) = w.requests().pop().unwrap();
            assert!(matches!(
                request,
                Control::Assign {
                    paths: 1,
                    spare: true,
                    spare_held: Some(ref held),
                    ..
                } if held == "e3"
            ));
            // The entry names the spare: it is promoted, and a new spare is
            // asked for at once (a leg is leaving).
            w.answer(to, nonce, vec![cache(3)], 1, false, None);
            assert_eq!(w.d.spare(), None);
            let (to, nonce, request) = w.requests().pop().expect("a new spare is asked for");
            let Control::Assign {
                paths, spare, hold, ..
            } = request
            else {
                unreachable!()
            };
            assert_eq!(
                (paths, spare, hold),
                (0, true, vec!["e2".to_owned(), "e3".to_owned()])
            );
            // A region of three edges: no other cache to name.
            w.answer(to, nonce, vec![failover_entry()], 0, true, None);
            assert!(
                w.leaving("e1") && !w.s.legs[&e1].released,
                "{reason}: {:?}",
                w.s.log
            );
            assert!(
                !w.s.log.iter().any(|line| line.starts_with("takeback")),
                "{reason}: {:?}",
                w.s.log
            );
            assert_eq!(w.d.stats.failovers, 0);
            w.live("e3");
            let e3 = w.key("e3");
            w.view("e3", |view| view.steady = false);
            w.run_to(w.ms() + 500);
            assert!(
                !w.s.legs[&e1].released && !w.s.legs[&e3].released,
                "{reason}: {:?}",
                w.s.log
            );
            w.view("e3", |view| view.steady = true);
            w.s.level.insert((e3, e1));
            w.run_to(w.ms() + 5);
            assert!(w.s.legs[&e1].released, "{reason}");
            assert_eq!(w.held(), ["e2", "e3"]);
        }
    }

    #[test]
    fn a_one_leg_rebalance_or_lost_parents_with_no_cache_stays_in_the_region() {
        let failover_addr = entry_addr("https://10.0.9.1:19444");
        for reason in ["rebalance", "parents-lost"] {
            let mut w = World::new(&format!("one-leg-{reason}"), 1, true);
            w.poll();
            let (to, nonce, _) = w.requests().pop().unwrap();
            w.answer(
                to,
                nonce,
                vec![cache(1), cache(2), failover_entry()],
                1,
                true,
                None,
            );
            w.live("e1");
            assert!(w.token_from(edge_addr(2), 0x22));
            let e1 = w.key("e1");
            w.event(LegEvent::Moved {
                leg: e1,
                reason: reason.into(),
            });
            let (to, nonce, _) = w.requests().pop().unwrap();
            assert_ne!(to, failover_addr);
            // No receiver under the level: only the failover entry is named.
            w.answer(to, nonce, vec![failover_entry()], 1, false, None);
            assert_eq!(w.s.log.last().unwrap(), "takeback e1", "{reason}");
            assert!(!w.leaving("e1"));
            assert_eq!(w.d.stats.failovers, 0, "{reason}");
            assert_eq!(w.d.away_since(), None);
            assert!(w.requests().iter().all(|(to, _, _)| *to != failover_addr));
        }
        // A drain with nothing left in the region goes to the failover
        // region (step 9), make before break.
        let mut w = World::new("one-leg-drain", 1, false);
        w.poll();
        let (to, nonce, _) = w.requests().pop().unwrap();
        w.answer(to, nonce, vec![cache(1), failover_entry()], 1, false, None);
        w.live("e1");
        let e1 = w.key("e1");
        w.event(LegEvent::Moved {
            leg: e1,
            reason: "draining".into(),
        });
        let (to, nonce, _) = w.requests().pop().unwrap();
        w.answer(to, nonce, vec![failover_entry()], 1, false, None);
        assert_eq!(w.d.stats.failovers, 1);
        assert!(!w.s.legs[&e1].released);
        assert_eq!(w.requests().pop().unwrap().0, failover_addr);
    }

    #[test]
    fn two_legs_moved_together_are_each_released_by_their_own_replacement() {
        let mut w = World::new("two-drains", 2, false);
        w.join(&[1, 2], None);
        let (e1, e2) = (w.key("e1"), w.key("e2"));
        w.event(LegEvent::Moved {
            leg: e1,
            reason: "draining".into(),
        });
        let (to, nonce, _) = w.requests().pop().unwrap();
        w.event(LegEvent::Moved {
            leg: e2,
            reason: "draining".into(),
        });
        // One edge named: it is e1's replacement, and e2's is asked for.
        w.answer(to, nonce, vec![cache(3)], 1, false, None);
        let e3 = w.key("e3");
        assert_eq!(
            w.d.legs()
                .iter()
                .find(|leg| leg.key == e3)
                .unwrap()
                .stands_for,
            Some(e1)
        );
        let (to, nonce, request) = w.requests().pop().expect("e2's replacement");
        assert!(matches!(request, Control::Assign { paths: 1, .. }));
        w.live("e3");
        w.s.level.insert((e3, e1));
        w.s.level.insert((e3, e2));
        w.run_to(w.ms() + 5);
        assert!(w.s.legs[&e1].released);
        assert!(
            !w.s.legs[&e2].released,
            "e2 stays until a leg of its own is level"
        );
        w.run_to(w.ms() + 500);
        assert!(!w.s.legs[&e2].released);
        w.answer(to, nonce, vec![cache(4)], 1, false, None);
        w.live("e4");
        let e4 = w.key("e4");
        w.s.level.insert((e4, e2));
        w.run_to(w.ms() + 5);
        assert!(w.s.legs[&e2].released);
        assert_eq!(w.held(), ["e3", "e4"]);
        let handovers: Vec<(&str, &str)> =
            w.d.replacements
                .iter()
                .filter(|record| record.kind == ReplacementKind::Handover)
                .map(|record| (record.from.as_str(), record.to.as_str()))
                .collect();
        assert_eq!(handovers, [("e1", "e3"), ("e2", "e4")]);
    }

    #[test]
    fn a_rebalance_give_up_releases_only_the_leg_added_for_it() {
        let mut w = World::new("giveup-2", 2, false);
        w.join(&[1, 2], None);
        let (e1, e2) = (w.key("e1"), w.key("e2"));
        let moved = w.ms();
        w.event(LegEvent::Moved {
            leg: e1,
            reason: "rebalance".into(),
        });
        let (to, nonce, _) = w.requests().pop().unwrap();
        w.answer(to, nonce, vec![cache(3)], 1, false, None);
        // Steady, never level.
        w.live("e3");
        let e3 = w.key("e3");
        // A second later e2 dies; e4 takes its place.
        w.run_to(moved + 1_000);
        w.event(LegEvent::Failed {
            leg: e2,
            cause: LegFailure::Dead,
        });
        let (to, nonce, _) = w.requests().pop().expect("asked at once: a leg is leaving");
        w.answer(to, nonce, vec![cache(4)], 1, false, None);
        w.live("e4");
        let e4 = w.key("e4");
        w.run_to(moved + 3_005);
        assert!(w.s.legs[&e3].released);
        assert!(w.s.log.contains(&"takeback e1".to_owned()));
        assert!(!w.s.legs[&e4].released, "e4 holds e2's place");
        assert_eq!(w.held(), ["e1", "e4"]);
        assert!(w.d.replacements.iter().any(|record| {
            record.kind == ReplacementKind::Replace && record.from == "e2" && record.to == "e4"
        }));
    }

    #[test]
    fn a_move_while_another_request_is_out_is_asked_for_once_it_is_answered() {
        for spare_answer in [vec![cache(5)], Vec::new()] {
            let mut w = World::new("stale-1", 2, true);
            // No spare named: one is asked for after the backoff.
            w.join(&[1, 2], None);
            let (to, nonce) = loop {
                w.run_to(w.ms() + 1);
                if let Some((to, nonce, request)) = w.requests().pop() {
                    assert!(matches!(
                        request,
                        Control::Assign {
                            paths: 0,
                            spare: true,
                            ..
                        }
                    ));
                    break (to, nonce);
                }
                assert!(w.ms() < 2_000);
            };
            let e1 = w.key("e1");
            w.event(LegEvent::Moved {
                leg: e1,
                reason: "draining".into(),
            });
            assert!(w.requests().is_empty(), "the request out is waited for");
            w.run_to(w.ms() + 2);
            w.answer(to, nonce, spare_answer.clone(), 0, true, None);
            let requests = w.requests();
            assert_eq!(
                requests.len(),
                1,
                "e1's replacement is asked for at once: {requests:?}"
            );
            assert!(
                matches!(&requests[0].2, Control::Assign { paths: 1, skip, .. } if skip == &["e1"]),
                "{requests:?}"
            );
        }
    }

    #[test]
    fn a_failed_legs_record_goes_only_to_a_leg_added_for_its_place() {
        let mut w = World::new("record-1", 2, false);
        w.join(&[1, 2], None);
        let (e1, e2) = (w.key("e1"), w.key("e2"));
        w.event(LegEvent::Failed {
            leg: e2,
            cause: LegFailure::Dead,
        });
        // No other edge in the region: nothing replaces e2.
        let (to, nonce) = loop {
            w.run_to(w.ms() + 1);
            if let Some((to, nonce, _)) = w.requests().pop() {
                break (to, nonce);
            }
        };
        w.answer(to, nonce, Vec::new(), 0, false, None);
        // Then e1 is drained, and a new edge comes for it.
        w.event(LegEvent::Moved {
            leg: e1,
            reason: "draining".into(),
        });
        let (to, nonce, _) = w.requests().pop().unwrap();
        w.answer(to, nonce, vec![cache(3)], 1, false, None);
        w.live("e3");
        let e3 = w.key("e3");
        w.s.level.insert((e3, e1));
        w.run_to(w.ms() + 5);
        assert!(w.s.legs[&e1].released);
        let records: Vec<(ReplacementKind, &str, &str)> =
            w.d.replacements
                .iter()
                .map(|record| (record.kind, record.from.as_str(), record.to.as_str()))
                .collect();
        assert_eq!(records, [(ReplacementKind::Handover, "e1", "e3")]);
    }

    #[test]
    fn a_hedge_whose_edge_sent_it_away_is_not_the_spare_again() {
        let mut w = World::new("hedge-moved", 2, true);
        w.join(&[1, 2], Some(3));
        let e1 = w.key("e1");
        w.view("e1", |view| {
            view.delivering = false;
            view.steady = false;
        });
        w.event(LegEvent::HedgeDue(e1));
        w.run_to(w.ms() + 20);
        w.live("e3");
        let e3 = w.key("e3");
        w.event(LegEvent::Moved {
            leg: e3,
            reason: "draining".into(),
        });
        assert!(w.requests().is_empty(), "a hedge holds no place");
        w.view("e1", |view| {
            view.delivering = true;
            view.steady = true;
        });
        let back = w.ms();
        w.run_to(back + 1_010);
        assert!(w.s.legs[&e3].released);
        assert_eq!(w.d.spare(), None);
        assert_eq!(w.d.stats.hedges_released, 1);
        // A new spare is asked for, the draining edge skipped.
        let request = loop {
            w.run_to(w.ms() + 1);
            if let Some((_, _, request)) = w.requests().pop() {
                break request;
            }
            assert!(w.ms() < back + 1_300);
        };
        assert!(
            matches!(&request, Control::Assign { paths: 0, spare: true, skip, .. } if skip.contains(&"e3".to_owned())),
            "{request:?}"
        );
    }

    #[test]
    fn entry_and_cache_urls_take_either_scheme() {
        assert_eq!(
            url_host("http://10.0.1.7:19444/live").as_deref(),
            Some("10.0.1.7")
        );
        assert_eq!(url_host("https://[::1]:19444").as_deref(), Some("::1"));
        assert_eq!(url_host("ftp://10.0.1.7"), None);
        let mut w = World::with("http-1", 1, false, vec!["http://10.0.0.1:19444/".into()]);
        w.poll();
        let (to, nonce, _) = w.requests().pop().unwrap();
        assert_eq!(to, entry_addr(E1));
        w.answer(
            to,
            nonce,
            vec![AssignedCache {
                node_id: "e7".into(),
                playback_base_url: "http://10.0.1.7:19444/live".into(),
                failover: false,
            }],
            1,
            false,
            None,
        );
        assert_eq!(w.held(), ["e7"]);
        assert_eq!(w.fake("e7").addr, edge_addr(7));
    }

    #[test]
    fn only_failures_with_no_leg_send_the_listener_to_the_failover_region() {
        let mut w = World::with("fail-1", 1, false, vec![E1.into()]);
        w.poll();
        let (to, nonce, _) = w.requests().pop().unwrap();
        w.answer(to, nonce, vec![cache(1), failover_entry()], 1, false, None);
        w.live("e1");
        let e1 = w.key("e1");
        w.event(LegEvent::Moved {
            leg: e1,
            reason: "draining".into(),
        });
        // Nobody answers while the leg still delivers.
        let moved = w.ms();
        w.run_to(moved + 1_000);
        assert_eq!(w.d.stats.assignment_failures, 1);
        // The edge's grace cuts the leg: no leg at all now.
        w.event(LegEvent::Failed {
            leg: e1,
            cause: LegFailure::Dead,
        });
        let cut = w.ms();
        w.run_to(cut + 1_000);
        assert_eq!(w.d.stats.assignment_failures, 2);
        assert_eq!(w.d.stats.failovers, 0, "one failure with no leg is not two");
        w.run_to(cut + 2_300);
        assert_eq!(w.d.stats.assignment_failures, 3);
        assert_eq!(w.d.stats.failovers, 1);
    }

    /// The lead's decision on a short grant: `why=gate` (or `one-edge`)
    /// keeps two legs wanted and asks again on the backoff, beside the
    /// alternates the entry named; each short grant is counted by reason.
    #[test]
    fn a_short_grant_for_the_gate_is_asked_again_and_counted() {
        let mut w = World::new("gate-1", 2, true);
        w.poll();
        let (to, nonce, _) = w.requests().pop().unwrap();
        w.answer_why(
            to,
            nonce,
            vec![cache(1), cache(2), cache(3)],
            1,
            true,
            Some(0xe0),
            Some(ShortGrant::Gate),
        );
        assert_eq!(w.held(), ["e1"]);
        assert_eq!(w.d.spare(), Some("e2"));
        assert_eq!(w.d.want(), 2, "the gate is no reason to stay on one leg");
        assert_eq!(w.d.stats.short_grants.gate, 1);
        w.live("e1");
        assert!(w.token_from(edge_addr(2), 0x22));
        let answered = w.ms();
        let spread = spread("gate-1", 0, ASK_SPREAD).as_millis() as u64;
        let (to, nonce, request) = loop {
            w.run_to(w.ms() + 1);
            if let Some(request) = w.requests().pop() {
                break request;
            }
            assert!(w.ms() <= answered + 1_000 + spread + 1, "asked again");
        };
        assert!(w.ms() + 1 >= answered + 1_000 + spread);
        let Control::Assign {
            paths, spare, hold, ..
        } = request
        else {
            unreachable!()
        };
        assert_eq!((paths, spare, hold), (1, true, vec!["e1".to_owned()]));
        // Still over the gate: counted again; then the second leg comes.
        w.answer_why(
            to,
            nonce,
            Vec::new(),
            0,
            false,
            None,
            Some(ShortGrant::Gate),
        );
        assert_eq!(w.d.stats.short_grants.gate, 2);
        let (to, nonce, _) = loop {
            w.run_to(w.ms() + 1);
            if let Some(request) = w.requests().pop() {
                break request;
            }
            assert!(w.ms() <= answered + 10_000, "asked a third time");
        };
        w.answer(to, nonce, vec![cache(3)], 1, false, None);
        assert_eq!(w.held(), ["e1", "e3"]);
        assert_eq!(w.d.stats.short_grants.unexplained, 0);
    }

    /// The lead's decision on a leg its edge sends away before its `OK`
    /// (`Refused`): the skip ends by cause, 3 s for a rebalance or a
    /// refusal for load, 30 s for a drain, lost parents or anything else.
    #[test]
    fn a_refused_leg_is_skipped_by_its_cause() {
        for (reason, skip_ms) in [
            ("rebalance", 3_000),
            ("edge-busy", 3_000),
            ("edge-full", 3_000),
            ("draining", 30_000),
            ("parents-lost", 30_000),
            ("stale-copy", 30_000),
        ] {
            let mut w = World::new(&format!("refused-{reason}"), 2, false);
            w.join(&[1, 2], None);
            let e2 = w.key("e2");
            w.event(LegEvent::Failed {
                leg: e2,
                cause: LegFailure::Dead,
            });
            let (to, nonce, _) = loop {
                w.run_to(w.ms() + 1);
                if let Some(request) = w.requests().pop() {
                    break request;
                }
            };
            w.answer(to, nonce, vec![cache(3)], 1, false, None);
            let e3 = w.key("e3");
            w.event(LegEvent::Refused {
                leg: e3,
                reason: reason.into(),
            });
            let refused = w.ms();
            assert!(w.s.legs[&e3].released, "{reason}");
            w.run_to(refused + skip_ms - 1);
            assert!(w.d.skipped(w.now).contains(&"e3".to_owned()), "{reason}");
            w.run_to(refused + skip_ms);
            assert!(!w.d.skipped(w.now).contains(&"e3".to_owned()), "{reason}");
        }
    }

    /// The lead's decision on a one-leg listener whose edge drains while
    /// its region names no other edge: it moves make-before-break to the
    /// failover region's entry, and comes home by the probe of 30 s.
    #[test]
    fn a_one_leg_drain_with_no_other_edge_goes_to_the_failover_region_and_comes_home() {
        let failover_addr = entry_addr("https://10.0.9.1:19444");
        let away = AssignedCache {
            node_id: "n1".into(),
            playback_base_url: "https://10.0.2.1:19444".into(),
            failover: false,
        };
        let mut w = World::new("one-leg-home", 1, false);
        w.poll();
        let (to, nonce, _) = w.requests().pop().unwrap();
        w.answer(to, nonce, vec![cache(1), failover_entry()], 1, false, None);
        w.live("e1");
        let e1 = w.key("e1");
        w.event(LegEvent::Moved {
            leg: e1,
            reason: "draining".into(),
        });
        let (to, nonce, _) = w.requests().pop().unwrap();
        w.answer(to, nonce, vec![failover_entry()], 1, false, None);
        assert_eq!(w.d.stats.failovers, 1);
        let (to, nonce, _) = w.requests().pop().unwrap();
        assert_eq!(to, failover_addr);
        w.answer(to, nonce, vec![away], 1, false, None);
        assert!(!w.s.legs[&e1].released, "make before break");
        w.live("n1");
        let n1 = w.key("n1");
        w.s.level.insert((n1, e1));
        w.run_to(w.ms() + 5);
        assert_eq!(w.held(), ["n1"]);
        assert_eq!(
            w.d.replacements
                .last()
                .map(|r| (r.kind, r.from.as_str(), r.to.as_str())),
            Some((ReplacementKind::Handover, "e1", "n1"))
        );
        // Home is probed 30 s after the move; it names e2 now.
        let left = (w.d.away_since().unwrap() - w.t0).as_millis() as u64;
        w.run_to(left + 29_999);
        assert!(w.requests().is_empty());
        w.run_to(left + 30_000);
        let (to, nonce, _) = w.requests().pop().expect("the home region is probed");
        assert_eq!(to, w.first_entry());
        w.answer(to, nonce, vec![cache(2)], 1, false, None);
        assert!(w.leaving("n1") && !w.s.legs[&n1].released);
        w.live("e2");
        let e2 = w.key("e2");
        w.s.level.insert((e2, n1));
        w.run_to(w.ms() + 5);
        assert_eq!(w.held(), ["e2"]);
        assert_eq!(w.d.stats.home_returns, 1);
        assert_eq!(w.d.away_since(), None);
    }

    /// The time on one path: from when the second-freshest leg's media went
    /// stale to the datagram that brings a second leg back; nothing in the
    /// join (before two legs first delivered together, within the
    /// allowance), nor while one leg is wanted.
    #[test]
    fn single_path_spans_run_from_the_stale_leg_to_the_second_leg_back() {
        let t0 = Instant::now();
        let ms = |n: u64| t0 + Duration::from_millis(n);
        let window = Duration::from_millis(20);
        let mut path = SinglePath::new(Duration::from_millis(400));
        // A join: one leg for 5 ms is not single-path time.
        path.observe(ms(0), window, [ms(0)], true);
        path.observe(ms(5), window, [ms(5), ms(5)], true);
        assert!(!path.is_open());
        // Both deliver up to 100 ms; then one leg stops.
        path.observe(ms(100), window, [ms(100), ms(100)], true);
        path.observe(ms(110), window, [ms(110), ms(100)], true);
        assert!(!path.is_open(), "10 ms is within the window");
        path.observe(ms(130), window, [ms(130), ms(100)], true);
        assert!(path.is_open());
        // The hedge leg's first part at 175 ms.
        path.observe(ms(175), window, [ms(175), ms(100), ms(175)], true);
        assert!(!path.is_open());
        assert_eq!(path.spans, [(ms(120), ms(175))]);
        assert_eq!(path.total, Duration::from_millis(55));
        // One leg wanted: no time on one path.
        path.observe(ms(300), window, [ms(300)], false);
        assert!(!path.is_open());
        // Open at the end of the run: closed there.
        path.observe(ms(400), window, [ms(400), ms(350)], true);
        path.close(ms(410));
        assert_eq!(path.spans.len(), 2);
        assert_eq!(path.total, Duration::from_millis(55 + 40));
    }

    /// Review of B Y2, finding 5: a two-path session that never gets its
    /// second leg is on one path from its first media plus the join's
    /// allowance to the end, though two legs never delivered together.
    #[test]
    fn a_session_that_never_has_two_legs_is_on_one_path() {
        let t0 = Instant::now();
        let ms = |n: u64| t0 + Duration::from_millis(n);
        let window = Duration::from_millis(20);
        let mut path = SinglePath::new(Duration::from_millis(400));
        // Before any media: the join, nothing counted.
        path.observe(ms(0), window, [], true);
        assert!(!path.is_open());
        // One leg from 10 ms, for 5 s, two legs wanted throughout.
        for at in (10..5_010).step_by(5) {
            path.observe(ms(at), window, [ms(at)], true);
            assert_eq!(path.is_open(), at >= 410, "at {at} ms");
        }
        path.close(ms(5_010));
        assert_eq!(path.spans, [(ms(410), ms(5_010))]);
        assert_eq!(path.total, Duration::from_millis(4_600));
        // A second leg that comes after the allowance ends the span; one
        // that comes within it leaves nothing counted.
        let mut late = SinglePath::new(Duration::from_millis(400));
        late.observe(ms(0), window, [ms(0)], true);
        late.observe(ms(600), window, [ms(600)], true);
        late.observe(ms(1_000), window, [ms(1_000), ms(1_000)], true);
        assert_eq!(late.spans, [(ms(400), ms(1_000))]);
        let mut clean = SinglePath::new(Duration::from_millis(400));
        clean.observe(ms(0), window, [ms(0)], true);
        clean.observe(ms(300), window, [ms(300)], true);
        clean.observe(ms(310), window, [ms(310), ms(310)], true);
        clean.close(ms(2_000));
        assert_eq!(clean.total, Duration::ZERO);
    }

    /// Feed `path` as a caller does: the last media of each leg the director
    /// holds.
    fn observe_legs(w: &World, path: &mut SinglePath, window: Duration) {
        let lasts: Vec<Instant> =
            w.d.legs()
                .iter()
                .filter_map(|leg| w.s.views.get(&leg.key).and_then(|view| view.last_media))
                .collect();
        path.observe(w.now, window, lasts, w.d.want() >= 2);
    }

    /// Integration 4, `two_paths::roll_of_three_edges` in the whole suite:
    /// a drained leg released at an answer that names no cache the listener
    /// can take (its other edges skipped after earlier moves) leaves the
    /// session one leg, and no second leg's media to date the span by. The
    /// span began at the join (13 to 14 s in the suite's rolls); it begins
    /// at the release.
    #[test]
    fn a_leg_released_for_want_of_a_cache_is_on_one_path_from_its_release_not_the_join() {
        let mut w = World::new("released-1", 2, false);
        w.join(&[1, 2], None);
        let joined = w.now;
        let (e1, e2) = (w.key("e1"), w.key("e2"));
        let window = Duration::from_millis(20);
        let mut path = SinglePath::new(Duration::from_millis(400));
        // Both legs deliver for 10 s.
        while w.ms() < 10_000 {
            w.run_to(w.ms() + 10);
            let now = w.now;
            for key in [e1, e2] {
                w.s.views.entry(key).or_default().last_media = Some(now);
            }
            observe_legs(&w, &mut path, window);
        }
        assert!(path.spans.is_empty() && !path.is_open());
        let released = w.now;
        // e1 drained; the answer names no cache: e1 goes at once beside e2.
        w.event(LegEvent::Moved {
            leg: e1,
            reason: "draining".into(),
        });
        let (to, nonce, _) = w.requests().pop().expect("e1's replacement asked for");
        w.answer(to, nonce, Vec::new(), 0, false, None);
        assert!(w.s.legs[&e1].released, "{:?}", w.s.log);
        observe_legs(&w, &mut path, window);
        assert!(path.is_open());
        // Asked again on the one-path backoff: e3 comes.
        let (to, nonce) = loop {
            w.run_to(w.ms() + 1);
            let now = w.now;
            w.s.views.entry(e2).or_default().last_media = Some(now);
            observe_legs(&w, &mut path, window);
            if let Some((to, nonce, _)) = w.requests().pop() {
                break (to, nonce);
            }
            assert!(w.ms() < 12_000, "the lost place is asked for again");
        };
        w.answer(to, nonce, vec![cache(3)], 1, false, None);
        w.live("e3");
        let back = w.now;
        w.s.views.entry(e2).or_default().last_media = Some(back);
        observe_legs(&w, &mut path, window);
        assert!(!path.is_open());
        assert_eq!(path.spans.len(), 1);
        let (since, until) = path.spans[0];
        assert!(
            since >= released,
            "the span began {:?} after the join, before the release at {:?}",
            since - joined,
            released - joined
        );
        assert_eq!(until, back);
        assert!(path.total < Duration::from_millis(500), "{:?}", path.total);
    }

    /// Integration 4, `two_paths::roll_of_three_edges` in the whole suite
    /// (roll-3: e1 -> e2 drained, e4 -> e3 rebalanced, then e2 gone with no
    /// record): a drain that comes within 3 s of two earlier moves finds
    /// every other edge skipped by the listener itself. The entry names
    /// nothing; the drained leg was released at once beside the other, and
    /// the listener was on one path until the backoff's next answer. It is
    /// kept, and its place asked for again at once without the edges left
    /// lately: make before break.
    #[test]
    fn a_drain_soon_after_two_moves_keeps_its_leg_and_asks_again_without_the_edges_left() {
        let mut w = World::new("roll-3", 2, false);
        w.join(&[1, 4], None);
        let window = Duration::from_millis(20);
        let mut path = SinglePath::new(Duration::from_millis(400));
        observe_legs(&w, &mut path, window);
        // e1 drained, its place given to e2; e4 rebalanced to e3.
        for (old, new, reason) in [(1, 2, "draining"), (4, 3, "rebalance")] {
            let old_key = w.key(&format!("e{old}"));
            w.event(LegEvent::Moved {
                leg: old_key,
                reason: reason.into(),
            });
            let (to, nonce, _) = w.requests().pop().expect("a place asked for");
            w.answer(to, nonce, vec![cache(new)], 1, false, None);
            w.live(&format!("e{new}"));
            let new_key = w.key(&format!("e{new}"));
            w.s.level.insert((new_key, old_key));
            w.run_to(w.ms() + 5);
            assert!(w.s.legs[&old_key].released);
            observe_legs(&w, &mut path, window);
            w.run_to(w.ms() + 1_000);
            let now = w.now;
            for node in w.held() {
                w.view(&node, |view| view.last_media = Some(now));
            }
            observe_legs(&w, &mut path, window);
        }
        assert_eq!(w.held(), ["e2", "e3"]);
        // e2 drained 1 s after the last move: e1 and e4 are still skipped.
        let e2 = w.key("e2");
        w.event(LegEvent::Moved {
            leg: e2,
            reason: "draining".into(),
        });
        let (to, nonce, request) = w.requests().pop().expect("e2's place asked for");
        let Control::Assign { skip, hold, .. } = request else {
            unreachable!()
        };
        assert_eq!(skip, ["e1", "e2", "e4"]);
        assert_eq!(hold, ["e3"]);
        // The entry names none of the edges skipped, and holds e3.
        w.answer_why(
            to,
            nonce,
            Vec::new(),
            0,
            false,
            None,
            Some(ShortGrant::OneEdge),
        );
        observe_legs(&w, &mut path, window);
        assert!(
            !w.s.legs[&e2].released,
            "e2 is released for want of a cache the listener skips itself: {:?}",
            w.s.log
        );
        let (to, nonce, request) = w.requests().pop().expect("asked again at once");
        let Control::Assign { skip, hold, .. } = request else {
            unreachable!()
        };
        assert_eq!((skip, hold), (vec!["e2".to_owned()], vec!["e3".to_owned()]));
        w.answer(to, nonce, vec![cache(1)], 1, false, None);
        w.live("e1");
        let e1 = w.key("e1");
        w.s.level.insert((e1, e2));
        w.run_to(w.ms() + 5);
        assert!(w.s.legs[&e2].released);
        assert_eq!(w.held(), ["e3", "e1"]);
        let records: Vec<(&str, &str, ReplacementKind)> =
            w.d.replacements
                .iter()
                .map(|record| (record.from.as_str(), record.to.as_str(), record.kind))
                .collect();
        assert_eq!(
            records,
            [
                ("e1", "e2", ReplacementKind::Handover),
                ("e4", "e3", ReplacementKind::Handover),
                ("e2", "e1", ReplacementKind::Handover)
            ]
        );
        observe_legs(&w, &mut path, window);
        assert!(path.spans.is_empty() && !path.is_open(), "{:?}", path.spans);
    }

    /// The lead's question after integration 4: both legs sent away for a
    /// rebalance in the same poll. One request asks for both places (no leg
    /// is held), each new leg stands for its own old one, and each old leg
    /// stays until its own replacement is level: no time on one path.
    #[test]
    fn two_rebalance_moves_in_one_poll_are_each_replaced_make_before_break() {
        let mut w = World::new("two-rebalances", 2, true);
        w.join(&[1, 2], Some(3));
        let (e1, e2) = (w.key("e1"), w.key("e2"));
        let window = Duration::from_millis(20);
        let mut path = SinglePath::new(Duration::from_millis(400));
        observe_legs(&w, &mut path, window);
        w.s.events.push_back(LegEvent::Moved {
            leg: e1,
            reason: "rebalance".into(),
        });
        w.s.events.push_back(LegEvent::Moved {
            leg: e2,
            reason: "rebalance".into(),
        });
        w.poll();
        assert!(w.leaving("e1") && w.leaving("e2"));
        let requests = w.requests();
        assert_eq!(requests.len(), 1, "{requests:?}");
        let (to, nonce, request) = requests[0].clone();
        let Control::Assign {
            paths, skip, hold, ..
        } = request
        else {
            unreachable!()
        };
        assert_eq!(paths, 2);
        assert!(hold.is_empty(), "{hold:?}");
        assert!(skip.contains(&"e1".to_owned()) && skip.contains(&"e2".to_owned()));
        // The entry names the spare's edge and another for the two places.
        w.answer(to, nonce, vec![cache(3), cache(4), cache(5)], 2, true, None);
        let (e3, e4) = (w.key("e3"), w.key("e4"));
        let stands_for = |w: &World, key: LegKey| {
            w.d.legs()
                .iter()
                .find(|leg| leg.key == key)
                .and_then(|leg| leg.stands_for)
        };
        assert_eq!(stands_for(&w, e3), Some(e1));
        assert_eq!(stands_for(&w, e4), Some(e2));
        w.live("e3");
        w.live("e4");
        // e3 level first: e1 goes, e2 stays for e4.
        w.s.level.insert((e3, e1));
        w.run_to(w.ms() + 5);
        observe_legs(&w, &mut path, window);
        assert!(w.s.legs[&e1].released && !w.s.legs[&e2].released);
        assert_eq!(stands_for(&w, e4), Some(e2));
        w.s.level.insert((e4, e2));
        w.run_to(w.ms() + 5);
        observe_legs(&w, &mut path, window);
        assert!(w.s.legs[&e2].released);
        assert_eq!(w.held(), ["e3", "e4"]);
        let handovers: Vec<(&str, &str, ReplacementKind)> =
            w.d.replacements
                .iter()
                .map(|record| (record.from.as_str(), record.to.as_str(), record.kind))
                .collect();
        assert_eq!(
            handovers,
            [
                ("e1", "e3", ReplacementKind::Handover),
                ("e2", "e4", ReplacementKind::Handover)
            ]
        );
        assert!(path.spans.is_empty() && !path.is_open(), "{:?}", path.spans);
    }

    /// A 4K viewer on one leg at e1, objects shown, a spare at e2.
    fn one_leg_viewer(sid: &str) -> World {
        let mut w = World::new(sid, 2, true);
        w.poll();
        let (to, nonce, _) = w.requests().pop().unwrap();
        w.answer_why(
            to,
            nonce,
            vec![cache(1), cache(2), cache(3)],
            1,
            true,
            None,
            Some(ShortGrant::Bitrate),
        );
        w.live("e1");
        assert!(w.token_from(edge_addr(2), 0x22));
        w.event(LegEvent::ObjectLane);
        w.run_to(w.ms() + 100);
        w.take();
        w
    }

    /// Place240 rule 3: a lone leg judged late is left make-before-break:
    /// the entry is asked at once with its node skipped, the old leg is
    /// kept until the replacement's first object, and the move is counted
    /// by reason; the node stays skipped for the hold-off.
    #[test]
    fn a_degraded_lone_leg_moves_make_before_break_and_its_node_is_held_off() {
        let mut w = one_leg_viewer("late-1");
        let e1 = w.key("e1");
        w.event(LegEvent::Degraded {
            leg: e1,
            reason: LATE_PARTS.into(),
        });
        assert!(w.leaving("e1"));
        let requests = w.requests();
        let (to, nonce, request) = requests.last().cloned().expect("asked at once");
        let Control::Assign { skip, .. } = &request else {
            unreachable!()
        };
        assert!(skip.contains(&"e1".to_owned()), "{skip:?}");
        w.answer(to, nonce, vec![cache(3)], 1, false, None);
        assert!(!w.s.legs[&e1].released, "kept until the new leg delivers");
        // The late leg's holes are the new leg's: it starts from the oldest.
        assert_eq!(w.fake("e3").start.from, StartFrom::OldestHole);
        w.live("e3");
        w.run_to(w.ms() + 5);
        assert!(w.s.legs[&e1].released);
        assert_eq!(w.held(), ["e3"]);
        assert_eq!(w.d.stats.quality.late_parts, 1);
        let replaced = w.d.replacements.last().unwrap();
        assert_eq!((replaced.reason.as_str(), replaced.kind), (LATE_PARTS, ReplacementKind::Handover));
        // Skipped for the hold-off, not just the move's 3 s.
        w.run_to(w.ms() + 20_000);
        assert!(w.d.skipped(w.now).contains(&"e1".to_owned()));
        w.run_to(w.ms() + 11_000);
        assert!(!w.d.skipped(w.now).contains(&"e1".to_owned()));
    }

    /// Place240 rule 3's guards: a degradation soon after such a move (the
    /// new edge no better: an upstream stall at every edge) moves nobody
    /// and holds such moves off; more than the minute's budget moves
    /// nobody; a replacement that brings nothing is given up and the old
    /// leg kept.
    #[test]
    fn quality_moves_do_not_ping_pong() {
        let mut w = one_leg_viewer("upstream-1");
        let e1 = w.key("e1");
        w.event(LegEvent::Degraded {
            leg: e1,
            reason: LATE_PARTS.into(),
        });
        let (to, nonce, _) = w.requests().pop().unwrap();
        w.answer(to, nonce, vec![cache(3)], 1, false, None);
        w.live("e3");
        w.run_to(w.ms() + 5);
        assert_eq!(w.held(), ["e3"]);
        // e3 is late too, 2 s later: no better.
        w.run_to(w.ms() + 2_000);
        let e3 = w.key("e3");
        w.event(LegEvent::Degraded {
            leg: e3,
            reason: LATE_PARTS.into(),
        });
        assert!(!w.leaving("e3"));
        assert!(w.requests().is_empty());
        assert_eq!(w.d.stats.quality.no_better, 1);
        // And in the hold-off that follows, still none.
        w.run_to(w.ms() + 15_000);
        w.event(LegEvent::Degraded {
            leg: e3,
            reason: MISSING_PARTS.into(),
        });
        assert!(!w.leaving("e3"));
        assert_eq!(w.d.stats.quality.no_better, 2);
        assert_eq!(w.d.stats.quality.moves(), 1);

        // The budget: at most two a minute.
        let mut w = one_leg_viewer("budget-1");
        w.d.config.quality_no_better = Duration::ZERO;
        for (index, next) in [3_u8, 4].into_iter().enumerate() {
            let node = w.held()[0].clone();
            let key = w.key(&node);
            w.event(LegEvent::Degraded {
                leg: key,
                reason: MISSING_PARTS.into(),
            });
            let (to, nonce, _) = w.requests().pop().unwrap();
            w.answer(to, nonce, vec![cache(next)], 1, false, None);
            w.live(&format!("e{next}"));
            w.run_to(w.ms() + 1_000);
            assert_eq!(w.d.stats.quality.missing_parts, index as u64 + 1);
        }
        let e4 = w.key("e4");
        w.event(LegEvent::Degraded {
            leg: e4,
            reason: MISSING_PARTS.into(),
        });
        assert!(!w.leaving("e4"));
        assert_eq!(w.d.stats.quality.over_budget, 1);

        // No replacement within the give-up: the old leg stays.
        let mut w = one_leg_viewer("giveup-1");
        let e1 = w.key("e1");
        w.event(LegEvent::Degraded {
            leg: e1,
            reason: LATE_PARTS.into(),
        });
        let (to, nonce, _) = w.requests().pop().unwrap();
        w.answer(to, nonce, vec![cache(3)], 1, false, None);
        w.run_to(w.ms() + 3_100);
        assert_eq!(w.held(), ["e1"]);
        assert!(!w.leaving("e1"));
        assert_eq!(w.d.stats.quality.given_up, 1);
        assert!(w.requests().is_empty(), "nothing asked once it is kept");
    }

    /// Place240 rule 4: an edge's distress hint moves a one-leg listener;
    /// a two-leg listener ignores it unless both legs' edges say so.
    #[test]
    fn a_distress_hint_moves_one_leg_and_two_legs_only_together() {
        let mut w = one_leg_viewer("distress-1");
        let e1 = w.key("e1");
        w.event(LegEvent::Distress {
            leg: e1,
            reason: "distress-send-blocked".into(),
        });
        assert!(w.leaving("e1"));
        assert_eq!(w.d.stats.quality.distress, 1);

        let mut w = World::new("distress-2", 2, true);
        w.join(&[1, 2], Some(3));
        let (e1, e2) = (w.key("e1"), w.key("e2"));
        w.event(LegEvent::Distress {
            leg: e1,
            reason: "distress-tee-shed".into(),
        });
        assert!(!w.leaving("e1"));
        assert_eq!(w.d.stats.quality.distress_ignored, 1);
        w.run_to(w.ms() + 500);
        w.event(LegEvent::Distress {
            leg: e2,
            reason: "distress-tee-shed".into(),
        });
        assert!(w.leaving("e2"), "both legs said so");
    }
}
