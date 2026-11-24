//! The edge-choosing code of a UDP listener (B-listener.md 3.5): here, only
//! the surface of the session that the director drives ([`SessionLegs`]) and
//! the types it needs, as unit Y1 defines them on `rm/by1`. The session
//! ([`crate::client_session::UdpSession`]) implements it. The director
//! itself is Y1's and joins this file when the two branches meet.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// Legs a session holds at most: two wanted, a replacement or a hedge each.
pub const MAX_LEGS: usize = 4;

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
}

impl LegFailure {
    pub fn reason(self) -> &'static str {
        match self {
            Self::Dead => "silence",
            Self::NoAnswer => "no-answer",
            Self::Stale => "stale-start",
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
