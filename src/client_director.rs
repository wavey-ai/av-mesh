//! The edge-choosing code of a UDP listener (B, section 3.5): what the
//! director needs of a session that holds legs (one subscription of the
//! listener at one edge each). The session ([`crate::client_session`])
//! implements [`SessionLegs`]; the director decides which edges to hold,
//! and the session runs each leg's protocol and reports what happens to it
//! as [`LegEvent`]s.

pub use crate::client_compact::LegId;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// Where a leg is in its life (B 3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegState {
    /// Asked, no `TOKEN` yet.
    Asking,
    /// The edge gave its token; no `OK` yet.
    Tokened,
    /// The edge answered `OK`: it serves the leg.
    Live,
    /// The edge sent `MOVE`: it still serves the leg until it is released
    /// or the edge's grace ends.
    Leaving,
}

/// What happened to a leg, for the director (B 3.1, 3.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LegEvent {
    /// The edge gave its token (the first time, or a new one).
    Tokened { leg: LegId, token: u64 },
    /// No `TOKEN` within the token wait: the director asks another edge;
    /// the leg goes on asking until the answer wait and is used if its edge
    /// answers.
    PassedOver { leg: LegId },
    /// The edge's first `OK`: it sends from `first_part`.
    Live { leg: LegId, first_part: u64 },
    /// The leg brought 28 of the last 32 parts delivered (each time it
    /// becomes so).
    Steady { leg: LegId },
    /// The leg is level with `with`, a leaving leg (once a pair).
    Level { leg: LegId, with: LegId },
    /// The edge sent `MOVE`: the leg is leaving.
    Moved { leg: LegId, reason: String },
    /// Another leg is 8 parts ahead and this one silent: the plans are
    /// made without it.
    Suspect { leg: LegId },
    /// The leg is silent (beside a delivering leg, or alone): hedge it at
    /// the spare.
    HedgeDue { leg: LegId },
    /// The leg failed and is gone (`UNSUB` sent if it held a token).
    Failed {
        leg: LegId,
        node: String,
        reason: String,
    },
    /// The edge took back a leaving leg (after [`SessionLegs::takeback_leg`]).
    TakenBack { leg: LegId },
    /// No leg has sent a datagram for the silence: nobody is failed.
    AllSilent { silence: Duration },
}

/// A leg as the director sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegView {
    pub leg: LegId,
    pub node: String,
    pub target: SocketAddr,
    pub state: LegState,
    pub token: Option<u64>,
    /// The part its edge's `OK` named.
    pub first_part: Option<u64>,
    /// The reason of its edge's `MOVE`, while it is leaving.
    pub leaving: Option<String>,
    /// It brings parts now (live or leaving, not suspect, media within
    /// max(20 ms, 8 part intervals)).
    pub delivering: bool,
    pub suspect: bool,
    /// Its edge's `OK` said `C`: it serves the compact lane.
    pub compact: bool,
    pub rtt: Option<Duration>,
    /// Media datagrams from its edge, their bytes on the wire, and the
    /// longest time between two of them.
    pub datagrams: u64,
    pub wire_bytes: u64,
    pub longest_gap: Duration,
}

/// What a session that holds legs does for the director. Every method that
/// may send takes `send`; the session performs no I/O of its own.
pub trait SessionLegs {
    /// Hold a new leg at `node` (`target` its client UDP address), asked
    /// from `from_part`: a tokenless `SUB`, or with `token` (the spare's,
    /// for a hedge) a `SUB` with it. `None` when the session holds as many
    /// legs as it can.
    fn add_leg(
        &mut self,
        node: &str,
        target: SocketAddr,
        from_part: Option<u64>,
        token: Option<u64>,
        now: Instant,
        send: &mut dyn FnMut(SocketAddr, &[u8]),
    ) -> Option<LegId>;

    /// Let a leg go: `UNSUB` twice, 20 ms apart; a datagram from its edge
    /// in the next 10 s is answered with another.
    fn release_leg(&mut self, leg: LegId, now: Instant, send: &mut dyn FnMut(SocketAddr, &[u8]));

    /// Ask the edge of a leaving leg to keep it (`SUB <newest + 1>
    /// <token>`): a `MOVE` within a round trip and before the `OK` is
    /// ignored.
    fn takeback_leg(&mut self, leg: LegId, now: Instant, send: &mut dyn FnMut(SocketAddr, &[u8]));

    /// Ask a leg's edge again from the oldest hole (else the newest part
    /// + 1), with `r=1`: every leg went silent.
    fn reask_leg(&mut self, leg: LegId, now: Instant, send: &mut dyn FnMut(SocketAddr, &[u8]));

    /// The timers of every leg; what they found is in [`Self::take_events`].
    fn poll_legs(&mut self, now: Instant, send: &mut dyn FnMut(SocketAddr, &[u8]));

    /// The events since the last call, in order.
    fn take_events(&mut self) -> Vec<LegEvent>;

    /// The legs held now.
    fn legs(&self) -> Vec<LegView>;

    /// The newest part delivered, and the oldest part still missing.
    fn newest(&self) -> Option<u64>;
    fn oldest_hole(&self) -> Option<u64>;

    /// Whether a leg is steady, and whether `new` is level with `old`
    /// (B 3.1).
    fn steady(&self, leg: LegId) -> bool;
    fn level(&self, new: LegId, old: LegId) -> bool;

    /// 8 of the last 16 parts delivered came as RaptorQ objects: a stream
    /// over the two-path bit rate (B 3.9).
    fn mostly_raptorq(&self) -> bool;

    /// When [`Self::poll_legs`] wants to run next.
    fn next_wake(&self) -> Instant;
}
