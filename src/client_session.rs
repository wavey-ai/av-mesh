//! One client's subscription to a stream at an edge, without its sockets:
//! the protocol of `needletail-udp-client` as a state machine, so that the
//! client and the load generator (`needletail-load`, thousands of them on
//! one host) run the same code.
//!
//! The session holds what the client keeps across edges (the loss that it
//! measured, the repair and the compact plan that it asks for, whether it
//! asks for the compact lane) and what it keeps for the edge it uses now
//! (the token, whether the edge answered, when it last heard from it).
//! The caller owns the socket and the clock: it passes each datagram that
//! arrived to [`UdpSession::on_datagram`], calls [`UdpSession::poll`] when
//! [`UdpSession::next_wake`] comes, and sends the control datagrams that
//! both hand to its `send` function. It chooses the edge: when `poll` or
//! `on_datagram` says that the client must move (`MOVE`, `REFUSED`, no
//! answer, silence), it picks the next edge and calls
//! [`UdpSession::begin`] with it.
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

use crate::client_compact::{smoothed_loss, CompactPlan, NackTiming};
use crate::client_udp::{
    Control, ObjectReceiver, PartObject, RepairPolicy, REFRESH, UNMEASURED_LOSS,
};
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

/// The silence after which a client moves (wave 8 S2; 1,000 before).
pub const DEFAULT_SILENCE_MS: u64 = 200;
/// See [`SessionConfig::silence_repeat`].
pub const DEFAULT_SILENCE_REPEAT_MS: u64 = 1_000;
/// Times a client asks an edge for the compact lane again after falling
/// back from it (see [`SessionConfig::compact_retry`]).
pub const COMPACT_RETRIES: u32 = 2;

/// What a client asks for, as `needletail-udp-client`'s flags say.
#[derive(Debug, Clone, Copy)]
pub struct SessionConfig {
    pub stream_id: u64,
    /// Repair of each RaptorQ block, per thousand source symbols, at least
    /// (by default the lane's).
    pub repair_permille: Option<u32>,
    pub min_repair: Option<u32>,
    /// Move after this long without a datagram from a subscribed edge
    /// (wave 8 S2: 200 ms; 1 s before).
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
        }
    }

    fn base_for(&self, compact: bool) -> RepairPolicy {
        RepairPolicy::base_for_lane(compact, self.repair_permille, self.min_repair)
    }
}

/// What a datagram was.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Received {
    /// Not from the edge in use, or before any edge.
    Ignored,
    /// A control datagram that changes nothing for the caller.
    Control,
    /// The edge's `OK`; `first` when it is the first at this edge.
    Subscribed { next_part: u64, first: bool },
    /// A media datagram: take its objects with [`UdpSession::next_object`].
    Media,
    /// The edge sent `MOVE` or `REFUSED`: move to the next edge.
    Move(String),
}

#[derive(Debug)]
pub struct UdpSession {
    pub config: SessionConfig,
    pub receiver: ObjectReceiver,
    // Across edges.
    pub base_repair: RepairPolicy,
    pub repair: RepairPolicy,
    pub loss_estimate: Option<f64>,
    /// The largest loss measured in one check, and the largest mean burst.
    pub loss_max: Option<f64>,
    pub burst_max: Option<f64>,
    pub repair_changes: u64,
    pub plan: CompactPlan,
    pub plan_changes: u64,
    pub burst_estimate: Option<f64>,
    compact_estimate: Option<f64>,
    /// The edge in use was asked for the compact lane; edges that did not
    /// answer a request for it and were asked without it.
    pub compact_asked: bool,
    pub compact_fallbacks: u64,
    /// The short form is asked for (until an edge refuses it), and the
    /// edges that refused it and were asked again with the long form.
    pub short_header: bool,
    pub short_header_dropped: u64,
    /// The last edge's `OK` said `C` (kept until the next edge's `OK`).
    pub compact_confirmed: bool,
    pub nack_datagrams: u64,
    /// The part that the first edge said it would send first.
    pub first_subscribed_part: Option<u64>,
    /// Edges begun.
    pub edges: u64,
    /// When the first token and the first `OK` came (time to first audio,
    /// step by step).
    pub first_token_at: Option<Instant>,
    pub first_subscribed_at: Option<Instant>,
    /// While set, the session reports no failure (no answer, silence) and
    /// keeps the edge: the end-of-run drain, which waits for NACK answers.
    pub hold: bool,
    /// Set by the caller before [`Self::poll`]: datagrams are queued on its
    /// socket (NACKs wait for them, with `nack_after_drain`).
    pub queued: bool,
    /// Since when [`Self::queued`] has been set without a break: NACKs wait
    /// for a queue at most one NACK retry (see [`Self::poll`]).
    queued_since: Option<Instant>,
    /// The compact lane asked for again after a fall-back (see
    /// [`SessionConfig::compact_retry`]), over the run and at this edge.
    pub compact_retries: u64,
    compact_retries_here: u32,
    /// When the compact lane was asked for again while subscribed, until an
    /// `OK` with it comes: an edge that drops compact requests (one older
    /// than the lane) never answers, and the client asks without it again
    /// after [`COMPACT_FALLBACK_AFTER`] (wave 8 review: the retry undid the
    /// fall-back, every refresh was dropped and the subscription expired).
    compact_retry_at: Option<Instant>,
    /// Retries given up that way.
    pub compact_retries_unanswered: u64,
    /// The last move was for silence and the edge in use has sent nothing
    /// since: the longer silence applies.
    moved_for_silence: bool,
    // The edge in use.
    target: Option<SocketAddr>,
    from_part: Option<u64>,
    /// The edge in use has sent a part (or a repair datagram) since the
    /// request: it serves this client, so a repeated request no longer
    /// names `from_part` (an edge that has admitted the client would go
    /// back to it and send those parts again; wave 9 review).
    media_here: bool,
    token: Option<u64>,
    subscribed: bool,
    asked_at: Instant,
    last_request_at: Instant,
    unanswered: u32,
    /// Repeats of the request now unanswered (the backoff of
    /// [`SessionConfig::first_retry`]), and the round trip to the edge.
    repeats: u32,
    rtt: Option<Duration>,
    last_datagram: Instant,
    next_refresh: Instant,
    next_loss_check: Instant,
    pending: Option<PartObject>,
}

impl UdpSession {
    pub fn new(config: SessionConfig, now: Instant) -> Self {
        let base_repair = config.base_for(config.compact);
        // Until the path is measured, plan for some loss.
        let repair = if config.fixed_repair {
            base_repair
        } else {
            RepairPolicy::for_loss(UNMEASURED_LOSS, base_repair)
        };
        let mut receiver = ObjectReceiver::new();
        if config.known_parts > 0 {
            receiver.compact.set_known_parts(config.known_parts);
        }
        receiver.compact.set_nack_timing(config.nack_timing);
        receiver.compact.drop_early = config.drop_early_parts;
        Self {
            config,
            receiver,
            base_repair,
            repair,
            loss_estimate: None,
            loss_max: None,
            burst_max: None,
            repair_changes: 0,
            // The compact lane's plan: window repair alone until the path
            // shows loss; the short form until an edge refuses it.
            plan: CompactPlan::CLEAN.with_short_header(config.compact_short_header),
            plan_changes: 0,
            burst_estimate: None,
            compact_estimate: None,
            compact_asked: config.compact,
            compact_fallbacks: 0,
            short_header: config.compact_short_header,
            short_header_dropped: 0,
            compact_confirmed: false,
            nack_datagrams: 0,
            first_subscribed_part: None,
            edges: 0,
            first_token_at: None,
            first_subscribed_at: None,
            hold: false,
            queued: false,
            queued_since: None,
            compact_retries: 0,
            compact_retries_here: 0,
            compact_retry_at: None,
            compact_retries_unanswered: 0,
            moved_for_silence: false,
            target: None,
            from_part: None,
            media_here: false,
            token: None,
            subscribed: false,
            asked_at: now,
            last_request_at: now,
            unanswered: 0,
            repeats: 0,
            rtt: None,
            last_datagram: now,
            next_refresh: now,
            next_loss_check: now,
            pending: None,
        }
    }

    /// The edge in use.
    pub fn target(&self) -> Option<SocketAddr> {
        self.target
    }

    pub fn token(&self) -> Option<u64> {
        self.token
    }

    pub fn is_subscribed(&self) -> bool {
        self.subscribed
    }

    /// How long an unanswered request waits for its answer before it is
    /// repeated (see [`SessionConfig::first_retry`]).
    fn unanswered_wait(&self) -> Duration {
        let first = self.config.first_retry;
        if first.is_zero() {
            return UNANSWERED_REFRESH;
        }
        let first = first.max(self.rtt.map_or(Duration::ZERO, |rtt| rtt * 3));
        first
            .saturating_mul(1 << self.repeats.min(8))
            .min(UNANSWERED_REFRESH)
    }

    fn policy_for_base(&self) -> RepairPolicy {
        if self.config.fixed_repair {
            self.base_repair
        } else {
            RepairPolicy::for_loss(
                self.loss_estimate.unwrap_or(UNMEASURED_LOSS),
                self.base_repair,
            )
        }
    }

    fn subscribe_datagram(&self, from_part: Option<u64>) -> Vec<u8> {
        Control::Subscribe {
            stream_id: self.config.stream_id,
            from_part,
            repair: self.repair,
            token: self.token,
            fast_lane: self.config.fast_lane,
            compact: self.compact_asked.then_some(self.plan),
        }
        .encode()
    }

    /// Subscribe at `target` from `from_part` (the part after the last one
    /// received, if any): leave the edge in use first (`UNSUB`, so that it
    /// stops sending at once) and decode the new edge's symbols apart from
    /// the old edge's.
    pub fn begin(
        &mut self,
        target: SocketAddr,
        from_part: Option<u64>,
        now: Instant,
        send: &mut dyn FnMut(SocketAddr, &[u8]),
    ) {
        // A move: from the oldest hole the old edge left unrepaired.
        let from_part = match (from_part, self.receiver.compact.lowest_hole()) {
            (Some(from), Some(hole))
                if self.config.move_from_hole
                    && self.target.is_some()
                    && hole < from
                    && from - hole <= MOVE_HOLE_PARTS =>
            {
                Some(hole)
            }
            (from, _) => from,
        };
        if let Some(old) = self.target {
            if let Some(token) = self.token {
                send(
                    old,
                    &Control::Unsubscribe {
                        stream_id: self.config.stream_id,
                        token,
                    }
                    .encode(),
                );
            }
            self.receiver.new_flow();
        }
        self.edges += 1;
        self.compact_retries_here = 0;
        self.compact_retry_at = None;
        self.target = Some(target);
        self.from_part = from_part;
        self.media_here = false;
        self.token = None;
        self.pending = None;
        // Each edge is asked for the compact lane again, with its repair.
        if self.compact_asked != self.config.compact {
            self.compact_asked = self.config.compact;
            self.base_repair = self.config.base_for(self.compact_asked);
            self.repair = self.policy_for_base();
        }
        send(target, &self.subscribe_datagram(from_part));
        self.asked_at = now;
        // The last request sent, for the round trip that its answer
        // measures, and the requests that this edge has not answered.
        self.last_request_at = now;
        self.unanswered = 1;
        self.last_datagram = now;
        // Until the edge answers, the request is repeated often.
        self.repeats = 0;
        self.rtt = None;
        self.next_refresh = now + self.unanswered_wait();
        self.next_loss_check = now + LOSS_CHECK;
        self.subscribed = false;
    }

    /// Leave the edge in use (`UNSUB`), at the end of a run.
    pub fn leave(&mut self, send: &mut dyn FnMut(SocketAddr, &[u8])) {
        if let (Some(target), Some(token)) = (self.target, self.token) {
            send(
                target,
                &Control::Unsubscribe {
                    stream_id: self.config.stream_id,
                    token,
                }
                .encode(),
            );
        }
        self.token = None;
        self.subscribed = false;
    }

    /// The timers: whether the edge in use has failed (no answer, silence),
    /// the loss check, the NACKs and the refresh. Returns the reason to
    /// move, if the client must.
    pub fn poll(
        &mut self,
        now: Instant,
        send: &mut dyn FnMut(SocketAddr, &[u8]),
    ) -> Option<String> {
        let target = self.target?;
        if !self.hold && !self.subscribed && now.duration_since(self.asked_at) > self.config.answer
        {
            return Some(format!("no-answer-{}ms", self.config.answer.as_millis()));
        }
        let silence = self.silence();
        if !self.hold && self.subscribed && now.duration_since(self.last_datagram) > silence {
            self.moved_for_silence = true;
            return Some(format!("silence-{}ms", silence.as_millis()));
        }
        if now >= self.next_loss_check {
            self.next_loss_check = now + LOSS_CHECK;
            if let Some(loss) = self.receiver.take_loss(self.repair) {
                self.loss_max = Some(self.loss_max.map_or(loss, |max| max.max(loss)));
                // Rise at once, fall slowly.
                let estimate = match self.loss_estimate {
                    Some(previous) if loss < previous => previous * 0.8 + loss * 0.2,
                    _ => loss,
                };
                self.loss_estimate = Some(estimate);
                // The compact plan follows a smoother estimate: NACK covers
                // what a short rise of the loss leaves.
                let smooth = smoothed_loss(self.compact_estimate, loss);
                self.compact_estimate = Some(smooth);
                if let Some(burst) = self.receiver.compact.take_burst() {
                    self.burst_max = Some(self.burst_max.map_or(burst, |max| max.max(burst)));
                    self.burst_estimate = Some(match self.burst_estimate {
                        Some(previous) => previous * 0.7 + burst * 0.3,
                        None => burst,
                    });
                }
                if !self.config.fixed_repair {
                    let wanted = RepairPolicy::for_loss(estimate, self.base_repair);
                    if wanted != self.repair {
                        self.repair = wanted;
                        self.repair_changes += 1;
                        self.next_refresh = now;
                    }
                    let wanted = CompactPlan::for_measured_with(
                        smooth,
                        self.burst_estimate.unwrap_or(1.0),
                        self.config.compact_burst_rows,
                    )
                    .with_short_header(self.short_header);
                    if wanted != self.plan {
                        self.plan = wanted;
                        self.plan_changes += 1;
                        self.next_refresh = now;
                    }
                }
            }
        }
        // NACKs wait while datagrams are queued (they fill most holes), but
        // no longer than one NACK retry (2 ms, or 1.5 round trips): a
        // stream that keeps the socket busy (4K at 25k datagrams a second)
        // never empties it, and its holes would wait until they were given
        // up (wave 8 review).
        let queued = self.queued && self.config.nack_after_drain;
        let drained = if queued {
            let since = *self.queued_since.get_or_insert(now);
            now.saturating_duration_since(since) >= self.receiver.compact.nack_retry()
        } else {
            self.queued_since = None;
            true
        };
        if self.compact_confirmed && drained {
            if let Some(token) = self.token {
                let parts = self.receiver.nacks_due(now);
                if !parts.is_empty() {
                    self.nack_datagrams += 1;
                    send(
                        target,
                        &Control::Nack {
                            stream_id: self.config.stream_id,
                            token,
                            parts,
                        }
                        .encode(),
                    );
                }
            }
        }
        if self
            .compact_retry_at
            .is_some_and(|at| now.saturating_duration_since(at) >= COMPACT_FALLBACK_AFTER)
        {
            // The retry went unanswered while the subscription without the
            // lane stood: asked without it again, and not retried here.
            self.compact_retry_at = None;
            self.compact_retries_unanswered += 1;
            self.compact_retries_here = COMPACT_RETRIES;
            self.compact_asked = false;
            self.base_repair = self.config.base_for(false);
            self.repair = self.policy_for_base();
            self.next_refresh = now;
        }
        if now >= self.next_refresh {
            // An edge that has not answered two requests for the compact
            // lane (an edge older than the lane drops them) is asked without
            // it, on the RaptorQ lane with its repair: for the rest of the
            // run, so only after 1.5 s of silence (a lost ARP request alone
            // holds a path's first datagrams for a second; wave 7 review).
            if falls_back_from_compact(
                self.compact_asked,
                self.token.is_some() || self.subscribed,
                self.unanswered,
                now.saturating_duration_since(self.asked_at),
            ) {
                self.compact_asked = false;
                self.compact_fallbacks += 1;
                self.base_repair = self.config.base_for(false);
                self.repair = self.policy_for_base();
            }
            // Before the edge answers, keep asking from the same part
            // (until it sends parts: it has admitted the client).
            let from = if self.subscribed || self.media_here {
                None
            } else {
                self.from_part
            };
            send(target, &self.subscribe_datagram(from));
            self.last_request_at = now;
            if self.token.is_none() && !self.subscribed {
                self.unanswered += 1;
            }
            self.next_refresh = now
                + if self.subscribed {
                    REFRESH
                } else {
                    self.repeats = self.repeats.saturating_add(1);
                    self.unanswered_wait()
                };
        }
        None
    }

    /// The silence that moves the client now (see
    /// [`SessionConfig::silence_repeat`]).
    fn silence(&self) -> Duration {
        if self.moved_for_silence {
            self.config.silence.max(self.config.silence_repeat)
        } else {
            self.config.silence
        }
    }

    /// When [`Self::poll`] wants to run next.
    pub fn next_wake(&self) -> Instant {
        let silence_at = if self.subscribed {
            self.last_datagram + self.silence()
        } else {
            self.asked_at + self.config.answer
        };
        let mut wake = self.next_refresh.min(self.next_loss_check);
        // A held session reports no silence: no wake for it either (it
        // would be due at once, every time).
        if !self.hold {
            wake = wake.min(silence_at + Duration::from_millis(1));
        }
        if self.compact_confirmed {
            if let Some(at) = self.receiver.compact.next_nack_at() {
                wake = wake.min(at);
            }
        }
        wake
    }

    /// A datagram from `peer`, received at `now`.
    pub fn on_datagram(
        &mut self,
        peer: SocketAddr,
        datagram: &[u8],
        now: Instant,
        send: &mut dyn FnMut(SocketAddr, &[u8]),
    ) -> Received {
        let Some(target) = self.target else {
            return Received::Ignored;
        };
        if peer.ip() != target.ip() {
            return Received::Ignored;
        }
        self.last_datagram = now;
        if let Some(control) = Control::decode(datagram) {
            match control {
                Control::Token { token: given, .. } => {
                    self.first_token_at.get_or_insert(now);
                    if self.token != Some(given) {
                        // A request and its answer: the round trip that
                        // the NACKs wait for. Not when the request was
                        // repeated (Karn): the answer may be to an earlier
                        // copy, and the time since the last one is short
                        // of the round trip (wave 9 review).
                        if self.token.is_none() && self.repeats == 0 {
                            let rtt = now.saturating_duration_since(self.last_request_at);
                            self.receiver.compact.set_rtt(rtt);
                            self.rtt = Some(rtt);
                        }
                        self.token = Some(given);
                        let from = self.from_part.filter(|_| !self.media_here);
                        send(target, &self.subscribe_datagram(from));
                        self.last_request_at = now;
                        // The request with the token is a new request: its
                        // own repeats, from the first wait.
                        if !self.subscribed && !self.config.first_retry.is_zero() {
                            self.repeats = 0;
                            self.next_refresh = now + self.unanswered_wait();
                        }
                    }
                    Received::Control
                }
                Control::Subscribed {
                    next_part, compact, ..
                } => {
                    // An edge older than the short form refuses a plan word
                    // with it (C...7) and serves the RaptorQ lane: asked
                    // again at once with the long form (wave 7 review). An
                    // edge that refuses the lane for another reason refuses
                    // that too.
                    if drops_short_header(compact, self.compact_asked, self.plan) {
                        self.short_header = false;
                        self.short_header_dropped += 1;
                        self.plan = self.plan.with_short_header(false);
                        self.next_refresh = now;
                        return Received::Control;
                    }
                    let first = !self.subscribed;
                    if first {
                        self.first_subscribed_at.get_or_insert(now);
                        self.first_subscribed_part.get_or_insert(next_part);
                        self.receiver
                            .expect(self.config.stream_id, next_part, compact);
                    }
                    self.compact_confirmed = compact;
                    if compact {
                        self.compact_retry_at = None;
                    }
                    self.subscribed = true;
                    self.next_refresh = now + REFRESH;
                    // An edge that does not serve the compact lane (OK
                    // without C): the RaptorQ lane's repair, at once.
                    let wanted_base = self.config.base_for(compact);
                    if wanted_base != self.base_repair {
                        self.base_repair = wanted_base;
                        self.repair = self.policy_for_base();
                        self.next_refresh = now;
                    }
                    // Answered without the lane that the client gave up for
                    // want of an answer: the edge hears it, so the lane is
                    // asked for again (at most COMPACT_RETRIES times here).
                    if !compact
                        && !self.compact_asked
                        && self.config.compact
                        && self.config.compact_retry
                        && self.compact_fallbacks > 0
                        && self.compact_retries_here < COMPACT_RETRIES
                    {
                        self.compact_retries_here += 1;
                        self.compact_retries += 1;
                        self.compact_retry_at = Some(now);
                        self.compact_asked = true;
                        self.base_repair = self.config.base_for(true);
                        self.repair = self.policy_for_base();
                        self.next_refresh = now;
                    }
                    Received::Subscribed { next_part, first }
                }
                Control::Move { reason, .. } | Control::Refused { reason, .. } => {
                    Received::Move(reason)
                }
                _ => Received::Control,
            }
        } else {
            self.moved_for_silence = false;
            self.media_here = true;
            self.pending = self.receiver.push(datagram);
            Received::Media
        }
    }

    /// The objects that the last media datagram delivered, one at a time.
    pub fn next_object(&mut self) -> Option<PartObject> {
        self.pending.take().or_else(|| self.receiver.next_ready())
    }

    /// Whether [`Self::next_object`] has objects now: after an `OK`, the
    /// parts that came before it (take them at once, not after the next
    /// media datagram).
    pub fn has_objects(&self) -> bool {
        self.pending.is_some() || self.receiver.has_ready()
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

    /// Wave 9 B: an unanswered request is repeated after 25 ms, then after
    /// twice as long each time up to 300 ms; the request with the token
    /// starts its own repeats, after three round trips at least. With
    /// `first_retry` zero, every 300 ms as before.
    #[test]
    fn an_unanswered_request_is_repeated_after_a_few_round_trips() {
        let now = Instant::now();
        let mut session = UdpSession::new(SessionConfig::new(4001), now);
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
        let token = Control::Token { stream_id: 4001, token: 77 }.encode();
        let answered = last + Duration::from_millis(4);
        let mut with_token: Vec<Instant> = Vec::new();
        session.on_datagram(edge(), &token, answered, &mut |_, _| with_token.push(answered));
        assert_eq!(with_token.len(), 1);
        while with_token.len() < 3 {
            let wake = session.next_wake();
            assert!(wake < answered + Duration::from_millis(200));
            session.poll(wake, &mut |_, datagram| {
                assert!(matches!(
                    Control::decode(datagram),
                    Some(Control::Subscribe { token: Some(77), .. })
                ));
                with_token.push(wake)
            });
        }
        assert_eq!(with_token[1] - with_token[0], Duration::from_millis(25));
        assert_eq!(with_token[2] - with_token[1], Duration::from_millis(50));
        // A slower edge: three round trips.
        let mut session = UdpSession::new(SessionConfig::new(4001), now);
        session.begin(edge(), None, now, &mut |_, _| {});
        let answered = now + Duration::from_millis(40);
        session.on_datagram(edge(), &token, answered, &mut |_, _| {});
        assert_eq!(session.next_refresh - answered, Duration::from_millis(120));
        // Before wave 9: every 300 ms.
        let config = SessionConfig {
            first_retry: Duration::ZERO,
            ..SessionConfig::new(4001)
        };
        let mut session = UdpSession::new(config, now);
        session.begin(edge(), None, now, &mut |_, _| {});
        assert_eq!(session.next_refresh - now, UNANSWERED_REFRESH);
        session.poll(now + UNANSWERED_REFRESH, &mut |_, _| {});
        assert_eq!(session.next_refresh - now, UNANSWERED_REFRESH * 2);
    }

    /// Wave 9 review: an answer to a repeated request gives no round trip
    /// (Karn), and once the edge sends parts a repeated request no longer
    /// names the part to resume from (the edge would go back to it).
    #[test]
    fn a_repeated_request_gives_no_round_trip_and_stops_naming_its_part() {
        let now = Instant::now();
        let mut session = UdpSession::new(SessionConfig::new(4001), now);
        session.begin(edge(), Some(500), now, &mut |_, _| {});
        // The first request is repeated at 25 ms; the token comes at 45 ms
        // (an answer to either: 45 or 20 ms).
        let repeat = session.next_wake();
        assert_eq!(repeat - now, Duration::from_millis(25));
        session.poll(repeat, &mut |_, _| {});
        let token = Control::Token { stream_id: 4001, token: 9 }.encode();
        let answered = now + Duration::from_millis(45);
        let mut sent: Vec<Vec<u8>> = Vec::new();
        session.on_datagram(edge(), &token, answered, &mut |_, datagram| {
            sent.push(datagram.to_vec())
        });
        assert_eq!(session.rtt, None);
        // So the request with the token waits the first retry, not 3 x 20.
        assert_eq!(session.next_refresh - answered, Duration::from_millis(25));
        match Control::decode(&sent[0]) {
            Some(Control::Subscribe { from_part, .. }) => assert_eq!(from_part, Some(500)),
            other => panic!("{other:?}"),
        }
        // Unrepeated, the answer is a sample.
        let mut fresh = UdpSession::new(SessionConfig::new(4001), now);
        fresh.begin(edge(), None, now, &mut |_, _| {});
        fresh.on_datagram(edge(), &token, answered, &mut |_, _| {});
        assert_eq!(fresh.rtt, Some(Duration::from_millis(45)));
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
        let wake = session.next_refresh;
        let mut repeated: Vec<Vec<u8>> = Vec::new();
        session.poll(wake, &mut |_, datagram| repeated.push(datagram.to_vec()));
        let sub = repeated
            .iter()
            .find_map(|datagram| match Control::decode(datagram) {
                Some(Control::Subscribe { from_part, token, .. }) => Some((from_part, token)),
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
        assert!(session.compact_confirmed);
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
        assert_eq!(session.receiver.flows, 3);
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
        assert!(session.poll(now + Duration::from_millis(150), &mut send).is_none());
        let reason = session
            .poll(now + Duration::from_millis(201), &mut send)
            .expect("move");
        assert_eq!(reason, "silence-200ms");
        // The next edge answers but sends nothing.
        let moved_at = now + Duration::from_millis(201);
        let next: SocketAddr = "127.0.0.3:19446".parse().unwrap();
        session.begin(next, Some(11), moved_at, &mut send);
        let token = Control::Token { stream_id: 1, token: 6 }.encode();
        session.on_datagram(next, &token, moved_at, &mut send);
        let ok = Control::Subscribed { stream_id: 1, next_part: 11, compact: true }.encode();
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
            let token = Control::Token { stream_id: 3, token: 4 }.encode();
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
            let ok = Control::Subscribed { stream_id: 3, next_part: 70_000, compact: true }.encode();
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
            let token = Control::Token { stream_id: 3, token: 4 }.encode();
            session.on_datagram(edge(), &token, now, &mut send);
            let ok = Control::Subscribed { stream_id: 3, next_part: 100, compact: true }.encode();
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
            assert_eq!(session.from_part, expected, "move_from_hole {move_from_hole}");
            // A first subscription is not a move: the part asked is kept.
            let mut fresh = UdpSession::new(SessionConfig::new(3), now);
            fresh.begin(edge(), Some(105), now, &mut send);
            assert_eq!(fresh.from_part, Some(105));
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
        let token = Control::Token { stream_id: 3, token: 4 }.encode();
        session.on_datagram(edge(), &token, now, &mut send);
        for part in 70_000..70_004_u64 {
            let long =
                crate::client_compact::encode_part(part, 1_700_000_000_000_000, b"pcm").unwrap();
            let short = crate::client_compact::shorten(&long).expect("short form");
            session.on_datagram(edge(), &short, now, &mut send);
        }
        assert!(!session.has_objects());
        let ok = Control::Subscribed { stream_id: 3, next_part: 70_000, compact: true }.encode();
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
        assert_eq!(session.next_object().map(|object| object.part), Some(70_004));
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
        while session.compact_asked {
            at += UNANSWERED_REFRESH;
            session.poll(at, &mut send);
        }
        assert_eq!(session.compact_fallbacks, 1);
        // The edge answers the request without the lane.
        let token = Control::Token { stream_id: 1, token: 8 }.encode();
        session.on_datagram(edge(), &token, at, &mut send);
        let ok = Control::Subscribed { stream_id: 1, next_part: 10, compact: false }.encode();
        session.on_datagram(edge(), &ok, at, &mut send);
        assert!(session.compact_asked);
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
        let wait = Control::Waiting { stream_id: 1, next_part: 10 }.encode();
        while session.compact_asked {
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
        assert!(!session.compact_asked);
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
        while session.compact_asked {
            at += UNANSWERED_REFRESH;
            session.poll(at, &mut send);
        }
        session.on_datagram(edge(), &token, at, &mut send);
        session.on_datagram(edge(), &ok, at, &mut send);
        assert!(!session.compact_asked);
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
        assert!(session.compact_asked);
        while at < now + COMPACT_FALLBACK_AFTER {
            at += UNANSWERED_REFRESH;
            assert!(session.poll(at, &mut send).is_none());
        }
        assert!(!session.compact_asked);
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
        assert!(session.plan.short_header);
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
        assert!(!session.compact_confirmed);
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
}
