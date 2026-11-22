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

use crate::client_compact::{smoothed_loss, CompactPlan};
use crate::client_udp::{Control, ObjectReceiver, PartObject, RepairPolicy, REFRESH, UNMEASURED_LOSS};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// How often the session measures the loss of its path.
pub const LOSS_CHECK: Duration = Duration::from_millis(150);
/// How often a request that the edge has not answered is repeated.
pub const UNANSWERED_REFRESH: Duration = Duration::from_millis(300);

/// What a client asks for, as `needletail-udp-client`'s flags say.
#[derive(Debug, Clone, Copy)]
pub struct SessionConfig {
    pub stream_id: u64,
    /// Repair of each RaptorQ block, per thousand source symbols, at least
    /// (by default the lane's).
    pub repair_permille: Option<u32>,
    pub min_repair: Option<u32>,
    /// Move after this long without a datagram from a subscribed edge.
    pub silence: Duration,
    /// Move when the edge does not answer the subscription in this time.
    pub answer: Duration,
    /// Keep the repair that was asked for, whatever the measured loss.
    pub fixed_repair: bool,
    pub fast_lane: bool,
    pub compact: bool,
    pub compact_burst_rows: bool,
    /// Parts whose bytes the compact lane keeps for window repair (0: the
    /// receiver's default of 4,096; a load host keeps fewer).
    pub known_parts: usize,
}

impl SessionConfig {
    /// The defaults of `needletail-udp-client` for `stream_id`.
    pub fn new(stream_id: u64) -> Self {
        Self {
            stream_id,
            repair_permille: None,
            min_repair: None,
            silence: Duration::from_millis(1_000),
            answer: Duration::from_millis(5_000),
            fixed_repair: false,
            fast_lane: true,
            compact: true,
            compact_burst_rows: true,
            known_parts: 0,
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
    /// The last edge's `OK` said `C` (kept until the next edge's `OK`).
    pub compact_confirmed: bool,
    pub nack_datagrams: u64,
    /// The part that the first edge said it would send first.
    pub first_subscribed_part: Option<u64>,
    /// Edges begun.
    pub edges: u64,
    // The edge in use.
    target: Option<SocketAddr>,
    from_part: Option<u64>,
    token: Option<u64>,
    subscribed: bool,
    asked_at: Instant,
    last_request_at: Instant,
    unanswered: u32,
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
            // shows loss.
            plan: CompactPlan::CLEAN,
            plan_changes: 0,
            burst_estimate: None,
            compact_estimate: None,
            compact_asked: config.compact,
            compact_fallbacks: 0,
            compact_confirmed: false,
            nack_datagrams: 0,
            first_subscribed_part: None,
            edges: 0,
            target: None,
            from_part: None,
            token: None,
            subscribed: false,
            asked_at: now,
            last_request_at: now,
            unanswered: 0,
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
        self.target = Some(target);
        self.from_part = from_part;
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
        self.next_refresh = now + UNANSWERED_REFRESH;
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
    pub fn poll(&mut self, now: Instant, send: &mut dyn FnMut(SocketAddr, &[u8])) -> Option<String> {
        let target = self.target?;
        if !self.subscribed && now.duration_since(self.asked_at) > self.config.answer {
            return Some(format!("no-answer-{}ms", self.config.answer.as_millis()));
        }
        if self.subscribed && now.duration_since(self.last_datagram) > self.config.silence {
            return Some(format!("silence-{}ms", self.config.silence.as_millis()));
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
                    );
                    if wanted != self.plan {
                        self.plan = wanted;
                        self.plan_changes += 1;
                        self.next_refresh = now;
                    }
                }
            }
        }
        if self.compact_confirmed {
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
        if now >= self.next_refresh {
            // An edge that has not answered two requests for the compact
            // lane (an edge older than the lane drops them) is asked without
            // it, on the RaptorQ lane with its repair.
            if self.compact_asked && self.token.is_none() && !self.subscribed && self.unanswered >= 3
            {
                self.compact_asked = false;
                self.compact_fallbacks += 1;
                self.base_repair = self.config.base_for(false);
                self.repair = self.policy_for_base();
            }
            // Before the edge answers, keep asking from the same part.
            let from = if self.subscribed {
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
                    UNANSWERED_REFRESH
                };
        }
        None
    }

    /// When [`Self::poll`] wants to run next.
    pub fn next_wake(&self) -> Instant {
        let silence_at = if self.subscribed {
            self.last_datagram + self.config.silence
        } else {
            self.asked_at + self.config.answer
        };
        let mut wake = self
            .next_refresh
            .min(self.next_loss_check)
            .min(silence_at + Duration::from_millis(1));
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
                    if self.token != Some(given) {
                        if self.token.is_none() {
                            // A request and its answer: the round trip that
                            // the NACKs wait for.
                            self.receiver
                                .compact
                                .set_rtt(now.saturating_duration_since(self.last_request_at));
                        }
                        self.token = Some(given);
                        send(target, &self.subscribe_datagram(self.from_part));
                        self.last_request_at = Instant::now();
                    }
                    Received::Control
                }
                Control::Subscribed {
                    next_part, compact, ..
                } => {
                    let first = !self.subscribed;
                    if first {
                        self.first_subscribed_part.get_or_insert(next_part);
                        self.receiver
                            .expect(self.config.stream_id, next_part, compact);
                    }
                    self.compact_confirmed = compact;
                    self.subscribed = true;
                    self.next_refresh = Instant::now() + REFRESH;
                    // An edge that does not serve the compact lane (OK
                    // without C): the RaptorQ lane's repair, at once.
                    let wanted_base = self.config.base_for(compact);
                    if wanted_base != self.base_repair {
                        self.base_repair = wanted_base;
                        self.repair = self.policy_for_base();
                        self.next_refresh = Instant::now();
                    }
                    Received::Subscribed { next_part, first }
                }
                Control::Move { reason, .. } | Control::Refused { reason, .. } => {
                    Received::Move(reason)
                }
                _ => Received::Control,
            }
        } else {
            self.pending = self.receiver.push(datagram);
            Received::Media
        }
    }

    /// The objects that the last media datagram delivered, one at a time.
    pub fn next_object(&mut self) -> Option<PartObject> {
        self.pending.take().or_else(|| self.receiver.next_ready())
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
        assert!(session.poll(now + Duration::from_millis(100), &mut send).is_none());
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

    #[test]
    fn an_edge_that_ignores_the_compact_lane_is_asked_without_it() {
        let now = Instant::now();
        let mut session = UdpSession::new(SessionConfig::new(1), now);
        let mut sent: Vec<Vec<u8>> = Vec::new();
        let mut send = |_: SocketAddr, datagram: &[u8]| sent.push(datagram.to_vec());
        session.begin(edge(), None, now, &mut send);
        let mut at = now;
        for _ in 0..3 {
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
