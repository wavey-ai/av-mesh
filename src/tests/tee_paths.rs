//! teeperf: the receiver's short path (`RelayUdpDispatch::push_parsed`,
//! what the tee runs for a symbol of a part under way) against its full
//! path (`RelayUdpDispatch::push`). Property-style: random parts, symbol
//! sizes and repair, and arrivals reordered, duplicated, from both parents,
//! with sources lost (repaired by RaptorQ), several parts interleaved. Both
//! paths give the same outcome for every datagram, the same counters, and
//! every part that decodes is byte for byte its source envelope.

use super::*;
use av_mesh::relay_ingress::ParsedOutcome;
use fast_plane::view::RlsView;

const PRIMARY: &str = "127.0.0.1:41001";
const SECONDARY: &str = "127.0.0.1:41002";

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn dispatch() -> RelayUdpDispatch {
    let mut dispatch = fast_test_dispatch(
        PRIMARY.parse().unwrap(),
        SECONDARY.parse().unwrap(),
        ParentPath::PromotedSecondary,
    );
    dispatch.receiver_mut().set_defer_decode(2);
    dispatch
}

/// What a path made of one datagram (and of the decodes it made ready).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Seen {
    Buffered,
    Duplicate,
    Decoded(u64, Vec<u8>),
    Rejected(&'static str),
}

fn decoded(outcome: RelayIngressOutcome) -> Seen {
    match outcome {
        RelayIngressOutcome::Buffered { .. } => Seen::Buffered,
        RelayIngressOutcome::Duplicate { .. } => Seen::Duplicate,
        RelayIngressOutcome::Decoded {
            object, envelope, ..
        } => Seen::Decoded(object.key().object(), envelope.to_vec()),
    }
}

/// Feed `arrivals` through one path; the decodes a symbol makes ready run
/// at once, as on the pool.
fn feed(parsed: bool, arrivals: &[(SocketAddr, Vec<u8>)]) -> (Vec<Vec<Seen>>, String, u64) {
    let mut dispatch = dispatch();
    let limits = dispatch.receiver().config().relay_limits;
    let mut seen = Vec::new();
    for (index, (peer, datagram)) in arrivals.iter().enumerate() {
        let now_us = 1_000 + index as u64;
        let mut step = Vec::new();
        let outcome = if parsed {
            let view = RlsView::parse(datagram, limits).expect("a valid datagram");
            let symbol = view.parsed_symbol(datagram).expect("an RLS1 symbol");
            dispatch
                .push_parsed(*peer, datagram, &symbol, now_us)
                .map(|outcome| match outcome {
                    ParsedOutcome::Buffered => Seen::Buffered,
                    ParsedOutcome::Duplicate => Seen::Duplicate,
                    ParsedOutcome::Decoded(outcome) => decoded(outcome),
                    ParsedOutcome::Legacy => Seen::Rejected("legacy"),
                })
        } else {
            dispatch
                .push(*peer, datagram, now_us)
                .map(|outcome| match outcome {
                    RelayUdpDispatchOutcome::Relay(outcome) => decoded(outcome),
                    RelayUdpDispatchOutcome::Legacy => Seen::Rejected("legacy"),
                })
        };
        step.push(outcome.unwrap_or_else(|error| Seen::Rejected(error.kind().as_str())));
        for job in dispatch.receiver_mut().take_decode_jobs() {
            let envelope = job.run();
            if let Ok(Some(outcome)) =
                dispatch
                    .receiver_mut()
                    .complete_decode(job.key(), envelope, now_us)
            {
                step.push(decoded(outcome));
            }
        }
        seen.push(step);
    }
    (
        seen,
        format!("{:?}", dispatch.receiver().snapshot().counters),
        dispatch.receiver().short_path_datagrams(),
    )
}

#[test]
fn the_short_path_takes_every_arrival_as_the_full_path_does() {
    let deadline = now_unix_us() + 600_000_000;
    let primary: SocketAddr = PRIMARY.parse().unwrap();
    let secondary: SocketAddr = SECONDARY.parse().unwrap();
    let mut decoded_parts = 0;
    let mut repaired_parts = 0;
    let (mut short_path, mut all) = (0_u64, 0_u64);
    for seed in 1..=40_u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let parts = 1 + rng.below(3);
        let symbol_size = [64_u16, 200, 1_088, 1_316][rng.below(4) as usize];
        let mut envelopes = HashMap::new();
        let mut streams: Vec<Vec<(SocketAddr, Vec<u8>)>> = Vec::new();
        for part in 0..parts {
            let sequence = seed * 10 + part;
            let len = 200 + rng.below(60_000) as usize;
            let payload: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
            let envelope = encode_test_canonical_opaque_object(7, sequence, &payload);
            let sources_needed = envelope.len().div_ceil(usize::from(symbol_size)) as u32;
            let repair = 1 + rng.below(u64::from(sources_needed / 4 + 3)) as u32;
            let (sources, repairs) = fast_test_symbols(
                &envelope,
                relay_session::MediaPriority::VideoDelta,
                symbol_size,
                u16::MAX,
                repair,
                deadline,
            );
            envelopes.insert(sequence, envelope.to_vec());
            let lose = rng.below(4) == 0;
            let mut lost = 0_u32;
            let mut arrivals = Vec::new();
            for source in sources {
                // A part loses a source now and then, never more than its
                // repair covers.
                if lose && lost + 1 < repair && rng.below(10) == 0 {
                    lost += 1;
                    continue;
                }
                let peer = if rng.below(3) == 0 { secondary } else { primary };
                if rng.below(12) == 0 {
                    arrivals.push((secondary, source.clone()));
                }
                arrivals.push((peer, source));
            }
            for symbol in repairs {
                arrivals.push((secondary, symbol));
            }
            // Local reordering: swaps within a few places.
            for at in 0..arrivals.len() {
                let to = (at + rng.below(5) as usize).min(arrivals.len() - 1);
                arrivals.swap(at, to);
            }
            // Late copies, after the part is whole.
            for _ in 0..rng.below(4) {
                let pick = rng.below(arrivals.len() as u64) as usize;
                let copy = arrivals[pick].clone();
                arrivals.push(copy);
            }
            streams.push(arrivals);
        }
        // The parts interleaved, as two parents and a pause make them.
        let mut arrivals = Vec::new();
        let mut at = vec![0_usize; streams.len()];
        while at.iter().zip(&streams).any(|(at, stream)| *at < stream.len()) {
            let pick = rng.below(streams.len() as u64) as usize;
            if at[pick] < streams[pick].len() {
                arrivals.push(streams[pick][at[pick]].clone());
                at[pick] += 1;
            }
        }
        let (full, full_counters, none) = feed(false, &arrivals);
        let (short, short_counters, took) = feed(true, &arrivals);
        assert_eq!(none, 0);
        short_path += took;
        all += arrivals.len() as u64;
        assert_eq!(full, short, "seed {seed}: outcomes differ");
        assert_eq!(full_counters, short_counters, "seed {seed}: counters differ");
        let mut done = std::collections::HashSet::new();
        for seen in short.iter().flatten() {
            if let Seen::Decoded(sequence, envelope) = seen {
                assert_eq!(
                    envelope, &envelopes[sequence],
                    "seed {seed}: part {sequence} not byte for byte its source"
                );
                assert!(done.insert(*sequence), "seed {seed}: part {sequence} twice");
            }
        }
        assert_eq!(done.len(), envelopes.len(), "seed {seed}: every part decodes");
        decoded_parts += done.len();
        repaired_parts += usize::from(full_counters.contains("fec_recovered_objects: 0") == false);
    }
    assert!(decoded_parts >= 40);
    // Every symbol after a part's first from each parent.
    assert!(short_path * 10 >= all * 8, "short path {short_path} of {all}");
    assert!(repaired_parts > 0, "some seeds repaired a part");
}
