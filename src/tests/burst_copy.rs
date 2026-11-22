//! Burst copies on the long-haul lanes (low-latency wave 5, W2): a
//! forwarder's audio thread sends each audio source symbol to its compiled
//! children again `--relay-burst-copy-ms` later, so that a burst of loss on
//! the long haul (netem `gemodel 1% 20%`) cannot take a part.
//!
//! The child here is the far end of a long-haul lane: it drops datagrams in
//! the order they arrive by a Gilbert-Elliott model, as netem's `gemodel p
//! r` does (every datagram lost in the bad state), and counts the parts of
//! which no symbol came through: the parts that the next forwarder could
//! not recover. With the copies none should be lost; without them a part
//! goes with each burst that takes its source and its repair.

use super::*;
use crate::fast_plane::view::RlsView;
use relay_session::RelayLimits;

/// netem's `gemodel p r` on the datagrams as they arrive.
struct Bursts {
    bad: bool,
    p: f64,
    r: f64,
    state: u64,
}

impl Bursts {
    fn new(p: f64, r: f64, seed: u64) -> Self {
        Self {
            bad: false,
            p,
            r,
            state: seed | 1,
        }
    }

    fn uniform(&mut self) -> f64 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        (self.state >> 11) as f64 / (1_u64 << 53) as f64
    }

    fn lost(&mut self) -> bool {
        self.bad = if self.bad {
            self.uniform() >= self.r
        } else {
            self.uniform() < self.p
        };
        self.bad
    }
}

struct LaneResult {
    parts: u64,
    /// Parts with no symbol through the lossy lane.
    lost: u64,
    /// Datagrams the lane dropped, of those that arrived.
    dropped: u64,
    arrived: u64,
    /// Parts that only a copy brought.
    saved_by_copy: u64,
    copies_sent: u64,
    /// The shortest time between a source and its copy, in microseconds.
    min_copy_delay_us: Option<u64>,
    /// The copies differ from their originals.
    copy_mismatch: bool,
}

/// Forward `parts` 1 ms parts (a source and one repair symbol each, back
/// to back, as the contributor sends them) through a fast-plane forwarder
/// with burst copies of `burst_copy_ms`, to a child behind a bursty lane.
async fn run_lane(burst_copy_ms: u64, parts: u64, seed: u64) -> LaneResult {
    let (child, _child_video) = bind_class_pair(1_000).await;
    let relay = start_fast_relay_with(
        ParentPath::Secondary,
        &[(child.local_addr().unwrap(), RelayForwardRole::All)],
        fast_plane::relay::RelayClassConfig {
            port_offset: 1_000,
            cpus: [None, None],
            realtime: false,
            tee_capacity: 8_192,
            burst_copy_ms,
            ..Default::default()
        },
    )
    .await;
    let deadline = now_unix_us() + 30_000_000;
    let stream_id = 4001;
    let symbols: Vec<(Vec<u8>, Vec<u8>)> = (0..parts)
        .map(|sequence| {
            let envelope =
                encode_test_canonical_opaque_object(stream_id, sequence, &[sequence as u8; 304]);
            let (sources, repairs) = fast_test_symbols(
                &envelope,
                relay_session::MediaPriority::Audio,
                1_200,
                48,
                1,
                deadline,
            );
            assert_eq!(sources.len(), 1, "a part is one source symbol");
            (sources[0].clone(), repairs[0].clone())
        })
        .collect();

    let receive = tokio::spawn(async move {
        let mut bursts = Bursts::new(0.01, 0.2, seed);
        let mut through: HashMap<u64, bool> = HashMap::new();
        let mut first_source: HashMap<u64, (Instant, Vec<u8>)> = HashMap::new();
        let mut result = LaneResult {
            parts: 0,
            lost: 0,
            dropped: 0,
            arrived: 0,
            saved_by_copy: 0,
            copies_sent: 0,
            min_copy_delay_us: None,
            copy_mismatch: false,
        };
        let mut buf = [0_u8; 2_048];
        loop {
            let Ok(Ok((len, _))) =
                tokio::time::timeout(Duration::from_millis(300), child.recv_from(&mut buf)).await
            else {
                break;
            };
            let datagram = &buf[..len];
            let view = RlsView::parse(datagram, RelayLimits::default()).unwrap();
            let object = view.object;
            let source = matches!(view.role, raptorq_datagram_fec::MediaDatagramRole::Source);
            // The second arrival of a source is its copy.
            let copy = source
                && match first_source.get(&object) {
                    Some((at, original)) => {
                        let delay = at.elapsed().as_micros() as u64;
                        result.min_copy_delay_us =
                            Some(result.min_copy_delay_us.map_or(delay, |min| min.min(delay)));
                        result.copy_mismatch |= original.as_slice() != datagram;
                        true
                    }
                    None => {
                        first_source.insert(object, (Instant::now(), datagram.to_vec()));
                        false
                    }
                };
            result.arrived += 1;
            if bursts.lost() {
                result.dropped += 1;
                through.entry(object).or_insert(false);
                continue;
            }
            let entry = through.entry(object).or_insert(false);
            if !*entry && copy {
                result.saved_by_copy += 1;
            }
            *entry = true;
        }
        result.parts = through.len() as u64;
        result.lost = through.values().filter(|through| !**through).count() as u64;
        result
    });

    // The contributor's pace: a part each millisecond, its source and its
    // repair, which the forwarder sends on back to back.
    let started = Instant::now();
    for (index, (source, repair)) in symbols.iter().enumerate() {
        tokio::time::sleep_until((started + Duration::from_millis(index as u64)).into()).await;
        // The repair on the secondary lane, which carries repair.
        relay
            .primary_sender
            .send_to(source, relay.primary_audio)
            .await
            .unwrap();
        relay
            .secondary_sender
            .send_to(repair, relay.secondary_audio)
            .await
            .unwrap();
    }
    let mut result = receive.await.unwrap();
    result.copies_sent = relay.stats().burst_copies;
    relay.stop().await;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn burst_copies_save_the_parts_that_bursts_take_on_the_long_haul() {
    let parts = 1_500;
    let seed = 0x5eed_0002;
    let without = run_lane(0, parts, seed).await;
    let with = run_lane(fast_plane::burst_copy::DEFAULT_BURST_COPY_MS, parts, seed).await;
    let report = format!(
        "without copies: {} of {} parts lost ({} of {} datagrams dropped); \
         with copies of {} ms: {} lost ({} of {} dropped, {} saved by a copy, {} copies sent)",
        without.lost,
        without.parts,
        without.dropped,
        without.arrived,
        fast_plane::burst_copy::DEFAULT_BURST_COPY_MS,
        with.lost,
        with.dropped,
        with.arrived,
        with.saved_by_copy,
        with.copies_sent,
    );
    println!("{report}");
    assert_eq!(without.parts, parts, "{report}");
    assert_eq!(with.parts, parts, "{report}");
    assert_eq!(without.copies_sent, 0);
    assert_eq!(with.copies_sent, parts, "one copy of each source: {report}");
    assert!(!with.copy_mismatch, "a copy is the original, byte for byte");
    // A copy leaves its delay after the original (the poll's millisecond
    // granularity aside).
    let delay = with.min_copy_delay_us.unwrap();
    assert!(
        delay >= (fast_plane::burst_copy::DEFAULT_BURST_COPY_MS - 1) * 1_000,
        "a copy came {delay} us after its source"
    );
    assert!(without.lost >= 3, "the bursts took too few parts: {report}");
    assert!(with.saved_by_copy > 0, "{report}");
    assert!(
        with.lost * 5 <= without.lost,
        "the copies did not save the parts: {report}"
    );
}

#[tokio::test]
async fn no_copy_goes_out_when_the_copies_are_off() {
    let result = run_lane(0, 20, 3).await;
    assert_eq!(result.copies_sent, 0);
    assert!(result.min_copy_delay_us.is_none());
    assert_eq!(result.arrived, 40);
}
