//! The repair horizon of the live path (low-latency wave 5, W2).
//!
//! Before, a part that a node never recovered held the stream's contiguous
//! watermark (the LL-HLS playlist and the cache-mesh push) for the gap hold,
//! 1 s (400 ms on long-haul distributors): with 1 ms parts and any loss on
//! the long haul the referee measured every part about 0.9 s late at the
//! distributors. Now a hole holds the parts behind it only for the repair
//! horizon (`--canonical-repair-horizon`, by default twice the repair delay
//! that the node measures), counted from when the first part after it came;
//! then they are published and the hole is listed as a gap. A repair that
//! comes later fills the hole in the cache.
//!
//! `watermark_delay_under_long_haul_loss` (ignored, a measurement) prints
//! how long parts wait behind holes for the watermark with the gap hold
//! alone and with the horizon: `cargo test --release
//! watermark_delay_under_long_haul_loss -- --ignored --nocapture`.

use super::*;

const STREAM: u64 = 77;

/// A canonical cache of 5 ms parts with the deployed 1 s gap hold and the
/// given repair horizon.
async fn horizon_cache(horizon: RepairHorizon) -> Arc<LiveTsCache> {
    let cache = LiveTsCache::new(1, Duration::from_millis(5), 2, 64, 64).await;
    cache.set_canonical_gap_hold(Duration::from_millis(DEFAULT_CANONICAL_GAP_HOLD_MS));
    cache.set_repair_horizon(horizon);
    cache
}

async fn commit_init(cache: &LiveTsCache) {
    cache
        .commit_stream_payload(
            STREAM,
            encode_test_canonical_fmp4_object(
                STREAM,
                0,
                ObjectKind::Initialization,
                false,
                b"ftyp-moov",
            ),
        )
        .await
        .unwrap();
}

async fn commit(cache: &LiveTsCache, sequence: u64) {
    assert_eq!(
        cache
            .commit_stream_payload(
                STREAM,
                encode_test_canonical_fmp4_object(
                    STREAM,
                    sequence,
                    ObjectKind::Media,
                    true,
                    format!("moof-mdat-{sequence}").as_bytes(),
                ),
            )
            .await
            .unwrap(),
        sequence
    );
}

async fn watermark(cache: &LiveTsCache) -> Option<usize> {
    let stream_idx = cache.chunk_cache.get_stream_idx(STREAM).await?;
    cache.chunk_cache.last(stream_idx)
}

#[test]
fn the_horizon_flag_reads_auto_off_and_milliseconds() {
    assert_eq!("auto".parse::<RepairHorizon>(), Ok(RepairHorizon::Auto));
    assert_eq!("off".parse::<RepairHorizon>(), Ok(RepairHorizon::Off));
    assert_eq!("0".parse::<RepairHorizon>(), Ok(RepairHorizon::Off));
    assert_eq!("45".parse::<RepairHorizon>(), Ok(RepairHorizon::Fixed(45)));
    assert!("soon".parse::<RepairHorizon>().is_err());
    for horizon in [
        RepairHorizon::Off,
        RepairHorizon::Auto,
        RepairHorizon::Fixed(1),
        RepairHorizon::Fixed(45),
    ] {
        assert_eq!(RepairHorizon::from_code(horizon.code()), horizon);
    }
}

#[test]
fn the_measured_horizon_is_twice_the_ninetieth_percentile_within_bounds() {
    let mut delays = RepairDelays::default();
    assert_eq!(delays.horizon_us(), REPAIR_HORIZON_INITIAL_MS * 1_000);
    // Fast repairs: the floor.
    for _ in 0..50 {
        delays.record(500);
    }
    assert_eq!(delays.horizon_us(), REPAIR_HORIZON_MIN_MS * 1_000);
    // A long-haul repair round trip of about 20 ms in most repairs.
    for _ in 0..100 {
        delays.record(20_000);
    }
    assert_eq!(delays.p90_us(), Some(20_000));
    assert_eq!(delays.horizon_us(), 40_000);
    // The newest samples only: very slow repairs raise it to the ceiling.
    for _ in 0..REPAIR_DELAY_SAMPLES {
        delays.record(900_000);
    }
    assert_eq!(delays.horizon_us(), REPAIR_HORIZON_MAX_MS * 1_000);
    assert_eq!(delays.total, 150 + REPAIR_DELAY_SAMPLES as u64);
}

#[tokio::test]
async fn the_horizon_is_never_longer_than_the_gap_hold_and_off_without_it() {
    let cache = horizon_cache(RepairHorizon::Fixed(5_000)).await;
    assert_eq!(cache.repair_horizon_us(), Some(1_000_000));
    cache.set_canonical_gap_hold(Duration::ZERO);
    assert_eq!(cache.repair_horizon_us(), None);
    let off = horizon_cache(RepairHorizon::Off).await;
    assert_eq!(off.repair_horizon_us(), None);
    let auto = horizon_cache(RepairHorizon::Auto).await;
    assert_eq!(
        auto.repair_horizon_us(),
        Some(REPAIR_HORIZON_INITIAL_MS * 1_000)
    );
}

#[tokio::test]
async fn a_hole_holds_the_watermark_only_for_the_repair_horizon() {
    let horizon = Duration::from_millis(40);
    let cache = horizon_cache(RepairHorizon::Fixed(40)).await;
    commit_init(&cache).await;
    for sequence in 0..5 {
        commit(&cache, sequence).await;
    }
    assert_eq!(watermark(&cache).await, Some(4));
    // Part 5 is lost. The parts after it are stored at once, readable by
    // their number, but the watermark waits for the hole's repair.
    let hole_seen = Instant::now();
    for sequence in 6..9 {
        commit(&cache, sequence).await;
        assert!(cache
            .get_part_for_stream_id(STREAM, sequence)
            .await
            .is_some());
    }
    assert_eq!(watermark(&cache).await, Some(4));
    assert!(!cache.is_gap_part(STREAM, 5));

    // Within the horizon a part that comes still waits.
    tokio::time::sleep(horizon / 4).await;
    commit(&cache, 9).await;
    assert_eq!(watermark(&cache).await, Some(4));

    // Once the hole is the horizon old the next commit publishes every
    // part behind it, and lists the hole as a gap.
    tokio::time::sleep(horizon.saturating_sub(hole_seen.elapsed()) + Duration::from_millis(5))
        .await;
    commit(&cache, 10).await;
    assert_eq!(watermark(&cache).await, Some(10));
    assert!(cache.is_gap_part(STREAM, 5));
    let snapshot = cache.canonical_gap_skip_snapshot();
    assert_eq!(snapshot.repair_horizon, "40");
    assert_eq!(snapshot.repair_horizon_us, Some(40_000));
    assert_eq!(snapshot.repair_horizon_releases, 1);
    assert_eq!(snapshot.skipped_parts, 1);
    assert_eq!(
        cache.live_hole_wait(STREAM, 5, Duration::ZERO),
        Some(Duration::ZERO)
    );

    // A repair that comes late fills the hole for later readers.
    commit(&cache, 5).await;
    assert!(!cache.is_gap_part(STREAM, 5));
    assert!(cache.get_part_for_stream_id(STREAM, 5).await.is_some());
    let snapshot = cache.canonical_gap_skip_snapshot();
    assert_eq!(snapshot.late_gap_fills, 1);
    // It came about the horizon after part 6: a repair delay.
    assert_eq!(snapshot.repair_delays, 1);
    assert!(snapshot.repair_delay_p90_us.unwrap() >= 40_000);
    assert_eq!(watermark(&cache).await, Some(10));
}

#[tokio::test]
async fn a_repair_within_the_horizon_publishes_in_order_and_is_measured() {
    let cache = horizon_cache(RepairHorizon::Auto).await;
    commit_init(&cache).await;
    for sequence in (0..8).filter(|sequence| *sequence != 3) {
        commit(&cache, sequence).await;
    }
    assert_eq!(watermark(&cache).await, Some(2));
    tokio::time::sleep(Duration::from_millis(5)).await;
    commit(&cache, 3).await;
    assert_eq!(watermark(&cache).await, Some(7));
    let snapshot = cache.canonical_gap_skip_snapshot();
    assert_eq!(snapshot.skipped_parts, 0);
    assert_eq!(snapshot.repair_horizon_releases, 0);
    assert_eq!(snapshot.late_gap_fills, 0);
    assert_eq!(snapshot.repair_delays, 1);
    let delay_us = snapshot.repair_delay_p90_us.unwrap();
    assert!((5_000..200_000).contains(&delay_us), "{delay_us}");
    // Auto: twice the measured repair, at least the floor.
    assert_eq!(
        snapshot.repair_horizon_us,
        Some((2 * delay_us).clamp(REPAIR_HORIZON_MIN_MS * 1_000, REPAIR_HORIZON_MAX_MS * 1_000))
    );
}

#[tokio::test]
async fn a_stalled_stream_has_its_hole_released_without_another_commit() {
    let cache = horizon_cache(RepairHorizon::Fixed(20)).await;
    commit_init(&cache).await;
    for sequence in [0, 1, 3, 4] {
        commit(&cache, sequence).await;
    }
    assert_eq!(cache.release_expired_repair_holes().await, 0);
    assert_eq!(watermark(&cache).await, Some(1));
    tokio::time::sleep(Duration::from_millis(25)).await;
    let updates = cache.part_updates.subscribe();
    assert_eq!(cache.release_expired_repair_holes().await, 1);
    assert_eq!(watermark(&cache).await, Some(4));
    assert!(cache.is_gap_part(STREAM, 2));
    // Readers that wait for the playlist are woken.
    assert!(updates.has_changed().unwrap());
    // Nothing more to release.
    assert_eq!(cache.release_expired_repair_holes().await, 0);
}

#[tokio::test]
async fn only_the_holes_past_the_horizon_are_released() {
    let cache = horizon_cache(RepairHorizon::Fixed(30)).await;
    commit_init(&cache).await;
    for sequence in [0, 1, 3, 4] {
        commit(&cache, sequence).await;
    }
    tokio::time::sleep(Duration::from_millis(35)).await;
    // A second hole (7) opens now: it is young.
    for sequence in [5, 6, 8] {
        commit(&cache, sequence).await;
    }
    assert_eq!(watermark(&cache).await, Some(6));
    assert!(cache.is_gap_part(STREAM, 2));
    assert!(!cache.is_gap_part(STREAM, 7));
}

#[tokio::test]
async fn the_gap_hold_alone_still_holds_when_the_horizon_is_off() {
    let cache = horizon_cache(RepairHorizon::Off).await;
    commit_init(&cache).await;
    for sequence in [0, 1, 3, 4] {
        commit(&cache, sequence).await;
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    commit(&cache, 5).await;
    assert_eq!(cache.release_expired_repair_holes().await, 0);
    assert_eq!(watermark(&cache).await, Some(1));
    assert_eq!(cache.live_hole_wait(STREAM, 2, Duration::ZERO), None);
}

#[tokio::test]
async fn the_playlist_lists_a_released_hole_as_a_gap_until_it_is_filled() {
    let cache = horizon_cache(RepairHorizon::Fixed(10)).await;
    commit_init(&cache).await;
    for sequence in [0, 1, 2, 4] {
        commit(&cache, sequence).await;
    }
    let held = cache.playlist_for_stream_id(STREAM).await;
    assert!(held.contains("URI=\"part2.mp4\""), "{held}");
    assert!(!held.contains("part4.mp4"), "{held}");
    tokio::time::sleep(Duration::from_millis(15)).await;
    commit(&cache, 5).await;
    let playlist = cache.playlist_for_stream_id(STREAM).await;
    assert!(
        playlist.contains("#EXT-X-PART:DURATION=0.005,URI=\"part3.mp4\",GAP=YES\n"),
        "{playlist}"
    );
    // In its place, between the parts around it.
    let gap = playlist.find("part3.mp4").unwrap();
    assert!(playlist.find("part2.mp4").unwrap() < gap);
    assert!(gap < playlist.find("part4.mp4").unwrap());
    assert!(playlist.contains("URI=\"part5.mp4\""), "{playlist}");

    commit(&cache, 3).await;
    let filled = cache.playlist_for_stream_id(STREAM).await;
    assert!(!filled.contains("GAP=YES"), "{filled}");
    assert!(filled.contains("URI=\"part3.mp4\""), "{filled}");
}

#[tokio::test]
async fn a_new_epoch_forgets_the_gaps_of_the_old() {
    let cache = horizon_cache(RepairHorizon::Fixed(10)).await;
    commit_init(&cache).await;
    for sequence in [0, 2] {
        commit(&cache, sequence).await;
    }
    tokio::time::sleep(Duration::from_millis(15)).await;
    commit(&cache, 3).await;
    assert!(cache.is_gap_part(STREAM, 1));
    cache
        .commit_stream_payload(
            STREAM,
            encode_test_canonical_fmp4_object_with_epoch(
                STREAM,
                1,
                0,
                ObjectKind::Media,
                true,
                b"new-epoch",
            ),
        )
        .await
        .unwrap();
    assert!(!cache.is_gap_part(STREAM, 1));
}

#[tokio::test]
async fn with_the_horizon_off_a_gap_hold_skip_lists_no_gap() {
    // The A/B setting: --canonical-repair-horizon off must leave the
    // playlist as before the horizon, where a skipped hole was left out.
    let cache = horizon_cache(RepairHorizon::Off).await;
    cache.set_canonical_gap_hold(Duration::from_millis(20));
    commit_init(&cache).await;
    for sequence in [0, 1, 2, 4] {
        commit(&cache, sequence).await;
    }
    // Past the hold (four 5 ms parts), counted in parts committed.
    for sequence in 5..12 {
        commit(&cache, sequence).await;
    }
    assert!(watermark(&cache).await.unwrap() >= 4, "the hold released");
    assert!(cache.canonical_gap_skip_snapshot().skipped_parts >= 1);
    assert!(!cache.is_gap_part(STREAM, 3));
    let playlist = cache.playlist_for_stream_id(STREAM).await;
    assert!(!playlist.contains("GAP=YES"), "{playlist}");
    assert!(!playlist.contains("part3.mp4"), "{playlist}");
    assert!(playlist.contains("URI=\"part4.mp4\""), "{playlist}");
}

#[tokio::test]
async fn reorderings_are_not_repairs_and_the_auto_horizon_waits_for_a_burst_copy() {
    let cache = horizon_cache(RepairHorizon::Auto).await;
    cache.set_repair_horizon_floor_for_burst_copy(Duration::from_millis(20));
    assert_eq!(cache.repair_horizon_us(), Some(REPAIR_HORIZON_INITIAL_MS * 1_000));
    commit_init(&cache).await;
    // Two parents: each odd part comes a moment before the even one.
    for pair in 0..40 {
        commit(&cache, pair * 2 + 1).await;
        commit(&cache, pair * 2).await;
    }
    let snapshot = cache.canonical_gap_skip_snapshot();
    assert_eq!(snapshot.repair_delays, 0, "reorderings counted as repairs");
    // Fast repairs alone would put the measured horizon at its minimum; the
    // floor keeps it past the burst copy.
    for _ in 0..50 {
        cache.record_repair_delay(500);
    }
    assert_eq!(cache.repair_horizon_us(), Some(30_000));

    // A part that a burst took comes as its copy 20 ms later: no gap.
    for sequence in 81..86 {
        commit(&cache, sequence).await;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(cache.release_expired_repair_holes().await, 0);
    assert_eq!(watermark(&cache).await, Some(79));
    commit(&cache, 80).await;
    assert_eq!(watermark(&cache).await, Some(85));
    assert!(!cache.is_gap_part(STREAM, 80));
    assert_eq!(cache.canonical_gap_skip_snapshot().skipped_parts, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn burst_copies_and_two_parent_reordering_leave_no_gap_with_the_auto_horizon() {
    // A long-haul lane under gemodel 1% 20% with burst copies on: a part
    // that a burst took comes as its copy --relay-burst-copy-ms later, and
    // half the parts come from the other parent a moment before the part
    // ahead of them. With the auto horizon, none of them is a gap.
    let part = Duration::from_millis(2);
    let copy_delay = Duration::from_millis(fast_plane::burst_copy::DEFAULT_BURST_COPY_MS);
    let cache = LiveTsCache::new(1, part, 200, 2_000, 64).await;
    cache.set_canonical_gap_hold(Duration::from_millis(DEFAULT_CANONICAL_GAP_HOLD_MS));
    cache.set_repair_horizon(RepairHorizon::Auto);
    cache.set_repair_horizon_floor_for_burst_copy(copy_delay);
    commit_init(&cache).await;
    let mut path = LongHaul::new(0.01, 0.2, 5);
    let mut copies: VecDeque<(Instant, u64)> = VecDeque::new();
    let started = Instant::now();
    let mut lost = 0;
    let mut swapped = None::<u64>;
    let parts = 600_u64;
    for sequence in 0..parts {
        tokio::time::sleep_until((started + part * sequence as u32).into()).await;
        while copies.front().is_some_and(|(due, _)| *due <= Instant::now()) {
            let (_, copy) = copies.pop_front().unwrap();
            commit(&cache, copy).await;
        }
        if sequence > 0 && path.part_lost(1) {
            lost += 1;
            copies.push_back((Instant::now() + copy_delay, sequence));
            continue;
        }
        // The other parent's copy of an even part comes first: hold it
        // until the odd part after it is stored.
        if sequence % 2 == 0 && sequence + 1 < parts {
            swapped = Some(sequence);
            continue;
        }
        commit(&cache, sequence).await;
        if let Some(held) = swapped.take() {
            commit(&cache, held).await;
        }
        cache.release_expired_repair_holes().await;
    }
    if let Some(held) = swapped.take() {
        commit(&cache, held).await;
    }
    for (due, copy) in copies {
        tokio::time::sleep_until(due.into()).await;
        commit(&cache, copy).await;
    }
    assert!(lost > 5, "the path lost {lost} parts");
    let snapshot = cache.canonical_gap_skip_snapshot();
    assert_eq!(
        snapshot.skipped_parts, 0,
        "parts listed as gaps: {snapshot:?}"
    );
    assert_eq!(snapshot.late_gap_fills, 0);
    // The copies were measured as repairs, the reorderings were not.
    assert!(snapshot.repair_delays <= lost, "{snapshot:?}");
    assert!(
        snapshot.repair_horizon_us.unwrap() >= duration_us(copy_delay),
        "{snapshot:?}"
    );
}

/// A long-haul path with Gilbert-Elliott loss, as netem's `gemodel p r`
/// makes it (every datagram lost in the bad state), deciding which parts
/// a distributor never recovers.
struct LongHaul {
    bad: bool,
    p: f64,
    r: f64,
    state: u64,
}

impl LongHaul {
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

    /// Whether a part of `datagrams` back-to-back datagrams loses all of
    /// them.
    fn part_lost(&mut self, datagrams: usize) -> bool {
        let mut lost = 0;
        for _ in 0..datagrams {
            self.bad = if self.bad {
                self.uniform() >= self.r
            } else {
                self.uniform() < self.p
            };
            lost += usize::from(self.bad);
        }
        lost == datagrams
    }
}

/// Commit `parts` parts of `part` each at their pace, losing the ones the
/// long haul loses (none recovered), and return how long each published
/// part waited for the watermark after it was stored, in microseconds.
async fn watermark_waits(
    horizon: RepairHorizon,
    gap_hold_ms: u64,
    part: Duration,
    parts: u64,
    datagrams: usize,
    path: &mut LongHaul,
) -> (Vec<u64>, u64) {
    let cache = LiveTsCache::new(1, part, 200, 2_000, 64).await;
    cache.set_canonical_gap_hold(Duration::from_millis(gap_hold_ms));
    cache.set_repair_horizon(horizon);
    commit_init(&cache).await;
    let started = Instant::now();
    let mut stored_at: BTreeMap<u64, Instant> = BTreeMap::new();
    let mut published = None::<usize>;
    let mut waits = Vec::new();
    let mut lost = 0;
    for sequence in 0..parts {
        let due = started + part * sequence as u32;
        tokio::time::sleep_until(due.into()).await;
        if sequence > 0 && path.part_lost(datagrams) {
            lost += 1;
        } else {
            commit(&cache, sequence).await;
            stored_at.insert(sequence, Instant::now());
        }
        cache.release_expired_repair_holes().await;
        let now = watermark(&cache).await;
        if now > published {
            let from = published.map_or(0, |last| last + 1) as u64;
            for seq in from..=now.unwrap() as u64 {
                if let Some(at) = stored_at.remove(&seq) {
                    waits.push(at.elapsed().as_micros() as u64);
                }
            }
            published = now;
        }
    }
    (waits, lost)
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() as f64 * p).ceil() as usize).clamp(1, sorted.len()) - 1;
    sorted[index]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn under_burst_loss_parts_wait_at_most_the_horizon() {
    let mut path = LongHaul::new(0.01, 0.2, 7);
    let (mut waits, lost) = watermark_waits(
        RepairHorizon::Fixed(30),
        1_000,
        Duration::from_millis(2),
        600,
        2,
        &mut path,
    )
    .await;
    waits.sort_unstable();
    assert!(lost > 0, "the path lost no part");
    let max = *waits.last().unwrap();
    // The horizon, a part's pace, and a busy test host.
    assert!(max < 30_000 + 25_000, "a part waited {max} us");
}

#[tokio::test]
#[ignore = "a measurement: prints watermark waits under long-haul loss"]
async fn watermark_delay_under_long_haul_loss() {
    let seconds: u64 = std::env::var("GAP_BENCH_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(4);
    for (name, part) in [
        ("PCM 1 ms", Duration::from_millis(1)),
        ("Opus 2.5 ms", Duration::from_micros(2_500)),
    ] {
        let parts = seconds * 1_000_000 / part.as_micros() as u64;
        // Random loss on a lane that carries a part's source alone (its
        // repair on the other lane, lost too); bursts on a lane that
        // carries the source and one repair back to back.
        for (loss, p, r, datagrams) in [
            ("random 1%", 0.01, 1.0, 1),
            ("random 5%", 0.05, 1.0, 1),
            ("gemodel 1% 20%", 0.01, 0.2, 2),
        ] {
            for (label, horizon, hold) in [
                ("hold 1000 ms (before)", RepairHorizon::Off, 1_000),
                ("hold 400 ms (long-haul deploy)", RepairHorizon::Off, 400),
                (
                    "horizon auto (60 ms unmeasured)",
                    RepairHorizon::Auto,
                    1_000,
                ),
                ("horizon 30 ms", RepairHorizon::Fixed(30), 1_000),
            ] {
                let mut path = LongHaul::new(p, r, 11);
                let (mut waits, lost) =
                    watermark_waits(horizon, hold, part, parts, datagrams, &mut path).await;
                waits.sort_unstable();
                let held = waits.iter().filter(|wait| **wait > 1_000).count();
                println!(
                    "{name:12} {loss:15} {label:32} lost {lost:4}/{parts}  wait p50 {:7.2} p99 {:7.2} max {:7.2} ms  parts held >1 ms {:5.1}%",
                    percentile(&waits, 0.5) as f64 / 1_000.0,
                    percentile(&waits, 0.99) as f64 / 1_000.0,
                    percentile(&waits, 1.0) as f64 / 1_000.0,
                    held as f64 * 100.0 / waits.len().max(1) as f64,
                );
            }
        }
    }
}
