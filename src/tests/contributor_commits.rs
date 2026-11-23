//! What a node commits when two contributors send it the same parts
//! (resilient mesh, A AM5).
//!
//! A commit has three outcomes: the part is stored; the cache holds these
//! bytes already (another copy came first) and nothing happens; or the cache
//! holds other bytes under the part's identity, or the part is of an epoch
//! the stream has left, and the commit is refused and counted. No outcome is
//! an error, resets a stream, or logs a line per part: two contributors on
//! two builds (the `two-version` state of a roll) commit every part once.

use super::*;

const STREAM: u64 = 4_001;

/// An opaque audio part of object `version`, as a contributor whose
/// identity gives that version publishes it.
fn opaque_part(sequence: u64, version: u32, payload: &[u8]) -> Bytes {
    let key = media_object::ObjectKey::for_payload(
        "default",
        STREAM.to_string(),
        "opaque-audio",
        0,
        0,
        sequence,
        version,
        payload,
    )
    .unwrap();
    let object = MediaObject::builder(key, ObjectKind::Media, payload.to_vec())
        .with_metadata("container", b"opaque".to_vec())
        .with_metadata("payload-format", b"opaque-part-v1".to_vec())
        .with_metadata("content-type", b"application/octet-stream".to_vec())
        .with_metadata("file-extension", b"bin".to_vec())
        .build()
        .unwrap();
    Bytes::from(media_object::encode(&object).unwrap())
}

async fn journaled_cache() -> Arc<LiveTsCache> {
    let cache = LiveTsCache::new(1, Duration::from_millis(1), 2, 64, 64).await;
    cache.set_hop_journal(Arc::new(HopJournal::new("lon-dist-a", 4_096)));
    cache
}

/// The log lines written while the guard lives (this thread only).
#[derive(Clone, Default)]
struct CapturedLog(Arc<StdMutex<Vec<u8>>>);

impl std::io::Write for CapturedLog {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A second dispatcher that lives as long as the test binary. With one
/// dispatcher registered, tracing works out a callsite's interest from the
/// default of the thread that first reaches it: a callsite that another
/// test's thread reaches first (with no subscriber) would stay off for the
/// subscriber of the test that reads the log. With two or more, every live
/// subscriber is asked.
static SECOND_DISPATCH: std::sync::OnceLock<tracing::Dispatch> = std::sync::OnceLock::new();

impl CapturedLog {
    fn capture(&self) -> tracing::subscriber::DefaultGuard {
        SECOND_DISPATCH
            .get_or_init(|| tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()));
        let writer = self.clone();
        tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::WARN)
                .with_ansi(false)
                .with_writer(move || writer.clone())
                .finish(),
        )
    }

    fn lines_with(&self, needle: &str) -> usize {
        String::from_utf8_lossy(
            &self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
        .lines()
        .filter(|line| line.contains(needle))
        .count()
    }
}

#[tokio::test]
async fn a_conflict_keeps_the_first_copy_and_is_counted() {
    let cache = journaled_cache().await;
    let log = CapturedLog::default();
    let _guard = log.capture();
    let first = opaque_part(0, 1, b"first copy");
    assert_eq!(
        cache.commit_stream_payload(STREAM, first).await.unwrap(),
        Committed::Stored(0)
    );
    let stored = cache.get_part_for_stream_id(STREAM, 0).await.unwrap().0;
    let epoch = |cache: &LiveTsCache| {
        cache
            .state
            .try_read()
            .unwrap()
            .stream_canonical_epoch
            .get(&STREAM)
            .copied()
    };
    let first_epoch = epoch(&cache);
    assert_eq!(first_epoch, Some(0));
    // Ten other copies under the same number: each refused and counted,
    // one log line at 1, 2, 4 and 8.
    for attempt in 0..10_u8 {
        let other = opaque_part(0, 1, &[b'x', attempt]);
        assert_eq!(
            cache.commit_stream_payload(STREAM, other).await.unwrap(),
            Committed::Refused(CommitRefusal::IdentityConflict)
        );
    }
    assert_eq!(
        cache.get_part_for_stream_id(STREAM, 0).await.unwrap().0,
        stored
    );
    assert_eq!(epoch(&cache), first_epoch);
    let snapshot = cache.relay_ingress_snapshot();
    assert_eq!(snapshot.canonical_conflicts, 10);
    assert_eq!(snapshot.canonical_stale_epoch, 0);
    assert_eq!(log.lines_with("canonical commit refused"), 4);
    assert_eq!(hop_events(&cache, HopStage::Committed, STREAM).len(), 1);
    // The stream goes on.
    assert_eq!(
        cache
            .commit_stream_payload(STREAM, opaque_part(1, 1, b"next"))
            .await
            .unwrap(),
        Committed::Stored(1)
    );
}

#[tokio::test]
async fn a_second_commit_of_a_part_has_no_side_effect() {
    let cache = journaled_cache().await;
    let part = opaque_part(0, 1, b"the part");
    let object = media_object::decode(&part).unwrap();
    assert_eq!(
        cache
            .commit_validated_stream_payload(STREAM, part.clone(), object.clone())
            .await
            .unwrap(),
        Committed::Stored(0)
    );
    let record = |cache: &LiveTsCache| {
        let state = cache.state.try_read().unwrap();
        format!("{:?}", state.stream_parts[&STREAM][&0])
    };
    let before = record(&cache);
    let late_fills = cache.late_gap_fills.load(Ordering::Relaxed);
    assert_eq!(
        cache
            .commit_validated_stream_payload(STREAM, part, object)
            .await
            .unwrap(),
        Committed::AlreadyPresent(0)
    );
    assert_eq!(record(&cache), before);
    assert_eq!(cache.late_gap_fills.load(Ordering::Relaxed), late_fills);
    assert_eq!(hop_events(&cache, HopStage::Committed, STREAM).len(), 1);
    let snapshot = cache.relay_ingress_snapshot();
    assert_eq!(snapshot.canonical_conflicts, 0);
    assert_eq!(snapshot.canonical_stale_epoch, 0);
}

/// Two contributors on two builds whose numbering is equal (the
/// `two-version` state of a roll): the object versions differ, so the first
/// forwarder's dedup and its receiver take both copies of each part. The
/// cache commits each number once, from the first copy; the other copy is a
/// counted conflict, journals no decode, and logs a line only at counts 1,
/// 2, 4, 8...
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn different_versions_commit_every_part_once() {
    let relay = start_fast_relay(0, ParentPath::PromotedSecondary, &[], 1_024).await;
    // Fewer than the test cache's ring of 32 parts.
    let parts = 24_u64;
    let deadline_us = now_unix_us() + 10_000_000;
    let symbols = |sequence: u64, version: u32| {
        fast_test_symbols(
            &opaque_part(sequence, version, &[sequence as u8; 120]),
            relay_session::MediaPriority::Audio,
            400,
            48,
            0,
            deadline_us + sequence * 1_000,
        )
        .0
    };
    for sequence in 0..parts {
        for (sender, version) in [(&relay.primary_sender, 1), (&relay.secondary_sender, 2)] {
            let target = if version == 1 {
                relay.primary_audio
            } else {
                relay.secondary_audio
            };
            for symbol in symbols(sequence, version) {
                sender.send_to(&symbol, target).await.unwrap();
            }
        }
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while relay.cache.relay_ingress_snapshot().canonical_conflicts < parts {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("every part's second copy is refused");
    let snapshot = relay.cache.relay_ingress_snapshot();
    assert_eq!(snapshot.decoded_objects, 2 * parts);
    assert_eq!(snapshot.canonical_conflicts, parts);
    assert_eq!(snapshot.datagrams_rejected, 0);
    for sequence in 0..parts {
        let (stored, _) = relay
            .cache
            .chunk_cache
            .get_for_stream_id(STREAM, sequence as usize)
            .await
            .expect("every part is stored");
        assert!(
            [1, 2]
                .iter()
                .any(|version| stored == encode_test_slot(sequence, *version)),
            "part {sequence} holds one of the copies, whole"
        );
    }
    let committed = hop_events(&relay.cache, HopStage::Committed, STREAM);
    assert_eq!(committed.len(), parts as usize);
    let decoded = hop_events(&relay.cache, HopStage::RelayDecoded, STREAM);
    assert_eq!(decoded.len(), parts as usize);
    relay.stop().await;
}

/// The cache slot of `opaque_part(sequence, version, ..)` as the commit
/// writes it.
fn encode_test_slot(sequence: u64, version: u32) -> Bytes {
    let part = opaque_part(sequence, version, &[sequence as u8; 120]);
    let object = media_object::decode(&part).unwrap();
    let kind = av_mesh::canonical_slot::live_media_kind(&object);
    encode_canonical_live_slot(STREAM, &object, part, kind).unwrap()
}

/// A source held back from a warm repair-only child is kept for the media
/// budget from its receipt, whatever its wire deadline says (a point on
/// its stream's timeline, here an hour behind this node's clock).
#[tokio::test]
async fn a_warm_source_ages_on_the_local_clock() {
    let child = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let forwarder = RelayDownstreamForwarder::bind(&[RelayForwardEndpoint {
        bind: "127.0.0.1:0".parse().unwrap(),
        target: child.local_addr().unwrap(),
        role: RelayForwardRole::Repair,
    }])
    .await
    .unwrap()
    .unwrap();
    forwarder.set_warm_hold_ms(200);
    let hour_ago_us = now_unix_us() - 3_600_000_000;
    let symbols = fast_audio_symbols(STREAM, 1, hour_ago_us);
    let key = media_object::decode(&opaque_part(1, 1, &[1; 120]))
        .unwrap()
        .key()
        .clone();
    forwarder
        .forward(&symbols[0], MediaDatagramRole::Source, Some(&key))
        .await;
    let snapshot = forwarder.snapshot();
    assert_eq!(snapshot.warm_source_buffered_datagrams, 1);
    assert_eq!(snapshot.warm_source_expired_datagrams, 0);
    // The hold passes on this node's clock: the next push finds it gone.
    tokio::time::sleep(Duration::from_millis(250)).await;
    forwarder
        .forward(&symbols[0], MediaDatagramRole::Source, Some(&key))
        .await;
    let snapshot = forwarder.snapshot();
    assert_eq!(snapshot.warm_source_expired_datagrams, 1);
    assert_eq!(snapshot.warm_source_buffered_datagrams, 1);
}

/// The log lines of the dispatch seam's limiter for rejections of `kind`:
/// the total each line gives.
fn rejection_totals(log: &CapturedLog, kind: &str) -> Vec<u64> {
    String::from_utf8_lossy(
        &log.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    )
    .lines()
    .filter(|line| line.contains("RelaySession datagram rejected") && line.contains(kind))
    .map(|line| {
        let total = line
            .split(" total=")
            .nth(1)
            .expect("the line gives the total");
        total
            .split(|c: char| !c.is_ascii_digit())
            .next()
            .unwrap()
            .parse()
            .unwrap()
    })
    .collect()
}

/// The symbols of one part of 16 or so symbols, and the same symbols with a
/// deadline 100 ms later (an announcement that conflicts with the first,
/// fresh on any media clock); and the symbols of other bytes under the
/// part's number (an object identity conflict).
fn conflicting_symbols() -> [Vec<Vec<u8>>; 3] {
    let deadline_us = now_unix_us() + 10_000_000;
    let symbols = |payload: &[u8], deadline_us| {
        fast_test_symbols(
            &opaque_part(0, 1, payload),
            relay_session::MediaPriority::Audio,
            400,
            48,
            0,
            deadline_us,
        )
        .0
    };
    let first = symbols(&[3; 6_000], deadline_us);
    assert!(first.len() >= 12);
    [
        symbols(&[3; 6_000], deadline_us + 100_000),
        symbols(&[4; 6_000], deadline_us),
        first,
    ]
}

fn rejected(cache: &LiveTsCache, kind: av_mesh::relay_ingress::RejectionKind) -> u64 {
    cache.relay_ingress_snapshot().rejected_by_kind.get(kind)
}

/// The tee counts every rejection by kind and logs one line a kind at
/// counts 1, 2, 4, 8..., with the total (one 4K edge wrote 17,984 lines,
/// one a datagram).
#[tokio::test]
async fn the_tee_logs_each_rejection_kind_at_counts_one_two_four_eight() {
    use av_mesh::relay_ingress::RejectionKind;
    let log = CapturedLog::default();
    let _guard = log.capture();
    let relay = start_fast_relay(0, ParentPath::PromotedSecondary, &[], 1_024).await;
    let [later, other, first] = conflicting_symbols();
    for symbol in std::iter::once(&first[0]).chain(&later[1..=10]) {
        relay
            .primary_sender
            .send_to(symbol, relay.primary_audio)
            .await
            .unwrap();
    }
    super::fast_lease::wait_until("ten conflicts", || {
        rejected(&relay.cache, RejectionKind::AnnouncementConflict) == 10
    })
    .await;
    relay
        .primary_sender
        .send_to(&other[11], relay.primary_audio)
        .await
        .unwrap();
    super::fast_lease::wait_until("an identity conflict", || {
        rejected(&relay.cache, RejectionKind::ObjectIdentityConflict) == 1
    })
    .await;
    assert_eq!(
        rejection_totals(&log, "announcement_conflict"),
        [1, 2, 4, 8]
    );
    assert_eq!(rejection_totals(&log, "object_identity_conflict"), [1]);
    assert_eq!(relay.cache.relay_ingress_snapshot().datagrams_rejected, 11);
    relay.stop().await;
}

/// The single loop (`--relay-class-threads false`) logs its rejections as
/// the tee does.
#[tokio::test]
async fn the_single_loop_logs_each_rejection_kind_at_counts_one_two_four_eight() {
    use av_mesh::relay_ingress::RejectionKind;
    let log = CapturedLog::default();
    let _guard = log.capture();
    let cache = journaled_cache().await;
    let peer: SocketAddr = "127.0.0.1:41001".parse().unwrap();
    let mut dispatch = fast_test_dispatch(
        peer,
        "127.0.0.1:41002".parse().unwrap(),
        ParentPath::PromotedSecondary,
    );
    let mut receiver = UdpFecReceiver::new();
    let [later, other, first] = conflicting_symbols();
    for symbol in std::iter::once(&first[0])
        .chain(&later[1..=10])
        .chain(std::iter::once(&other[11]))
    {
        process_udp_fec_ingest_datagram(peer, symbol, &cache, &mut receiver, &mut dispatch, None)
            .await
            .unwrap();
    }
    assert_eq!(rejected(&cache, RejectionKind::AnnouncementConflict), 10);
    assert_eq!(rejected(&cache, RejectionKind::ObjectIdentityConflict), 1);
    assert_eq!(
        rejection_totals(&log, "announcement_conflict"),
        [1, 2, 4, 8]
    );
    assert_eq!(rejection_totals(&log, "object_identity_conflict"), [1]);
}

/// The cache mesh can write a part before the fast plane's copy of it
/// decodes (a lease's warm-up window, a slower parent). The tee's commit
/// then finds it present, and the stream is still marked as committing
/// canonical slots: the edge's fast lane serves it even while the mesh wins
/// every part.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_part_the_cache_mesh_wrote_first_still_turns_the_fast_lane_on() {
    let edge = super::fast_lane::LaneEdge::start(super::fast_lease::loopback(11)).await;
    let part = opaque_part(0, 1, &[7; 120]);
    assert_eq!(
        edge.cache
            .commit_stream_payload(STREAM, part.clone())
            .await
            .unwrap(),
        Committed::Stored(0)
    );
    assert!(!edge.lane.is_eligible(STREAM));
    let symbols = fast_test_symbols(
        &part,
        relay_session::MediaPriority::Audio,
        400,
        48,
        0,
        now_unix_us() + 10_000_000,
    );
    edge.send(false, &symbols, false).await;
    super::fast_lease::wait_until("the tee decodes the part", || {
        edge.cache.relay_ingress_snapshot().decoded_objects >= 1
    })
    .await;
    assert!(edge.lane.is_eligible(STREAM));
    assert!(hop_events(&edge.cache, HopStage::RelayDecoded, STREAM).is_empty());
    edge.stop().await;
}
