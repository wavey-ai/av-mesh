//! Copies sized to loss (wave 7 item 2), the forwarder's half: a repair
//! lane carries the one symbol of a one-symbol audio part, and the audio
//! thread reports the loss of each parent lane (RLR1) to the parent.

use super::*;
use crate::fast_plane::loss_report::decode_loss_report;
use crate::fast_plane::view::RlsView;
use relay_session::{
    encode_datagram, AdaptiveFecController, AdaptiveFecPolicy, CongestionConfig, MediaDeadline,
    RaptorQObjectEncoder, RelayLimits,
};

/// The RLS1 symbols of `count` audio parts of `payload` bytes from one
/// encoder (so that their packet sequences follow each other), each split
/// into symbols of `symbol_size` with `repair` repair symbols.
fn encoded_parts(
    stream_id: u64,
    count: u64,
    payload: usize,
    symbol_size: u16,
    repair: u32,
) -> Vec<(Vec<Vec<u8>>, Vec<Vec<u8>>)> {
    let policy = AdaptiveFecPolicy {
        min_repair_symbols: repair,
        max_repair_symbols: repair,
        min_repair_ratio: 0.0,
        max_repair_ratio: 0.0,
        max_source_symbols: 64,
        delta_repair_floor_symbols: 0,
        loss_repair_safety_symbols: 0,
        symbol_size,
        ..AdaptiveFecPolicy::default()
    };
    let mut encoder = RaptorQObjectEncoder::new(
        AdaptiveFecController::new(policy, CongestionConfig::default()),
        RelayLimits::default(),
    )
    .unwrap();
    let deadline = MediaDeadline::from_micros(now_unix_us() + 30_000_000);
    (0..count)
        .map(|sequence| {
            let envelope =
                encode_test_canonical_opaque_object(stream_id, sequence, &vec![sequence as u8; payload]);
            let object = media_object::decode(&envelope).unwrap();
            let encoded = encoder
                .encode_object(
                    &object,
                    TopologyGeneration::new(FAST_GENERATION).unwrap(),
                    SubscriptionId::new(FAST_SUBSCRIPTION).unwrap(),
                    deadline,
                    relay_session::MediaPriority::Audio,
                )
                .unwrap();
            let wire = |symbols: &[relay_session::RelayDatagram]| {
                symbols
                    .iter()
                    .map(|symbol| encode_datagram(symbol, RelayLimits::default()).unwrap())
                    .collect::<Vec<_>>()
            };
            (wire(&encoded.source_symbols), wire(&encoded.repair_symbols))
        })
        .collect()
}

/// Every datagram that arrives at `socket` until it is quiet for 300 ms:
/// (object, source?) of each.
async fn collect(socket: &UdpSocket) -> Vec<(u64, bool)> {
    let mut out = Vec::new();
    let mut buf = [0_u8; 4_096];
    while let Ok(Ok((len, _))) =
        tokio::time::timeout(Duration::from_millis(300), socket.recv_from(&mut buf)).await
    {
        let view = RlsView::parse(&buf[..len], RelayLimits::default()).unwrap();
        out.push((
            view.object,
            matches!(view.role, raptorq_datagram_fec::MediaDatagramRole::Source),
        ));
    }
    out
}

async fn run_lanes(single_symbol: bool, payload: usize) -> (Vec<(u64, bool)>, Vec<(u64, bool)>, usize) {
    let (source_child, _) = bind_class_pair(1_000).await;
    let (repair_child, _) = bind_class_pair(1_000).await;
    let relay = start_fast_relay_with(
        ParentPath::Secondary,
        &[
            (source_child.local_addr().unwrap(), RelayForwardRole::Source),
            (repair_child.local_addr().unwrap(), RelayForwardRole::Repair),
        ],
        fast_plane::relay::RelayClassConfig {
            port_offset: 1_000,
            cpus: [None, None],
            realtime: false,
            tee_capacity: 8_192,
            burst_copy_ms: 5,
            ..Default::default()
        },
    )
    .await;
    relay
        .forwarder
        .as_ref()
        .unwrap()
        .set_repair_lane_single_symbol(single_symbol);
    // No repair: the contributor's clean-path part.
    let parts = encoded_parts(4001, 20, payload, 1_200, 0);
    let sources = parts.iter().map(|(sources, _)| sources.len()).max().unwrap();
    for (sources, _) in &parts {
        for symbol in sources {
            relay
                .primary_sender
                .send_to(symbol, relay.primary_audio)
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let (at_source, at_repair) = tokio::join!(collect(&source_child), collect(&repair_child));
    relay.stop().await;
    (at_source, at_repair, sources)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_repair_lane_carries_the_one_symbol_of_a_one_symbol_audio_part() {
    let (at_source, at_repair, symbols) = run_lanes(true, 300).await;
    assert_eq!(symbols, 1, "a 1 ms part is one symbol");
    // The source lane: every source and its burst copy.
    assert_eq!(at_source.len(), 40, "{at_source:?}");
    // The repair lane: every part's one symbol, once (no burst copy).
    let mut objects: Vec<u64> = at_repair.iter().map(|(object, _)| *object).collect();
    objects.sort_unstable();
    assert_eq!(objects, (0..20).collect::<Vec<_>>(), "{at_repair:?}");
    assert!(at_repair.iter().all(|(_, source)| *source));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_the_flag_a_repair_lane_carries_no_source() {
    let (at_source, at_repair, _) = run_lanes(false, 300).await;
    assert_eq!(at_source.len(), 40);
    assert!(at_repair.is_empty(), "{at_repair:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_part_of_several_symbols_keeps_its_sources_off_the_repair_lane() {
    let (at_source, at_repair, symbols) = run_lanes(true, 3_000).await;
    assert!(symbols > 1, "{symbols}");
    assert!(!at_source.is_empty());
    assert!(at_repair.is_empty(), "{at_repair:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_audio_thread_reports_the_loss_of_its_parent_lane() {
    let (child, _child_video) = bind_class_pair(1_000).await;
    let relay = start_fast_relay_with(
        ParentPath::Secondary,
        &[(child.local_addr().unwrap(), RelayForwardRole::All)],
        fast_plane::relay::RelayClassConfig {
            port_offset: 1_000,
            cpus: [None, None],
            realtime: false,
            tee_capacity: 8_192,
            loss_report: true,
            ..Default::default()
        },
    )
    .await;
    // 2,000 parts at 1 ms, every 20th lost on the way (5 %).
    let parts = encoded_parts(4001, 2_000, 300, 1_200, 0);
    let started = Instant::now();
    for (index, (sources, _)) in parts.iter().enumerate() {
        if index % 20 == 7 {
            continue;
        }
        tokio::time::sleep_until((started + Duration::from_millis(index as u64)).into()).await;
        relay
            .primary_sender
            .send_to(&sources[0], relay.primary_audio)
            .await
            .unwrap();
    }
    // The parent's lane socket hears the reports.
    let mut reports = Vec::new();
    let mut buf = [0_u8; 64];
    while let Ok(Ok((len, from))) = tokio::time::timeout(
        Duration::from_millis(700),
        relay.primary_sender.recv_from(&mut buf),
    )
    .await
    {
        assert_eq!(from, relay.primary_audio, "from the socket the symbols came to");
        reports.push(decode_loss_report(&buf[..len]).expect("an RLR1 report"));
    }
    let snapshot = relay.stats();
    relay.stop().await;
    let (last, _) = *reports.last().expect("a lossy lane is reported");
    assert!((35_000..=65_000).contains(&last), "{reports:?}");
    assert!(snapshot.loss_reports >= reports.len() as u64);
    assert!(snapshot.parent_loss_ppm > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clean_lane_sends_no_report() {
    let (child, _) = bind_class_pair(1_000).await;
    let relay = start_fast_relay_with(
        ParentPath::Secondary,
        &[(child.local_addr().unwrap(), RelayForwardRole::All)],
        fast_plane::relay::RelayClassConfig {
            port_offset: 1_000,
            cpus: [None, None],
            realtime: false,
            tee_capacity: 8_192,
            loss_report: true,
            ..Default::default()
        },
    )
    .await;
    let parts = encoded_parts(4001, 1_200, 300, 1_200, 0);
    let started = Instant::now();
    for (index, (sources, _)) in parts.iter().enumerate() {
        tokio::time::sleep_until((started + Duration::from_millis(index as u64)).into()).await;
        relay
            .primary_sender
            .send_to(&sources[0], relay.primary_audio)
            .await
            .unwrap();
    }
    let mut buf = [0_u8; 64];
    let heard =
        tokio::time::timeout(Duration::from_millis(700), relay.primary_sender.recv_from(&mut buf))
            .await;
    let snapshot = relay.stats();
    relay.stop().await;
    assert!(heard.is_err(), "a clean lane is not reported");
    assert_eq!(snapshot.loss_reports, 0);
    assert_eq!(snapshot.parent_loss_ppm, 0);
}

