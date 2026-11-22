//! Copies sized to loss (wave 7 item 2), the forwarder's half: a repair
//! lane carries the one symbol of a one-symbol audio part, and the audio
//! thread reports the loss of each parent lane (RLR1) to the parent,
//! measured against the parent's ledgers (RLL1).

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
    run_lanes_with(single_symbol, true, payload).await
}

async fn run_lanes_with(
    single_symbol: bool,
    repair_lane_copy: bool,
    payload: usize,
) -> (Vec<(u64, bool)>, Vec<(u64, bool)>, usize) {
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
    relay
        .forwarder
        .as_ref()
        .unwrap()
        .set_repair_lane_burst_copy(repair_lane_copy);
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
    // The repair lane: every part's one symbol and its burst copy (wave 7
    // review: four copies across the two forwarders, as before item 2).
    let mut objects: Vec<u64> = at_repair.iter().map(|(object, _)| *object).collect();
    objects.sort_unstable();
    let twice: Vec<u64> = (0..20).flat_map(|object| [object, object]).collect();
    assert_eq!(objects, twice, "{at_repair:?}");
    assert!(at_repair.iter().all(|(_, source)| *source));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_its_burst_copy_a_repair_lane_carries_the_one_symbol_once() {
    let (at_source, at_repair, _) = run_lanes_with(true, false, 300).await;
    assert_eq!(at_source.len(), 40, "{at_source:?}");
    let mut objects: Vec<u64> = at_repair.iter().map(|(object, _)| *object).collect();
    objects.sort_unstable();
    assert_eq!(objects, (0..20).collect::<Vec<_>>(), "{at_repair:?}");
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

/// Send the primary lane `parts` (sources, and the repair too with
/// `with_repair`), one part a millisecond, skipping the datagrams that
/// `lost` names, with a ledger (RLL1) of what was sent every 250 sent; then
/// collect the reports that the lane's socket hears.
async fn lane_with_ledgers(
    with_repair: bool,
    lost: impl Fn(u64) -> bool,
) -> (Vec<(u32, u32)>, fast_plane::relay::FastPlaneSnapshot) {
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
    // 2,000 parts at 1 ms, each with one repair symbol from one encoder: a
    // lane without the repair sees every other packet sequence go missing.
    let parts = encoded_parts(4001, 2_000, 300, 1_200, 1);
    let started = Instant::now();
    let mut sent = 0_u64;
    let mut index = 0_u64;
    for (part, (sources, repair)) in parts.iter().enumerate() {
        tokio::time::sleep_until((started + Duration::from_millis(part as u64)).into()).await;
        let datagrams = if with_repair {
            vec![&sources[0], &repair[0]]
        } else {
            vec![&sources[0]]
        };
        for datagram in datagrams {
            if !lost(index) {
                relay
                    .primary_sender
                    .send_to(datagram, relay.primary_audio)
                    .await
                    .unwrap();
            }
            index += 1;
            sent += 1;
            if sent % 250 == 0 {
                relay
                    .primary_sender
                    .send_to(
                        &fast_plane::loss_report::encode_ledger(sent),
                        relay.primary_audio,
                    )
                    .await
                    .unwrap();
            }
        }
    }
    // The parent's lane socket hears the reports.
    let mut reports = Vec::new();
    let mut buf = [0_u8; 64];
    while let Ok(Ok((len, from))) = tokio::time::timeout(
        Duration::from_millis(1_200),
        relay.primary_sender.recv_from(&mut buf),
    )
    .await
    {
        assert_eq!(from, relay.primary_audio, "from the socket the symbols came to");
        reports.push(decode_loss_report(&buf[..len]).expect("an RLR1 report"));
        if reports.len() > 64 {
            break;
        }
    }
    let snapshot = relay.stats();
    relay.stop().await;
    (reports, snapshot)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_audio_thread_reports_the_loss_of_its_parent_lane() {
    // Every 20th datagram lost on the way (5 %).
    let (reports, snapshot) = lane_with_ledgers(true, |index| index % 20 == 7).await;
    let (last, _) = *reports.last().expect("a lossy lane is reported");
    assert!((35_000..=65_000).contains(&last), "{reports:?}");
    assert!(snapshot.loss_reports >= reports.len() as u64);
    assert!(snapshot.parent_loss_ppm > 0);
}

/// The wave 7 review's latch: a lane that carries the sources only, beside
/// a secondary that carries the repair, saw half of the encoder's packet
/// sequences go missing and reported 50 % loss. Against the ledger it
/// reports none, and a clean lane is reported as clean (zero, once a
/// second), so the contributor can tell it from a forwarder that says
/// nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lane_of_sources_only_reports_no_loss() {
    let (reports, snapshot) = lane_with_ledgers(false, |_| false).await;
    assert!(!reports.is_empty(), "a clean lane is reported as clean");
    assert!(reports.iter().all(|&(loss, _)| loss == 0), "{reports:?}");
    assert_eq!(snapshot.parent_loss_ppm, 0);
}

/// A lane whose parent sends no ledger (every lane below the first
/// forwarders) is not measured and not reported.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lane_without_ledgers_sends_no_report() {
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
        // Lossy, too: without a ledger there is nothing to measure.
        if index % 10 == 3 {
            continue;
        }
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
    assert!(heard.is_err(), "nothing is reported without a ledger");
    assert_eq!(snapshot.loss_reports, 0);
    assert_eq!(snapshot.parent_loss_ppm, 0);
}

/// Wave 7 item 5: a compiled child that reports reading Opus only gets no
/// PCM; a child without a report gets everything; a stream of no group goes
/// to both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_compiled_lane_carries_only_the_renditions_its_child_reads() {
    let (reader, _reader_video) = bind_class_pair(1_000).await;
    let (silent, _silent_video) = bind_class_pair(1_000).await;
    let relay = start_fast_relay_with(
        ParentPath::Secondary,
        &[
            (reader.local_addr().unwrap(), RelayForwardRole::All),
            (silent.local_addr().unwrap(), RelayForwardRole::All),
        ],
        fast_plane::relay::RelayClassConfig {
            port_offset: 1_000,
            cpus: [None, None],
            realtime: false,
            tee_capacity: 8_192,
            ..Default::default()
        },
    )
    .await;
    let forwarder = relay.forwarder.clone().unwrap();
    forwarder.set_demand_groups("2001,3001,4001".parse().unwrap());
    let readers = forwarder.spawn_demand_readers();
    // The reader's child reports Opus, to the socket its symbols come from.
    let source = forwarder.paths[0].socket.local_addr().unwrap();
    reader
        .send_to(&fast_plane::demand::encode_demand(&[3001]), source)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    for stream in [3001_u64, 4001, 7] {
        for (sources, _) in encoded_parts(stream, 5, 300, 1_200, 0) {
            relay
                .primary_sender
                .send_to(&sources[0], relay.primary_audio)
                .await
                .unwrap();
        }
    }
    let streams = |datagram: &[u8]| {
        RlsView::parse(datagram, RelayLimits::default())
            .unwrap()
            .stream_id()
            .unwrap()
    };
    let mut got = [Vec::new(), Vec::new()];
    for (index, socket) in [&reader, &silent].into_iter().enumerate() {
        let mut buf = [0_u8; 4_096];
        while let Ok(Ok((len, _))) =
            tokio::time::timeout(Duration::from_millis(300), socket.recv_from(&mut buf)).await
        {
            got[index].push(streams(&buf[..len]));
        }
    }
    for task in readers {
        task.abort();
    }
    relay.stop().await;
    let count = |list: &Vec<u64>, stream: u64| list.iter().filter(|s| **s == stream).count();
    assert_eq!(count(&got[0], 3001), 5, "{:?}", got[0]);
    assert_eq!(count(&got[0], 4001), 0, "{:?}", got[0]);
    assert_eq!(count(&got[0], 7), 5, "{:?}", got[0]);
    assert_eq!(count(&got[1], 4001), 5, "no report: every rendition {:?}", got[1]);
}

/// The senders of the demand reports that arrive at `socket` within a
/// report interval or two (each report must say Opus).
async fn demand_heard(socket: &UdpSocket) -> Vec<SocketAddr> {
    let mut from = Vec::new();
    let mut buf = [0_u8; 256];
    while let Ok(Ok((len, sender))) =
        tokio::time::timeout(Duration::from_millis(600), socket.recv_from(&mut buf)).await
    {
        assert_eq!(fast_plane::demand::decode_demand(&buf[..len]), Some(vec![3001]));
        if !from.contains(&sender) {
            from.push(sender);
        }
        if from.len() == 2 {
            break;
        }
    }
    from
}

/// A node tells its relay parents its demand every 250 ms, from each of
/// its audio sockets (the parent takes the one from the address its
/// symbols go to).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_reports_its_demand_to_its_parents() {
    let (child, _child_video) = bind_class_pair(1_000).await;
    let demand = Arc::new(fast_plane::demand::NodeDemand::default());
    demand.set(vec![3001]);
    let relay = start_fast_relay_with(
        ParentPath::Secondary,
        &[(child.local_addr().unwrap(), RelayForwardRole::All)],
        fast_plane::relay::RelayClassConfig {
            port_offset: 1_000,
            cpus: [None, None],
            realtime: false,
            tee_capacity: 8_192,
            demand: Some(Arc::clone(&demand)),
            ..Default::default()
        },
    )
    .await;
    // Each parent hears the demand from both audio sockets, within a
    // report interval or two.
    let primary = demand_heard(&relay.primary_sender).await;
    let secondary = demand_heard(&relay.secondary_sender).await;
    for from in [&primary, &secondary] {
        assert!(from.contains(&relay.primary_audio), "{from:?}");
        assert!(from.contains(&relay.secondary_audio), "{from:?}");
    }
    assert!(relay.stats().demand_reports >= 4);
    relay.stop().await;
}

#[test]
fn the_repair_lane_flags_default_on_and_turn_off() {
    let args = Args::try_parse_from(["av-mesh"]).unwrap();
    assert!(args.relay_repair_lane_single_symbol);
    assert!(args.relay_repair_lane_burst_copy);
    assert!(args.relay_loss_report);
    let args = Args::try_parse_from([
        "av-mesh",
        "--relay-repair-lane-burst-copy",
        "false",
    ])
    .unwrap();
    assert!(!args.relay_repair_lane_burst_copy);
}

/// Wave 7 review: a forwarder passes its children's demand up only while
/// every child has reported: a child that has not (an older node, a lost
/// report) makes the forwarder's demand unknown, so its parents fail open
/// instead of holding back what that child reads. A forwarder reads the
/// reports without rendition groups of its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forwarder_passes_its_childrens_demand_only_when_each_has_reported() {
    let (reader, _reader_video) = bind_class_pair(1_000).await;
    let (silent, _silent_video) = bind_class_pair(1_000).await;
    let relay = start_fast_relay_with(
        ParentPath::Secondary,
        &[
            (reader.local_addr().unwrap(), RelayForwardRole::All),
            (silent.local_addr().unwrap(), RelayForwardRole::All),
        ],
        fast_plane::relay::RelayClassConfig {
            port_offset: 1_000,
            cpus: [None, None],
            realtime: false,
            tee_capacity: 8_192,
            ..Default::default()
        },
    )
    .await;
    let forwarder = relay.forwarder.clone().unwrap();
    assert!(forwarder.demand_groups().is_none(), "no groups here");
    let readers = forwarder.spawn_demand_readers();
    let source = forwarder.paths[0].socket.local_addr().unwrap();
    reader
        .send_to(&fast_plane::demand::encode_demand(&[3001]), source)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(forwarder.child_demand(Instant::now()), None);
    silent
        .send_to(&fast_plane::demand::encode_demand(&[4001]), source)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut demand = forwarder.child_demand(Instant::now()).unwrap();
    demand.sort_unstable();
    assert_eq!(demand, vec![3001, 4001]);
    for reader in readers {
        reader.abort();
    }
    relay.stop().await;
}
