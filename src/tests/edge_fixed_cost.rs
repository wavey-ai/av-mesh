//! An edge's fixed costs per part, from its hop journal (low-latency wave 5,
//! W1 and W5).
//!
//! On the fast plane an edge journals, for each part, `relay_first` (the
//! kernel's receive time of the part's first symbol, SO_TIMESTAMPNS),
//! `relay_woken` (when its class thread read that symbol off the socket) and
//! `udp_sent` (when the first datagram of the part was handed to the kernel
//! for a UDP client). With the listener's arrival and the contributor's
//! publication time, a part's time at the edge splits into the wire to the
//! edge, the thread's wake-up, the edge's own work, and the wire to the
//! client.
//!
//! `edge_fixed_costs` (ignored, a measurement) prints that split, p50/p99/max,
//! for a fast-plane edge and a mesh-fed edge (cut-through), 2.5 ms Opus and
//! 1 ms PCM. Run it alone: `cargo test --release edge_fixed_costs --
//! --ignored --nocapture --test-threads 1`, with FIXED_COST_SECONDS,
//! FIXED_COST_LISTENERS, FIXED_COST_FEED (fast, mesh or both) and
//! FIXED_COST_PAYLOAD (opus, pcm or both).

use super::compact_lane::{listen, publish_paced, AnyEdge, Feed, Mode, PathLoss, Payload, Shared};
use super::*;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use tokio::time::timeout;

/// The 4K stream beside the audio (FIXED_COST_WITH_4K=1).
const VIDEO_STREAM: u64 = 2;

/// Each part's earliest event of each stage in the edge's journal.
fn first_events(cache: &LiveTsCache, stream_id: u64) -> HashMap<(HopStage, u64), HopEvent> {
    let mut first: HashMap<(HopStage, u64), HopEvent> = HashMap::new();
    let events = cache
        .hop_journal
        .get()
        .expect("the test edge has a journal")
        .events(HopQuery {
            since_us: None,
            stream: Some(stream_id),
        });
    for event in events {
        first
            .entry((event.stage, event.seq))
            .and_modify(|kept| {
                if event.t_us < kept.t_us {
                    *kept = event;
                }
            })
            .or_insert(event);
    }
    first
}

fn count_stage(cache: &LiveTsCache, stream_id: u64, stage: HopStage) -> HashMap<u64, u32> {
    let mut counts = HashMap::new();
    for event in cache
        .hop_journal
        .get()
        .expect("the test edge has a journal")
        .events(HopQuery {
            since_us: None,
            stream: Some(stream_id),
        })
        .into_iter()
        .filter(|event| event.stage == stage)
    {
        *counts.entry(event.seq).or_insert(0) += 1;
    }
    counts
}

/// A stream through an edge to `listeners` of each mode: what they got
/// (part, published, arrival) and the edge.
async fn run_stream(
    feed: Feed,
    payload: Payload,
    modes: &[Mode],
    parts: u64,
    ip: u8,
) -> (AnyEdge, Vec<(u64, u64, u64)>, u64) {
    let stream_id = 4001;
    let (edge, first) = AnyEdge::start(feed, payload, stream_id, ip).await;
    let shared = Shared::new();
    let mut runs = Vec::new();
    for (index, mode) in modes.iter().enumerate() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        runs.push(tokio::spawn(listen(
            edge.client_port(),
            stream_id,
            *mode,
            true,
            PathLoss::Clean,
            index as u64,
            Arc::clone(&shared),
            tx,
        )));
        timeout(Duration::from_secs(5), rx).await.unwrap().unwrap();
    }
    shared.measuring.store(true, Ordering::Relaxed);
    let last = first + parts;
    publish_paced(&edge, payload, stream_id, first, last).await;
    shared.last.store(last - 1, Ordering::Relaxed);
    for run in runs {
        let _ = timeout(Duration::from_secs(10), run).await;
    }
    shared.measuring.store(false, Ordering::Relaxed);
    let arrivals = shared.arrivals.lock().unwrap().clone();
    (edge, arrivals, first)
}

/// A fast-plane edge journals each part it sends to its UDP clients: the
/// kernel's receive time, the class thread's wake-up and the send, in that
/// order, and at most four sends of a part.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fast_plane_edge_journals_receipt_wake_and_udp_sent() {
    let stream_id = 4001;
    let (edge, arrivals, first) = run_stream(
        Feed::Fast,
        Payload::Pcm,
        &[Mode::Compact, Mode::Before],
        200,
        41,
    )
    .await;
    assert!(!arrivals.is_empty(), "the listeners got parts");
    let events = first_events(edge.cache(), stream_id);
    let mut checked = 0;
    for part in first..first + 200 {
        let (Some(kernel), Some(woken), Some(sent)) = (
            events.get(&(HopStage::RelayFirst, part)),
            events.get(&(HopStage::RelayWoken, part)),
            events.get(&(HopStage::UdpSent, part)),
        ) else {
            panic!("part {part}: relay_first, relay_woken and udp_sent journaled");
        };
        assert!(
            kernel.t_us <= woken.t_us && woken.t_us <= sent.t_us,
            "part {part}: kernel {} <= woken {} <= sent {}",
            kernel.t_us,
            woken.t_us,
            sent.t_us
        );
        let client = sent.peer.expect("udp_sent names its client");
        assert!(client.ip().is_loopback());
        checked += 1;
    }
    assert_eq!(checked, 200);
    // Two clients of the part in one sendmmsg: one send, one event.
    let counts = count_stage(edge.cache(), stream_id, HopStage::UdpSent);
    assert!(counts
        .values()
        .all(|count| *count <= crate::hop_journal::UDP_SENT_PER_PART));
    // Every part a listener got was sent after the edge's kernel had it.
    for (part, _, arrived) in &arrivals {
        if let Some(kernel) = events.get(&(HopStage::RelayFirst, *part)) {
            assert!(
                kernel.t_us <= *arrived,
                "part {part} arrived after its receipt"
            );
        }
    }
    edge.stop().await;
}

/// A mesh-fed edge journals `udp_sent` for the parts it cuts through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_fed_edge_journals_udp_sent_for_cut_through_parts() {
    let stream_id = 4001;
    let (edge, arrivals, first) = run_stream(
        Feed::Mesh,
        Payload::Opus,
        &[Mode::Compact, Mode::Before],
        120,
        42,
    )
    .await;
    assert!(!arrivals.is_empty(), "the listeners got parts");
    let events = first_events(edge.cache(), stream_id);
    let sent: Vec<u64> = (first..first + 120)
        .filter(|part| events.contains_key(&(HopStage::UdpSent, *part)))
        .collect();
    assert_eq!(
        sent.len(),
        120,
        "every part sent to the listeners is journaled (missing: {:?})",
        (first..first + 120)
            .filter(|part| !sent.contains(part))
            .collect::<Vec<_>>()
    );
    let counts = count_stage(edge.cache(), stream_id, HopStage::UdpSent);
    assert!(counts
        .values()
        .all(|count| *count <= crate::hop_journal::UDP_SENT_PER_PART));
    edge.stop().await;
}

// ------------------------------------------------------------------ the measurement

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// p50 / p99 / max of µs values, in ms.
fn spread(values: &mut [i64]) -> String {
    if values.is_empty() {
        return "-".into();
    }
    values.sort_unstable();
    let at = |q: f64| values[((values.len() - 1) as f64 * q) as usize] as f64 / 1_000.0;
    format!(
        "{:.3}/{:.3}/{:.3}",
        at(0.5),
        at(0.99),
        *values.last().unwrap() as f64 / 1_000.0
    )
}

fn measure(feed: Feed, payload: Payload, listeners: usize, seconds: u64) -> String {
    let runtime = |name: &str, workers: usize| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .thread_name(name)
            .enable_all()
            .build()
            .unwrap()
    };
    // The edge, its listeners and the publisher on runtimes of their own, as
    // three hosts would be.
    let edge_rt = runtime("edge-rt", 2);
    let edge_handle = edge_rt.handle().clone();
    let client_rt = runtime("client-rt", 2);
    let main_rt = runtime("pub-rt", 2);
    let stream_id = 4001;
    // The listeners' lane: the compact lane (the client's default) or the
    // RaptorQ lane (FIXED_COST_MODE=raptorq).
    let mode = match std::env::var("FIXED_COST_MODE").as_deref() {
        Ok("raptorq") => Mode::Before,
        _ => Mode::Compact,
    };
    let (edge, first) = edge_rt
        .block_on(async { tokio::spawn(AnyEdge::start(feed, payload, stream_id, 43)).await })
        .unwrap();
    // FIXED_COST_FAST_PARSE=false: the compact datagram from the full
    // decode, as before --client-udp-compact-fast-parse.
    let fast_parse = std::env::var("FIXED_COST_FAST_PARSE").as_deref() != Ok("false");
    if let AnyEdge::Fast(lane_edge) = &edge {
        lane_edge.lane.set_compact_fast_parse(fast_parse);
    }
    let edge = Arc::new(edge);
    let shared = Shared::new();
    let client_handle = client_rt.handle().clone();
    let last = main_rt.block_on(async {
        let mut runs = Vec::new();
        for index in 0..listeners {
            let (tx, rx) = tokio::sync::oneshot::channel();
            runs.push(client_handle.spawn(listen(
                edge.client_port(),
                stream_id,
                mode,
                true,
                PathLoss::Clean,
                index as u64,
                Arc::clone(&shared),
                tx,
            )));
            timeout(Duration::from_secs(5), rx).await.unwrap().unwrap();
        }
        // FIXED_COST_WITH_4K=1: a 4K stream (25 Mbit/s, one frame a part)
        // through the same edge to a listener of its own, beside the audio.
        let video = (std::env::var("FIXED_COST_WITH_4K").as_deref() == Ok("1")
            && feed == Feed::Fast)
            .then_some(Payload::Video4k);
        let mut video_run = None;
        if let Some(video) = video {
            let video_edge = Arc::clone(&edge);
            edge_handle
                .spawn(async move {
                    video_edge.publish(video, VIDEO_STREAM, 0).await;
                    wait_for_part(video_edge.cache(), VIDEO_STREAM, 0).await;
                })
                .await
                .unwrap();
            let (tx, rx) = tokio::sync::oneshot::channel();
            let video_shared = Shared::new();
            video_run = Some((
                client_handle.spawn(listen(
                    edge.client_port(),
                    VIDEO_STREAM,
                    Mode::Compact,
                    true,
                    PathLoss::Clean,
                    99,
                    Arc::clone(&video_shared),
                    tx,
                )),
                video_shared,
            ));
            timeout(Duration::from_secs(5), rx).await.unwrap().unwrap();
        }
        let total = first + seconds * 1_000_000 / payload.part_us();
        let video_publisher = video.map(|video| {
            let edge = Arc::clone(&edge);
            let frames = 1 + seconds * 1_000_000 / video.part_us();
            tokio::spawn(async move { publish_paced(&edge, video, VIDEO_STREAM, 1, frames).await })
        });
        // A second to settle, then measure.
        let settle = first + 1_000_000 / payload.part_us();
        let publisher = {
            let edge = Arc::clone(&edge);
            tokio::spawn(
                async move { publish_paced(&edge, payload, stream_id, first, total).await },
            )
        };
        let shared_measuring = Arc::clone(&shared);
        let watch_edge = Arc::clone(&edge);
        tokio::spawn(async move {
            while watch_edge
                .cache()
                .stream_position_for_id(stream_id)
                .await
                .is_none_or(|(_, last)| (last as u64) < settle)
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            shared_measuring.measuring.store(true, Ordering::Relaxed);
        });
        publisher.await.unwrap();
        shared.last.store(total - 1, Ordering::Relaxed);
        for run in runs {
            let _ = timeout(Duration::from_secs(10), run).await;
        }
        if let Some(publisher) = video_publisher {
            publisher.await.unwrap();
        }
        if let Some((run, video_shared)) = video_run {
            video_shared.last.store(0, Ordering::Relaxed);
            let _ = timeout(Duration::from_secs(10), run).await;
        }
        total
    });
    let arrivals = shared.arrivals.lock().unwrap().clone();
    let events = first_events(edge.cache(), stream_id);
    let at = |stage: HopStage, part: u64| events.get(&(stage, part)).map(|event| event.t_us as i64);
    let (mut wire_in, mut wake, mut work, mut wire_out, mut total, mut edge_total) =
        (vec![], vec![], vec![], vec![], vec![], vec![]);
    // The LL-HLS side: the tee's commit of the part after its receipt.
    let mut tee = vec![];
    let mut slow_commits: Vec<(i64, u64, i64)> = vec![];
    for (part, published, arrived) in &arrivals {
        let (published, arrived) = (*published as i64, *arrived as i64);
        total.push(arrived - published);
        let receipt = match feed {
            Feed::Fast => at(HopStage::RelayFirst, *part),
            Feed::Mesh => at(HopStage::MeshFirst, *part),
        };
        let sent = at(HopStage::UdpSent, *part);
        if let Some(receipt) = receipt {
            wire_in.push(receipt - published);
        }
        if let (Some(receipt), Some(sent)) = (receipt, sent) {
            edge_total.push(sent - receipt);
        }
        if let (Some(kernel), Some(woken)) = (receipt, at(HopStage::RelayWoken, *part)) {
            wake.push(woken - kernel);
            if let Some(sent) = sent {
                work.push(sent - woken);
            }
        }
        if let Some(sent) = sent {
            wire_out.push(arrived - sent);
        }
        if let (Some(receipt), Some(committed)) = (receipt, at(HopStage::Committed, *part)) {
            tee.push(committed - receipt);
            slow_commits.push((committed - receipt, *part, receipt));
        }
    }
    // FIXED_COST_DUMP=1: the slowest commits, to find what holds the tee.
    if std::env::var("FIXED_COST_DUMP").as_deref() == Ok("1") {
        slow_commits.sort_unstable_by(|a, b| b.cmp(a));
        for (delay, part, receipt) in slow_commits.iter().take(12) {
            println!(
                "slow commit: part {part} +{:.3} ms at {receipt}",
                *delay as f64 / 1_000.0
            );
        }
    }
    let line = format!(
        "{feed:?} {:<30} {listeners} {mode:?} listeners (fast parse {fast_parse}), {} parts (last {last}): p50/p99/max ms \
         publish->edge receipt {} | wake (relay_woken - relay_first) {} | edge work \
         (udp_sent - relay_woken) {} | edge receipt->udp_sent {} | udp_sent->arrival {} | \
         publish->arrival {} | receipt->committed (LL-HLS) {}{}",
        payload.name(),
        arrivals.len(),
        spread(&mut wire_in),
        spread(&mut wake),
        spread(&mut work),
        spread(&mut edge_total),
        spread(&mut wire_out),
        spread(&mut total),
        spread(&mut tee),
        if std::env::var("FIXED_COST_WITH_4K").as_deref() == Ok("1") && feed == Feed::Fast {
            " | with 4K 25 Mbit/s on the edge"
        } else {
            ""
        },
    );
    let edge = Arc::try_unwrap(edge)
        .ok()
        .expect("the edge is no longer shared");
    edge_rt.block_on(edge.stop());
    line
}

#[test]
#[ignore = "a measurement: run it alone, in release"]
fn edge_fixed_costs() {
    let seconds = env_u64("FIXED_COST_SECONDS", 6);
    let listeners = env_u64("FIXED_COST_LISTENERS", 1) as usize;
    let feeds = match std::env::var("FIXED_COST_FEED").as_deref() {
        Ok("mesh") => vec![Feed::Mesh],
        Ok("both") => vec![Feed::Fast, Feed::Mesh],
        _ => vec![Feed::Fast],
    };
    let payloads = match std::env::var("FIXED_COST_PAYLOAD").as_deref() {
        Ok("opus") => vec![Payload::Opus],
        Ok("pcm") => vec![Payload::Pcm],
        _ => vec![Payload::Opus, Payload::Pcm],
    };
    let mut lines = Vec::new();
    for feed in &feeds {
        for payload in &payloads {
            let line = measure(*feed, *payload, listeners, seconds);
            println!("{line}");
            lines.push(line);
        }
    }
    println!("\n{}", lines.join("\n"));
}

#[test]
fn the_compact_fast_parse_flag_defaults_on_and_can_be_turned_off() {
    let args = Args::try_parse_from(["av-mesh"]).unwrap();
    assert!(args.client_udp_compact_fast_parse);
    let args =
        Args::try_parse_from(["av-mesh", "--client-udp-compact-fast-parse", "false"]).unwrap();
    assert!(!args.client_udp_compact_fast_parse);
}

/// The class thread's cost of a part's compact datagram: the envelope read
/// in place against the full decode and slot (an ignored measurement).
#[test]
#[ignore = "a measurement: run it alone, in release"]
fn compact_datagram_cost_per_part() {
    use av_mesh::canonical_slot::{
        canonical_part_from_envelope, envelope_part, object_published_unix_us, slot_media,
    };
    for payload in [Payload::Opus, Payload::Pcm] {
        let envelopes: Vec<Bytes> = (0..1_000)
            .map(|sequence| payload.envelope(4001, sequence).0)
            .collect();
        let rounds = 20;
        let started = Instant::now();
        let mut bytes = 0_usize;
        for _ in 0..rounds {
            for envelope in &envelopes {
                let part = canonical_part_from_envelope(Bytes::copy_from_slice(envelope)).unwrap();
                let (media, _) = slot_media(&part.slot, part.stream_id).unwrap();
                let published = object_published_unix_us(&part.object).unwrap_or(0);
                let datagram = av_mesh::client_compact::encode_part(
                    part.object.key().object(),
                    published,
                    &media,
                )
                .unwrap();
                bytes += Bytes::from(datagram).len();
            }
        }
        let decode_ns = started.elapsed().as_nanos() as f64 / (rounds * envelopes.len()) as f64;
        let started = Instant::now();
        for _ in 0..rounds {
            for envelope in &envelopes {
                let part = envelope_part(envelope).unwrap();
                let datagram = av_mesh::client_compact::encode_part(
                    part.object,
                    part.published_unix_us,
                    part.media,
                )
                .unwrap();
                bytes += Bytes::from(datagram).len();
            }
        }
        let in_place_ns = started.elapsed().as_nanos() as f64 / (rounds * envelopes.len()) as f64;
        println!(
            "{}: envelope {} B, compact datagram from the decode {:.2} µs, read in place {:.2} µs ({bytes} B)",
            payload.name(),
            envelopes[0].len(),
            decode_ns / 1_000.0,
            in_place_ns / 1_000.0
        );
    }
}

#[test]
fn busy_polling_is_off_by_default_and_takes_microseconds() {
    let args = Args::try_parse_from(["av-mesh"]).unwrap();
    assert_eq!(args.fast_busy_poll_us, 0);
    let args = Args::try_parse_from(["av-mesh", "--fast-busy-poll-us", "50"]).unwrap();
    assert_eq!(args.fast_busy_poll_us, 50);
}
