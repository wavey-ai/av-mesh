//! A send stall at an edge and at a forwarder (send240, A), in process.
//!
//! cmpd8 (25 Nov): an edge's public NIC took no datagram for 1 to 3 s; the
//! fast lane gave each part it could not finish to the cache-fed lane,
//! which sent whole parts again, and the stall became 3 to 5.7 s holds and
//! missing parts. Here an edge is fed 4K (24 frames a second of 1,088-byte
//! symbols, 200 Mbit/s of media: about 247 Mbit/s on the wire) and a 1 ms
//! PCM stream, with two 4K UDP clients and one PCM client on the fast lane.
//! The stall is the test's (`send_defer::stall`): the edge's sockets refuse
//! a client's datagrams (or every client's) as a full socket does, for both
//! the fast lane and the cache-fed lane, as a host's NIC would.
//!
//! Each client records when each part became whole. A client's hold is the
//! longest time between two parts becoming whole; a part is late by the
//! time from the source's last datagram of it to its completion.
//!
//! `a_short_stall_keeps_every_part_on_the_fast_lane` and
//! `a_stall_past_the_bound_hands_the_missing_symbols_on` run in the suite
//! (40 Mbit/s, counters and wholeness); `send_stall_measurement` is the
//! measurement (ignored: run it alone, in release,
//! `cargo test --release send_stall_measurement -- --ignored --nocapture
//! --test-threads 1`; ST_MBPS, ST_SECONDS, ST_STALLS ("100,1000,3000"),
//! ST_SCOPE ("all" or "one")).

use super::fast_lane::{packaged_envelope, LaneEdge, Packaging};
use super::fast_lease::loopback;
use super::video_throughput::{make_frames_shaped, send_frames_at, Frame, FrameShape, Spread};
use super::*;
use av_mesh::client_udp::{Control, ObjectKind as UdpObjectKind, ObjectReceiver, RepairPolicy};
use fast_plane::send_defer::{stall, DeferConfig};
use fast_plane::sys;
use socket2::SockRef;
use std::os::fd::AsRawFd;

const FPS: u64 = 24;
const SYMBOL: u16 = 1_088;
const VIDEO: u64 = 1;
const AUDIO: u64 = 2;
/// The PCM stream's part interval.
const AUDIO_PART: Duration = Duration::from_millis(5);

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// What one client saw: when each part became whole.
#[derive(Debug, Default)]
struct Seen {
    port: u16,
    complete: HashMap<u64, Instant>,
    errors: u64,
}

/// A fast-lane client of `stream_id`: subscribes, tells `ready` its port
/// and first part, and records each part's completion until `stop`.
fn client(
    edge: SocketAddr,
    stream_id: u64,
    stop: Arc<AtomicBool>,
    ready: std::sync::mpsc::Sender<(u16, u64)>,
) -> Seen {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let _ = SockRef::from(&socket).set_recv_buffer_size(32 << 20);
    socket
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    let port = socket.local_addr().unwrap().port();
    let subscribe = |token| {
        Control::Subscribe {
            stream_id,
            from_part: None,
            repair: RepairPolicy::default(),
            token,
            fast_lane: true,
            compact: None,
            others: Vec::new(),
            resume: false,
        }
        .encode()
    };
    let mut buf = vec![0_u8; 65_536];
    socket.send_to(&subscribe(None), edge).unwrap();
    let token = loop {
        let (len, _) = socket.recv_from(&mut buf).expect("the edge sends a token");
        if let Some(Control::Token { token, .. }) = Control::decode(&buf[..len]) {
            break token;
        }
    };
    socket.send_to(&subscribe(Some(token)), edge).unwrap();
    socket.set_nonblocking(true).unwrap();
    let mut seen = Seen {
        port,
        ..Seen::default()
    };
    let mut receiver = ObjectReceiver::new();
    let mut batch = sys::RecvBatch::new();
    let fd = socket.as_raw_fd();
    let mut ready = Some(ready);
    loop {
        if sys::wait_readable(&[fd], 20).unwrap_or(0) == 0 {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            continue;
        }
        loop {
            let count = batch.recv(&socket).unwrap_or(0);
            let now = Instant::now();
            for index in 0..count {
                let data = batch.data(index);
                if let Some(control) = Control::decode(data) {
                    if let Control::Subscribed { next_part, .. } = control {
                        if let Some(ready) = ready.take() {
                            let _ = ready.send((port, next_part));
                        }
                    }
                    continue;
                }
                if let Some(object) = receiver.push(av_mesh::client_compact::LegId::FIRST, data) {
                    if object.kind == UdpObjectKind::Part {
                        seen.complete.entry(object.part).or_insert(now);
                    }
                }
            }
            if count < sys::RECV_BATCH {
                break;
            }
        }
    }
    seen.errors = receiver.errors;
    let _ = socket.send_to(&Control::Unsubscribe { stream_id, token }.encode(), edge);
    seen
}

/// A client's parts `parts` as it saw them: missing, the longest hold
/// (ms between two completions, the first part's completion and the last's
/// included), and each part's lateness after `sent` (ms).
#[derive(Debug, Clone)]
struct Verdict {
    missing: Vec<u64>,
    hold_ms: f64,
    late_p50_ms: f64,
    late_max_ms: f64,
    /// The latest part that completed more than 300 ms after the stall's
    /// end, if any (a tail), and the lateness of the parts the source sent
    /// after the stall's end (max ms).
    tail: Option<u64>,
    after_max_ms: f64,
}

fn verdict(
    seen: &Seen,
    parts: std::ops::Range<u64>,
    sent: &HashMap<u64, Instant>,
    stall_end: Option<Instant>,
) -> Verdict {
    let mut times: Vec<Instant> = parts
        .clone()
        .filter_map(|part| seen.complete.get(&part).copied())
        .collect();
    times.sort();
    let hold_ms = times
        .windows(2)
        .map(|pair| pair[1].duration_since(pair[0]).as_secs_f64() * 1e3)
        .fold(0.0, f64::max);
    let mut late: Vec<f64> = parts
        .clone()
        .filter_map(|part| {
            let done = seen.complete.get(&part)?;
            Some(done.saturating_duration_since(*sent.get(&part)?).as_secs_f64() * 1e3)
        })
        .collect();
    late.sort_by(f64::total_cmp);
    let tail = stall_end.and_then(|end| {
        parts
            .clone()
            .filter(|part| {
                seen.complete.get(part).is_some_and(|done| {
                    *done > end + Duration::from_millis(300)
                        && sent.get(part).is_some_and(|sent| *sent < end)
                })
            })
            .max()
    });
    let after_max_ms = stall_end.map_or(f64::NAN, |end| {
        parts
            .clone()
            .filter(|part| sent.get(part).is_some_and(|sent| *sent > end))
            .filter_map(|part| {
                Some(
                    seen.complete
                        .get(&part)?
                        .saturating_duration_since(sent[&part])
                        .as_secs_f64()
                        * 1e3,
                )
            })
            .fold(0.0, f64::max)
    });
    Verdict {
        missing: parts
            .filter(|part| !seen.complete.contains_key(part))
            .collect(),
        hold_ms,
        late_p50_ms: late.get(late.len() / 2).copied().unwrap_or(f64::NAN),
        late_max_ms: late.last().copied().unwrap_or(f64::NAN),
        tail,
        after_max_ms,
    }
}

/// One run's options.
#[derive(Debug, Clone, Copy)]
struct Options {
    media_mbps: u64,
    seconds: u64,
    /// The stall's length (0: none), from this long after the start.
    stall_ms: u64,
    stall_at: Duration,
    /// Stall the first client's path alone (else every client's).
    one_client: bool,
    /// `--send-defer-ms` (0: off, the part given back as before).
    defer_ms: u64,
    ip: u8,
}

#[derive(Debug)]
struct Run {
    video: [Verdict; 2],
    audio: Verdict,
    lane: fast_plane::edge_clients::FastLaneSnapshot,
    /// The cache-fed lane's sends and its queue's drops.
    edge_parts: u64,
    queue_drops: u64,
}

impl Run {
    fn line(&self, options: &Options) -> String {
        let video = |verdict: &Verdict| {
            format!(
                "missing {} hold {:.0} ms late p50 {:.1} max {:.0} ms tail {:?} after-stall max {:.0} ms",
                verdict.missing.len(),
                verdict.hold_ms,
                verdict.late_p50_ms,
                verdict.late_max_ms,
                verdict.tail,
                verdict.after_max_ms
            )
        };
        let defer = &self.lane.send_defer;
        format!(
            "defer {} ms, stall {} ms of {}: A [{}] B [{}] PCM [missing {} late p50 {:.2} max {:.1} ms]; \
             lane would_block {} given_back {}; kept {} sent later {} expired {} ({} parts) refused {} \
             high-water {} kB; handed {} parts {} symbols; partial sends {} parts {} datagrams late {} \
             uncached {}; cache-fed lane parts {} queue drops {}",
            options.defer_ms,
            options.stall_ms,
            if options.one_client { "client A" } else { "every client" },
            video(&self.video[0]),
            video(&self.video[1]),
            self.audio.missing.len(),
            self.audio.late_p50_ms,
            self.audio.late_max_ms,
            self.lane.would_block,
            self.lane.given_back,
            defer.deferred,
            defer.deferred_sent,
            defer.expired,
            defer.expired_parts,
            defer.refused,
            defer.high_water_bytes / 1_000,
            defer.handed_parts,
            defer.handed_symbols,
            self.lane.partial_parts,
            self.lane.partial_datagrams,
            self.lane.partial_late,
            self.lane.partial_uncached,
            self.edge_parts,
            self.queue_drops,
        )
    }
}

fn runtime(name: &str) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name(name)
        .enable_all()
        .build()
        .unwrap()
}

/// The PCM part `sequence`'s symbols.
fn pcm(sequence: u64, deadline: u64) -> Vec<Vec<u8>> {
    let (envelope, _) = packaged_envelope(Packaging::Pcm, AUDIO, 0, sequence, 288);
    let (mut sources, repairs) = fast_test_symbols(
        &envelope,
        relay_session::MediaPriority::Audio,
        1_088,
        400,
        0,
        deadline,
    );
    sources.extend(repairs);
    sources
}

fn run(options: Options) -> Run {
    let _lock = stall::LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let rt = runtime("stall-rt");
    let frames = make_frames_shaped(
        VIDEO,
        0,
        1 + (options.seconds * FPS) as usize,
        options.media_mbps * 1_000_000,
        FrameShape {
            fps: FPS,
            symbol_bytes: SYMBOL,
            repair_per_sources: 20,
        },
    );
    let defer = DeferConfig::new(options.defer_ms, 32 << 20);
    let edge = rt.block_on(async {
        tokio::spawn(LaneEdge::start_with_playout(
            loopback(options.ip),
            Duration::from_millis(500),
            move |config| {
                config.send_defer = defer;
                // The node's ring (16 MB from the start): a 4K part at 247
                // Mbit/s needs twice its 1.1 MB free to be teed.
                config.tee_capacity = fast_plane::relay::DEFAULT_TEE_CAPACITY;
            },
        ))
        .await
        .unwrap()
    });
    let video_fd = edge.forwarder_fd(0);
    let _ = SockRef::from(edge.forwarder_socket(0)).set_send_buffer_size(32 << 20);
    let video_target = edge.relay_port(0, true);
    let audio_target = edge.relay_port(1, false);
    let audio_fd = edge.forwarder_fd(1);
    send_frames_at(
        video_fd,
        video_target,
        &frames[..1],
        options.media_mbps * 1_000_000,
        Spread::Burst,
        Instant::now(),
        FPS,
        SYMBOL,
    );
    let deadline = now_unix_us() + 600_000_000;
    for datagram in pcm(0, deadline) {
        sys::send_batch(audio_fd, 1, |_| (&datagram[..], audio_target)).unwrap();
    }
    rt.block_on(async {
        wait_for_part(&edge.cache, VIDEO, 0).await;
        wait_for_part(&edge.cache, AUDIO, 0).await;
    });
    let stop = Arc::new(AtomicBool::new(false));
    let mut readers = Vec::new();
    let mut ports = Vec::new();
    for (index, stream_id) in [VIDEO, VIDEO, AUDIO].into_iter().enumerate() {
        let (tx, rx) = std::sync::mpsc::channel();
        let (port, stop) = (edge.client_port, Arc::clone(&stop));
        readers.push(
            std::thread::Builder::new()
                .name(format!("stall-client-{index}"))
                .spawn(move || client(port, stream_id, stop, tx))
                .unwrap(),
        );
        ports.push(
            rx.recv_timeout(Duration::from_secs(5))
                .expect("the edge subscribes the client")
                .0,
        );
    }
    let stall_fds = edge.lane.video_fds_for_test();
    let start = Instant::now() + Duration::from_millis(50);
    let bits = options.media_mbps * 1_000_000;
    let audio_parts = (options.seconds * 1_000 / AUDIO_PART.as_millis() as u64).max(1);
    let (video_sent, audio_sent, stall_end) = std::thread::scope(|scope| {
        let frames = &frames[1..];
        let video = scope.spawn(move || {
            send_frames_at(
                video_fd,
                video_target,
                frames,
                bits,
                Spread::Paced,
                start,
                FPS,
                SYMBOL,
            )
        });
        let audio = scope.spawn(move || {
            let mut sent = HashMap::new();
            for sequence in 1..=audio_parts {
                let due = start + AUDIO_PART * sequence as u32;
                std::thread::sleep(due.saturating_duration_since(Instant::now()));
                for datagram in pcm(sequence, deadline) {
                    let _ = sys::send_batch(audio_fd, 1, |_| (&datagram[..], audio_target));
                }
                sent.insert(sequence, Instant::now());
            }
            sent
        });
        let mut stall_end = None;
        if options.stall_ms > 0 {
            std::thread::sleep((start + options.stall_at).saturating_duration_since(Instant::now()));
            let port = if options.one_client { ports[0] } else { 0 };
            stall::set(&stall_fds, port, options.stall_ms);
            stall_end = Some(Instant::now() + Duration::from_millis(options.stall_ms));
            std::thread::sleep(Duration::from_millis(options.stall_ms));
            stall::clear();
        }
        let video_sent = video.join().unwrap();
        (video_sent, audio.join().unwrap(), stall_end)
    });
    std::thread::sleep(Duration::from_millis(800));
    stop.store(true, Ordering::Relaxed);
    let seen: Vec<Seen> = readers
        .into_iter()
        .map(|reader| reader.join().unwrap())
        .collect();
    let sent: HashMap<u64, Instant> = frames[1..]
        .iter()
        .map(|frame: &Frame| frame.part)
        .zip(video_sent)
        .collect();
    let parts = 1..1 + options.seconds * FPS;
    for client in &seen {
        assert_eq!(client.errors, 0, "client {} decodes every datagram", client.port);
    }
    let run = Run {
        video: [
            verdict(&seen[0], parts.clone(), &sent, stall_end),
            verdict(&seen[1], parts, &sent, stall_end),
        ],
        audio: verdict(&seen[2], 1..audio_parts + 1, &audio_sent, None),
        lane: edge.lane.snapshot_stats(),
        edge_parts: edge.stats.parts.load(Ordering::Relaxed),
        queue_drops: edge.stats.send_queue_drops.load(Ordering::Relaxed),
    };
    rt.block_on(edge.stop());
    run
}

/// A 100 ms stall of one client's path: every part reaches both clients on
/// the fast lane; nothing is given back or handed on; what was kept is
/// sent once the path takes datagrams again. Without keeping (as before),
/// the stalled client's parts of the stall are given back whole.
#[test]
fn a_short_stall_keeps_every_part_on_the_fast_lane() {
    let options = Options {
        media_mbps: 40,
        seconds: 2,
        stall_ms: 100,
        stall_at: Duration::from_millis(800),
        one_client: true,
        defer_ms: 200,
        ip: 151,
    };
    let kept = run(options);
    println!("{}", kept.line(&options));
    for verdict in &kept.video {
        assert!(verdict.missing.is_empty(), "{kept:?}");
    }
    let defer = kept.lane.send_defer;
    assert_eq!(kept.lane.given_back, 0, "{kept:?}");
    assert!(defer.deferred > 0, "{kept:?}");
    assert_eq!(defer.deferred_sent, defer.deferred, "all kept is sent: {kept:?}");
    assert_eq!((defer.expired, defer.handed_parts), (0, 0), "{kept:?}");
    assert_eq!(kept.lane.partial_parts, 0);
    let before = run(Options {
        defer_ms: 0,
        ip: 152,
        ..options
    });
    println!("{}", before.line(&Options { defer_ms: 0, ..options }));
    assert!(before.lane.given_back > 0, "{before:?}");
}

/// A stall longer than the bound (200 ms) but within the playout deadline:
/// the parts whose deadline passed are handed to the cache-fed lane with
/// the symbols the client lacks, which it sends when the path is back, and
/// those parts complete from the client's symbols of both lanes.
#[test]
fn a_stall_past_the_bound_hands_the_missing_symbols_on() {
    let options = Options {
        media_mbps: 40,
        seconds: 2,
        stall_ms: 320,
        stall_at: Duration::from_millis(800),
        one_client: true,
        defer_ms: 200,
        ip: 153,
    };
    let run = run(options);
    println!("{}", run.line(&options));
    let defer = run.lane.send_defer;
    assert_eq!(run.lane.given_back, 0, "{run:?}");
    assert!(defer.expired_parts > 0 && defer.handed_parts > 0, "{run:?}");
    assert!(run.lane.partial_parts > 0, "{run:?}");
    // The handed-on parts were sent their missing symbols, not whole: fewer
    // datagrams than the parts' sources.
    assert!(
        run.lane.partial_datagrams <= defer.handed_symbols + defer.handed_parts * 64,
        "{run:?}"
    );
    // The client is whole again: no part sent after the stall is missing,
    // and nothing completes long after it.
    assert_eq!(run.video[0].tail, None, "{run:?}");
    assert!(run.video[1].missing.is_empty(), "{run:?}");
}

/// The measurement: 4K at `ST_MBPS` of media (200: about 247 Mbit/s on the
/// wire) to two clients and 1 ms PCM to one, with a stall of each length
/// in `ST_STALLS`, with keeping (`--send-defer-ms 200`) and without.
#[test]
#[ignore = "a measurement: run it alone, in release"]
fn send_stall_measurement() {
    let media_mbps = env_u64("ST_MBPS", 200);
    let seconds = env_u64("ST_SECONDS", 6);
    let stalls: Vec<u64> = std::env::var("ST_STALLS")
        .unwrap_or_else(|_| "100,1000,3000".into())
        .split(',')
        .filter_map(|value| value.trim().parse().ok())
        .collect();
    let one_client = std::env::var("ST_SCOPE").as_deref() == Ok("one");
    let mut lines = Vec::new();
    let mut ip = 160;
    for stall_ms in stalls {
        for defer_ms in [200, 0] {
            ip += 1;
            let options = Options {
                media_mbps,
                seconds,
                stall_ms,
                stall_at: Duration::from_secs(2),
                one_client,
                defer_ms,
                ip,
            };
            let run = run(options);
            let line = run.line(&options);
            println!("{line}");
            lines.push(line);
        }
    }
    println!("\n{}", lines.join("\n"));
}

/// A forwarder's video thread (send240, A; 4k.md fix 5 dropped them): what
/// a child's socket refused for 150 ms is kept and sent in order as soon as
/// it takes datagrams again, so the child gets every symbol of every part,
/// in the order the parent forwarded them; the other child is not held.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forwarder_keeps_a_stalled_child_s_symbols_in_order() {
    const OFFSET: u16 = 1_000;
    let _lock = stall::LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (a_audio, a_video) = bind_class_pair(OFFSET).await;
    let (b_audio, b_video) = bind_class_pair(OFFSET).await;
    for socket in [&a_video, &b_video] {
        let _ = SockRef::from(socket).set_recv_buffer_size(16 << 20);
    }
    let relay = start_fast_relay_with(
        ParentPath::PromotedSecondary,
        &[
            (a_audio.local_addr().unwrap(), RelayForwardRole::All),
            (b_audio.local_addr().unwrap(), RelayForwardRole::All),
        ],
        fast_plane::relay::RelayClassConfig {
            port_offset: OFFSET,
            cpus: [None, None],
            realtime: false,
            tee_capacity: 4_096,
            video_gso: true,
            send_defer: DeferConfig::new(200, 8 << 20),
            ..Default::default()
        },
    )
    .await;
    let frames = make_frames_shaped(
        VIDEO,
        0,
        8,
        40_000_000,
        FrameShape {
            fps: FPS,
            symbol_bytes: SYMBOL,
            repair_per_sources: 20,
        },
    );
    let expected: Vec<Vec<u8>> = frames
        .iter()
        .flat_map(|frame| frame.datagrams.iter().cloned())
        .collect();
    let fd = relay.secondary_sender.as_raw_fd();
    let _ = SockRef::from(&relay.secondary_sender).set_send_buffer_size(16 << 20);
    let target = relay.secondary_video.unwrap();
    let a_port = a_video.local_addr().unwrap().port();
    let (a_got, b_got) = tokio::join!(receive_all(&a_video, expected.len()), async {
        let sender = tokio::task::spawn_blocking(move || {
            // Frames 2 to 4 leave while child A's path is stalled.
            let start = Instant::now();
            for (index, frame) in frames.iter().enumerate() {
                if index == 2 {
                    stall::set_port(a_port, 150);
                }
                if index == 5 {
                    stall::clear();
                }
                send_frames_at(
                    fd,
                    target,
                    std::slice::from_ref(frame),
                    40_000_000,
                    Spread::Burst,
                    start + Duration::from_millis(42 * index as u64),
                    FPS,
                    SYMBOL,
                );
                std::thread::sleep(Duration::from_millis(42));
            }
            stall::clear();
        });
        let got = receive_all(&b_video, expected.len()).await;
        sender.await.unwrap();
        got
    });
    let bytes = |got: Vec<(Vec<u8>, SocketAddr)>| got.into_iter().map(|(bytes, _)| bytes).collect::<Vec<_>>();
    assert!(bytes(b_got) == expected, "child B: every symbol, in order");
    assert!(bytes(a_got) == expected, "child A: every symbol, in order, after its stall");
    let stats = relay
        .settled_stats(|stats| stats.child_send_defer.deferred_sent > 0)
        .await;
    let defer = stats.child_send_defer;
    assert!(defer.deferred > 0, "{defer:?}");
    assert_eq!(defer.deferred_sent, defer.deferred, "{defer:?}");
    assert_eq!((defer.expired, defer.refused), (0, 0), "{defer:?}");
    relay.stop().await;
    drop((a_audio, b_audio));
}
