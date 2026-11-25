//! The node's memory at 4K (memory round, 24 Nov).
//!
//! On the referee's Linode lab (cmpd7) every forwarder and edge of a 4K
//! sweep was killed by the kernel at 240 Mbit/s. The chunk cache kept 1,117
//! parts of a 4K stream (46.5 s: 1.44 GB at 247 Mbit/s), each 40 s step's new
//! source epoch left the old window in another playlist slot, and glibc kept
//! what was freed. These tests hold the bounds that replace that:
//!
//! - a 4K stream's parts stay within the cache's byte bound for minutes, the
//!   playlist keeps more than three target durations, a replaced epoch's
//!   bytes go back, and a reader who asks behind the window gets a 404 at
//!   once;
//! - a video stream that goes quiet keeps only its newest part;
//! - the memory guard sheds video to its floor and then to nothing, refuses
//!   a new video playlist, and audio keeps every part throughout;
//! - `memory_bench` (ignored): a forwarder's or an edge's cache, tee and
//!   assembly at 247 or 320 Mbit/s for five simulated minutes, with the
//!   process's memory by structure every ten simulated seconds.
//!
//! The unit tests run a 4K stream at a tenth of its size (parts of a tenth,
//! bounds of a tenth): the windows are counted in parts and time, so the
//! seconds are the same.

use super::*;
use crate::alloc_count;
use av_mesh::mem_guard::{Guard, GuardState};

const FPS: u64 = 24;
const GOP: u64 = 48;
const PART_US: u32 = 41_667;
const TARGET_US: u32 = 2_000_000;
const MB: u64 = 1 << 20;

/// A part of a 4K stream at `rate` bytes a second: a key frame of four
/// times the others every GOP (the referee's clips: 24 fps, GOP 48, key
/// frames of 3-6 MB at 247 Mbit/s).
fn frame_bytes(rate: u64, sequence: u64) -> usize {
    // 47 frames of p and one of 4p make two seconds: 51 p = 2 rate.
    let p = rate * 2 / 51;
    (if sequence.is_multiple_of(GOP) { 4 * p } else { p }) as usize
}

fn stamp(sequence: u64) -> PartStamp {
    PartStamp {
        msn: sequence / GOP,
        part: (sequence % GOP) as u32,
        part_target_us: PART_US,
        target_duration_us: TARGET_US,
    }
}

/// A canonical 4K part of `bytes` media bytes, stamped as the contributor
/// stamps it.
fn encode_sized_part(stream_id: u64, epoch: u64, sequence: u64, bytes: usize) -> Bytes {
    let keyframe = sequence.is_multiple_of(GOP);
    let init = test_h264_aac_init();
    let media = vec![(sequence % 251) as u8; bytes];
    let payload = encode_test_fmp4_slot(keyframe.then_some(init.as_slice()), &media);
    let key = media_object::ObjectKey::for_payload(
        "default",
        stream_id.to_string(),
        "muxed-fmp4",
        epoch,
        0,
        sequence,
        1,
        &payload,
    )
    .unwrap();
    let stamp = stamp(sequence);
    let object = MediaObject::builder(key, ObjectKind::Media, payload.to_vec())
        .with_keyframe(keyframe)
        .with_metadata("container", b"fmp4".to_vec())
        .with_metadata("payload-format", b"fmp4-slot-v1".to_vec())
        .with_metadata("duration-ms", b"42".to_vec())
        .with_metadata("hls-msn", stamp.msn.to_string().into_bytes())
        .with_metadata("hls-part", stamp.part.to_string().into_bytes())
        .with_metadata("hls-part-target-us", PART_US.to_string().into_bytes())
        .with_metadata("hls-target-duration-us", TARGET_US.to_string().into_bytes())
        .build()
        .unwrap();
    Bytes::from(media_object::encode(&object).unwrap())
}

/// A node shaped as the Linode lab's: 2.5 ms nominal parts, a 12,000-part
/// window (the deploy's 12 s of 1 ms parts), 8 MiB slots, four playlists.
async fn lab_node() -> Arc<LiveTsCache> {
    LiveTsCache::with_ring(
        1,
        Duration::from_micros(2_500),
        400,
        12_000,
        8_192,
        4,
        cache_ring_parts(2_500, 400, 12_000, DEFAULT_MIN_PART_US),
    )
    .await
}

async fn commit_frames(cache: &LiveTsCache, stream_id: u64, epoch: u64, frames: std::ops::Range<u64>, rate: u64) {
    for sequence in frames {
        cache
            .commit_stream_payload(
                stream_id,
                encode_sized_part(stream_id, epoch, sequence, frame_bytes(rate, sequence)),
            )
            .await
            .unwrap();
    }
}

async fn retained_bytes(cache: &LiveTsCache, stream_id: u64) -> u64 {
    cache
        .state
        .read()
        .await
        .stream_retained_bytes
        .get(&stream_id)
        .copied()
        .unwrap_or(0)
}

/// Seconds of a stream's playlist window and of all it retains.
async fn window_seconds(cache: &LiveTsCache, stream_id: u64) -> (f64, f64) {
    let state = cache.state.read().await;
    let (window, after) = state.stream_window_split(stream_id, cache.window_parts);
    let seconds = |parts: usize| parts as f64 / FPS as f64;
    (seconds(window), seconds(window + after))
}

/// Wait for the spawned release of a hidden stream's bytes.
async fn settle(cache: &LiveTsCache, at_most: u64) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let held = cache.chunk_cache.held_bytes();
        if held <= at_most || Instant::now() > deadline {
            return held;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn a_4k_stream_stays_within_its_byte_bound_for_minutes() {
    // 247 Mbit/s at a tenth: 3.09 MB/s; a 4 GB node's 392 MB at a tenth.
    let rate = 247_000_000 / 8 / 10;
    let budget = 392 * MB / 10;
    let cache = lab_node().await;
    cache
        .set_byte_budget(CacheByteBudget {
            playlist: budget,
            node: budget,
        })
        .await;
    let largest = frame_bytes(rate, 0) as u64 + 4_096;
    let bound = budget + budget / 8 + largest;
    let mut peak = 0;
    // Three simulated minutes in steps of ten seconds.
    for step in 0..18_u64 {
        commit_frames(&cache, 77, 5, step * 240..(step + 1) * 240, rate).await;
        let held = cache.chunk_cache.held_bytes();
        peak = peak.max(held);
        assert!(held <= bound, "step {step}: {held} bytes held, bound {bound}");
        assert!(retained_bytes(&cache, 77).await <= budget + budget / 8);
    }
    // The window keeps more than three target durations (6 s), and the
    // bound is used: at least 80% of it is held.
    let (window_s, retained_s) = window_seconds(&cache, 77).await;
    assert!(window_s >= 6.0, "a window of {window_s} s");
    assert!(peak >= budget * 8 / 10, "{peak} of {budget}");
    eprintln!(
        "247 Mbit/s (a tenth): held {} MB of {} MB, a window of {window_s:.1} s, {retained_s:.1} s retained",
        cache.chunk_cache.held_bytes() / MB,
        budget / MB
    );
    // Without the bound the same stream keeps over 1,100 parts (46-48 s;
    // cmpd7's /api/node: 1,117).
    let unbounded = lab_node().await;
    commit_frames(&unbounded, 77, 5, 0..1_300, rate).await;
    let parts = unbounded.state.read().await.stream_parts[&77].len();
    assert!((1_100..1_200).contains(&parts), "{parts}");

    // A late reader who asks behind the window is answered at once.
    let head = 18 * 240 - 1;
    let behind = head - 20 * FPS;
    assert!(!cache.part_request_may_wait(77, behind).await);
    assert!(cache.chunk_cache.get_for_stream_id(77, behind as usize).await.is_none());
    let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
    let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
    let started = Instant::now();
    let old = get_for_tests(&router, &format!("/live/77/part{behind}.mp4")).await;
    assert_eq!(old.status, StatusCode::NOT_FOUND);
    assert!(started.elapsed() < Duration::from_millis(200), "{:?}", started.elapsed());
    let newest = get_for_tests(&router, &format!("/live/77/part{head}.mp4")).await;
    assert_eq!(newest.status, StatusCode::OK);
    let playlist = cache.playlist_for_stream_id(77).await;
    assert!(playlist.contains(&format!("part{head}.mp4")), "{playlist}");
    assert!(!playlist.contains(&format!("part{behind}.mp4")));
    mesh.shutdown();

    // A new source epoch: the old window's bytes go back, not only out of
    // sight.
    commit_frames(&cache, 77, 6, 0..48, rate).await;
    let new_epoch = retained_bytes(&cache, 77).await;
    let held = settle(&cache, new_epoch).await;
    assert_eq!(held, new_epoch, "the old epoch's bytes are given back");
}

#[tokio::test]
async fn two_4k_streams_share_the_node_s_bound() {
    let rate = 247_000_000 / 8 / 10;
    let budget = 392 * MB / 10;
    let cache = lab_node().await;
    cache
        .set_byte_budget(CacheByteBudget {
            playlist: budget,
            node: budget,
        })
        .await;
    for step in 0..6_u64 {
        let frames = step * 240..(step + 1) * 240;
        commit_frames(&cache, 77, 5, frames.clone(), rate).await;
        commit_frames(&cache, 78, 5, frames, rate).await;
    }
    let largest = frame_bytes(rate, 0) as u64 + 4_096;
    let held = cache.chunk_cache.held_bytes();
    assert!(held <= budget + budget / 8 + 2 * largest, "{held}");
    for stream_id in [77, 78] {
        assert!(retained_bytes(&cache, stream_id).await <= (budget / 2) * 9 / 8);
    }
}

#[tokio::test]
async fn a_quiet_video_stream_gives_its_memory_back() {
    let rate = 247_000_000 / 8 / 10;
    let cache = lab_node().await;
    cache
        .set_byte_budget(CacheByteBudget {
            playlist: 392 * MB / 10,
            node: 392 * MB / 10,
        })
        .await;
    commit_frames(&cache, 77, 5, 0..480, rate).await;
    let full = cache.chunk_cache.held_bytes();
    assert!(full > 30 * MB, "{full}");
    let now = now_unix_ms();
    // Not yet quiet for 30 s: nothing is released.
    assert_eq!(cache.memory_pass(GuardState::Ok, 0, 30_000, now + 29_000).await, 0);
    assert_eq!(cache.chunk_cache.held_bytes(), full);
    // Quiet for longer: only the newest part stays.
    let released = cache.memory_pass(GuardState::Ok, 0, 30_000, now + 31_000).await;
    assert!(released >= full - 2 * frame_bytes(rate, 0) as u64, "{released} of {full}");
    let held = cache.chunk_cache.held_bytes();
    assert!(held <= frame_bytes(rate, 479) as u64 + 4_096, "{held}");
    assert_eq!(cache.state.read().await.stream_parts[&77].len(), 1);
    // The stream comes back: at the guard's next pass (within a second) the
    // cap goes and its window grows again.
    commit_frames(&cache, 77, 5, 480..482, rate).await;
    assert_eq!(cache.memory_pass(GuardState::Ok, 0, 30_000, now_unix_ms()).await, 0);
    commit_frames(&cache, 77, 5, 482..720, rate).await;
    assert!(cache.state.read().await.stream_parts[&77].len() > 200);
}

/// Audio parts of 5 ms, 300 bytes each, on stream 2001.
async fn commit_audio(cache: &LiveTsCache, parts: std::ops::Range<u64>) {
    for sequence in parts {
        cache
            .commit_stream_payload(
                2_001,
                encode_test_published_fmp4_part(2_001, 5, sequence, sequence % 200 == 0, 5),
            )
            .await
            .unwrap();
    }
}

async fn audio_parts_present(cache: &LiveTsCache, parts: std::ops::Range<u64>) -> bool {
    for sequence in parts {
        if cache
            .chunk_cache
            .get_for_stream_id(2_001, sequence as usize)
            .await
            .is_none()
        {
            return false;
        }
    }
    true
}

#[tokio::test]
async fn the_guard_sheds_the_largest_stores_and_audio_keeps_every_part() {
    let rate = 247_000_000 / 8 / 10;
    let budget = 392 * MB / 10;
    let floor = budget / 2;
    let cache = lab_node().await;
    cache
        .set_byte_budget(CacheByteBudget {
            playlist: budget,
            node: budget,
        })
        .await;
    let guard = Arc::new(Guard::new(2_000 * MB, 2_600 * MB));
    assert!(cache.memory_guard.set(Arc::clone(&guard)).is_ok());
    // Ten seconds of video beside ten seconds of audio in 5 ms parts.
    for second in 0..10_u64 {
        commit_frames(&cache, 77, 5, second * FPS..(second + 1) * FPS, rate).await;
        commit_audio(&cache, second * 200..(second + 1) * 200).await;
    }
    let audio_parts = cache.state.read().await.stream_parts[&2_001].len();
    assert_eq!(audio_parts, 2_000);

    // Soft: video down to its floor.
    assert_eq!(guard.tick(2_100 * MB, cache.chunk_cache.held_bytes()).state, GuardState::Soft);
    cache.memory_pass(GuardState::Soft, floor, 30_000, now_unix_ms()).await;
    let video = retained_bytes(&cache, 77).await;
    assert!(video <= floor + floor / 8, "{video} > floor {floor}");
    assert!(audio_parts_present(&cache, 0..2_000).await);
    // Above the soft limit a new playlist of large parts is refused, by
    // the size of its first part; one of small parts is taken.
    let refused = cache
        .commit_stream_payload(78, encode_sized_part(78, 5, 0, 1 << 20))
        .await;
    assert!(refused.is_err());
    let snapshot = guard.snapshot(&Default::default(), &Default::default());
    assert_eq!(snapshot.refused_large_playlists, 1);
    assert!(cache
        .commit_stream_payload(79, encode_sized_part(79, 5, 0, 1_000))
        .await
        .is_ok());
    commit_audio(&cache, 2_000..2_200).await;

    // Hard: every video cache shed, audio goes on without a missing part.
    assert_eq!(guard.tick(2_700 * MB, cache.chunk_cache.held_bytes()).state, GuardState::Hard);
    cache.memory_pass(GuardState::Hard, floor, 30_000, now_unix_ms()).await;
    for second in 11..13_u64 {
        commit_frames(&cache, 77, 5, second * FPS..(second + 1) * FPS, rate).await;
        commit_audio(&cache, second * 200..(second + 1) * 200).await;
        cache.memory_pass(GuardState::Hard, floor, 30_000, now_unix_ms()).await;
    }
    let video = retained_bytes(&cache, 77).await;
    assert!(video <= frame_bytes(rate, 0) as u64 + 4_096, "{video}");
    assert!(audio_parts_present(&cache, 0..2_600).await);
    let audio_playlist = cache.playlist_for_stream_id(2_001).await;
    assert!(audio_playlist.contains("part2599.mp4"));

    // Below 90% of the soft limit the caps go and the window grows again.
    assert_eq!(guard.tick(1_000 * MB, cache.chunk_cache.held_bytes()).state, GuardState::Ok);
    cache.memory_pass(GuardState::Ok, floor, 30_000, now_unix_ms()).await;
    commit_frames(&cache, 77, 5, 13 * FPS..20 * FPS, rate).await;
    assert!(cache.state.read().await.stream_parts[&77].len() > 100);
    // A playlist of large parts is taken again (here every slot is in use,
    // so the guard's word is checked).
    assert!(guard.takes_new_large_playlist());
}

fn mallinfo() -> (u64, u64, u64) {
    // SAFETY: mallinfo2 takes nothing and returns a plain struct.
    let info = unsafe { libc::mallinfo2() };
    // From the kernel (heap + mmapped), in use, free inside the heaps.
    (
        (info.arena + info.hblkhd) as u64,
        (info.uordblks + info.hblkhd) as u64,
        info.fordblks as u64,
    )
}

/// The in-process memory bench (ignored; run alone, in release):
/// `cargo test --release memory_bench -- --ignored --nocapture
/// --test-threads 1`. Env: MEM_MBPS (247), MEM_SECONDS (300 simulated),
/// MEM_EPOCH_S (40: a new source epoch every step, as the referee's sweep),
/// MEM_BEFORE=1 (no byte bound and hidden epochs left in place, as df6414c),
/// MEM_CACHE_MB (392), MEM_TRIM (1: the guard's trim after a release).
///
/// A tee thread takes each frame as 1,316-byte symbols through a 96 MiB
/// tee ring, assembles it as the decoder does (a copy of each symbol, then
/// the frame), and hands it to the cache. Every ten simulated seconds it
/// prints the process's RSS and the memory by structure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "a measurement; run alone"]
async fn memory_bench() {
    let env = |name: &str, default: u64| {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    };
    let mbps = env("MEM_MBPS", 247);
    let seconds = env("MEM_SECONDS", 300);
    let epoch_s = env("MEM_EPOCH_S", 40);
    let before = env("MEM_BEFORE", 0) == 1;
    let trim = env("MEM_TRIM", 1) == 1;
    let cache_mb = env("MEM_CACHE_MB", 392);
    // 4k-repair (integration 5b): each part kept in a forwarder's part
    // store (48 MiB) and, at MEM_REPAIR_LOSS_PPM (10,000) of one child's
    // loss, link repair made from it (its encoder: about twice the part),
    // as a forwarder with one lossy child. 0: no part repair.
    let repair_on = env("MEM_REPAIR", 1) == 1;
    let repair_loss_ppm = u32::try_from(env("MEM_REPAIR_LOSS_PPM", 10_000)).unwrap();
    let repair = repair_on.then(|| {
        fast_plane::part_repair::PartRepair::start(
            fast_plane::part_repair::PartRepairConfig::default(),
            relay_session::RelayLimits::default(),
        )
    });
    let sink = std::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    let sink_addr = sink.local_addr().unwrap();
    let via: Arc<dyn fast_plane::part_repair::SendSocket> =
        Arc::new(std::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap());
    LEGACY_HIDE.store(before, Ordering::Relaxed);
    let rate = mbps * 1_000_000 / 8;
    let rss = || av_mesh::mem_guard::rss_bytes() / MB;
    let start_rss = rss();
    // The Linode lab's node: 16 playlists.
    let cache = LiveTsCache::with_ring(
        1,
        Duration::from_micros(2_500),
        400,
        12_000,
        8_192,
        16,
        cache_ring_parts(2_500, 400, 12_000, DEFAULT_MIN_PART_US),
    )
    .await;
    let ring_slots = cache.chunk_cache.options.num_playlists * cache.state.read().await.ring_parts;
    if !before {
        cache
            .set_byte_budget(CacheByteBudget {
                playlist: cache_mb * MB,
                node: cache_mb * MB,
            })
            .await;
    }
    cache.stream_classes().mark_video(77);
    let (mut tee_tx, mut tee_rx) =
        fast_plane::tee_ring::tee_ring_with_bytes::<u32>(65_536, 96 << 20);
    let (frames_tx, mut frames_rx) = tokio::sync::mpsc::channel::<(u64, u64, Bytes)>(8);
    let frames = seconds * FPS;
    let per_epoch = epoch_s * FPS;
    // The tee thread: symbols in, assemblies, frames out.
    let tee = std::thread::spawn(move || {
        for frame in 0..frames {
            let (epoch, sequence) = (5 + frame / per_epoch, frame % per_epoch);
            let size = frame_bytes(rate, sequence);
            let encoded = encode_sized_part(77, epoch, sequence, size);
            let mut symbols: Vec<Vec<u8>> = Vec::new();
            for chunk in encoded.chunks(1_316) {
                while !tee_tx.push(frame as u32, chunk) {
                    std::thread::yield_now();
                }
                if let Some((_, data)) = tee_rx.peek() {
                    symbols.push(data.to_vec());
                    tee_rx.release();
                }
            }
            while let Some((_, data)) = tee_rx.peek() {
                symbols.push(data.to_vec());
                tee_rx.release();
            }
            let mut assembled = Vec::with_capacity(encoded.len());
            for symbol in &symbols {
                assembled.extend_from_slice(symbol);
            }
            drop(symbols);
            if frames_tx
                .blocking_send((epoch, sequence, Bytes::from(assembled)))
                .is_err()
            {
                return;
            }
        }
    });
    println!(
        "memory bench: {mbps} Mbit/s, {seconds} s, epoch every {epoch_s} s, {} (start RSS {start_rss} MB, ring index {} MB)",
        if before { "BEFORE (df6414c)" } else { "bounded" },
        ring_slots * 130 / MB as usize,
    );
    println!("  sim_s  rss_mb  cache_mb  retained_mb  malloc_os_mb  in_use_mb  free_in_heap_mb  live<4K  <64K  <1M  >=1M  trimmed_mb  store_mb  plans_mb  link_parts");
    let mut last_held = 0;
    let mut trimmed = 0;
    let mut peak_rss = 0;
    let mut after_first_window = Vec::new();
    while let Some((epoch, sequence, payload)) = frames_rx.recv().await {
        // A forwarder and a fast-plane-fed edge (a lease child) commit a
        // part alike; their other structures are fixed (see the report).
        if let Some(repair) = &repair {
            let part = fast_plane::part_repair::stored_part_for_test(77, epoch, sequence, payload.to_vec(), 1_316);
            repair.keep(part, &[], |_| fast_plane::part_repair::Children {
                lossy: vec![(Arc::clone(&via), sink_addr, repair_loss_ppm)],
                sources: Vec::new(),
            });
        }
        cache.commit_stream_payload(77, payload).await.unwrap();
        let frame = (epoch - 5) * per_epoch + sequence;
        if (frame + 1) % FPS == 0 {
            let held = cache.chunk_cache.held_bytes();
            if !before && trim && last_held >= held + av_mesh::mem_guard::TRIM_AFTER_RELEASE_BYTES {
                trimmed += av_mesh::mem_guard::trim();
                last_held = held;
            }
            last_held = last_held.max(held);
        }
        if (frame + 1) % (10 * FPS) == 0 {
            // Let the spawned releases run before the sample.
            tokio::task::yield_now().await;
            let sim_s = (frame + 1) / FPS;
            let (os, in_use, free) = mallinfo();
            let live = alloc_count::live_bytes_by_class();
            let now_rss = rss();
            peak_rss = peak_rss.max(now_rss);
            if sim_s > 60 {
                after_first_window.push(now_rss);
            }
            println!(
                "  {sim_s:5}  {now_rss:6}  {:8}  {:11}  {:12}  {:9}  {:15}  {:7}  {:4}  {:4}  {:4}  {:10}  {:8}  {:8}  {}",
                cache.chunk_cache.held_bytes() / MB,
                retained_bytes(&cache, 77).await / MB,
                os / MB,
                in_use / MB,
                free / MB,
                live[0] / MB,
                live[1] / MB,
                live[2] / MB,
                live[3] / MB,
                trimmed / MB,
                av_mesh::mem_guard::store_totals().part_store_bytes / MB,
                av_mesh::mem_guard::store_totals().plans_bytes / MB,
                repair.as_ref().map_or(0, |repair| repair.stats.link_repair_parts.load(Ordering::Relaxed)),
            );
        }
    }
    tee.join().unwrap();
    let (low, high) = after_first_window
        .iter()
        .fold((u64::MAX, 0), |(low, high), rss| (low.min(*rss), high.max(*rss)));
    println!(
        "memory bench: peak RSS {peak_rss} MB; after the first minute {low}-{high} MB; window {:?} s",
        window_seconds(&cache, 77).await
    );
    // The stream ends: quiet for 30 s, its parts go, and a trim returns them.
    let released = cache.memory_pass(GuardState::Ok, 0, 30_000, now_unix_ms() + 31_000).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let returned = av_mesh::mem_guard::trim();
    println!(
        "memory bench: the stream ends: released {} MB of parts, the trim returned {} MB, RSS now {} MB",
        released / MB,
        returned / MB,
        rss()
    );
}
