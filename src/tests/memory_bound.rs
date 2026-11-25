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
    println!("  sim_s  rss_mb  cache_mb  retained_mb  malloc_os_mb  in_use_mb  free_in_heap_mb  live<4K  <64K  <1M  >=1M  trimmed_mb");
    let mut last_held = 0;
    let mut trimmed = 0;
    let mut peak_rss = 0;
    let mut after_first_window = Vec::new();
    while let Some((epoch, sequence, payload)) = frames_rx.recv().await {
        // A forwarder and a fast-plane-fed edge (a lease child) commit a
        // part alike; their other structures are fixed (see the report).
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
                "  {sim_s:5}  {now_rss:6}  {:8}  {:11}  {:12}  {:9}  {:15}  {:7}  {:4}  {:4}  {:4}  {:10}",
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

/// glibc's arenas from `malloc_info`: (arenas, bytes they hold from the
/// kernel, bytes free inside them, the largest arena's bytes).
fn arena_stats() -> (usize, u64, u64, u64) {
    let mut buffer: *mut libc::c_char = std::ptr::null_mut();
    let mut size: libc::size_t = 0;
    // SAFETY: open_memstream writes the pointer and size it is given; the
    // stream is closed before the buffer is read and freed.
    let text = unsafe {
        let stream = libc::open_memstream(&mut buffer, &mut size);
        if stream.is_null() {
            return (0, 0, 0, 0);
        }
        libc::malloc_info(0, stream);
        libc::fclose(stream);
        let text = std::slice::from_raw_parts(buffer.cast::<u8>(), size).to_vec();
        libc::free(buffer.cast());
        String::from_utf8_lossy(&text).into_owned()
    };
    let attribute = |line: &str, name: &str| {
        line.split(&format!("{name}=\""))
            .nth(1)
            .and_then(|rest| rest.split('"').next())
            .and_then(|value| value.parse::<u64>().ok())
    };
    let (mut arenas, mut system, mut free, mut largest) = (0, 0, 0, 0);
    let mut in_heap = false;
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with("<heap ") {
            arenas += 1;
            in_heap = true;
        } else if line.starts_with("</heap>") {
            in_heap = false;
        } else if in_heap && line.starts_with("<system type=\"current\"") {
            let bytes = attribute(line, "size").unwrap_or(0);
            system += bytes;
            largest = largest.max(bytes);
        } else if in_heap && line.starts_with("<total type=\"rest\"") {
            free += attribute(line, "size").unwrap_or(0);
        } else if in_heap && line.starts_with("<total type=\"fast\"") {
            free += attribute(line, "size").unwrap_or(0);
        }
    }
    (arenas, system, free, largest)
}

/// A forwarder in this process at 4K with N cache-mesh children (mem2): the
/// forwarder's cache, its mesh split by class serving every child, a tee
/// thread that assembles each frame from 1,316-byte symbols, and N edges
/// whose caches are kept small (`MEM_EDGE_CACHE_MB`, 24) so that the
/// forwarder's own memory shows. Real time (the mesh is): `MEM_SECONDS`
/// (180), `MEM_MBPS` (247), `MEM_CHILDREN` (5), `MEM_TRIM_EVERY` (0: the
/// guard's trims only; N: also a trim every N s). Every 10 s: RSS, the
/// caches' bytes, live bytes by size class, the arenas.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement; run alone"]
async fn forwarder_memory_bench() {
    use playlists::mesh::{CacheMesh, CacheMeshConfig, CacheMeshRole as MeshRole};
    let env = |name: &str, default: u64| {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    };
    let mbps = env("MEM_MBPS", 247);
    let seconds = env("MEM_SECONDS", 180);
    let children = env("MEM_CHILDREN", 5) as usize;
    let edge_cache_mb = env("MEM_EDGE_CACHE_MB", 24);
    let trim_every = env("MEM_TRIM_EVERY", 0);
    let rate = mbps * 1_000_000 / 8;
    let rss = || av_mesh::mem_guard::rss_bytes() / MB;
    let node = || async {
        LiveTsCache::with_ring(
            1,
            Duration::from_micros(2_500),
            400,
            12_000,
            8_192,
            16,
            cache_ring_parts(2_500, 400, 12_000, DEFAULT_MIN_PART_US),
        )
        .await
    };
    let classify = |cache: &Arc<LiveTsCache>| {
        let classes = cache.stream_classes().clone();
        Arc::new(move |stream_id| classes.mesh_class(stream_id))
            as playlists::mesh::CacheMeshStreamClassifier
    };
    let forwarder = node().await;
    forwarder
        .set_byte_budget(CacheByteBudget {
            playlist: 392 * MB,
            node: 392 * MB,
        })
        .await;
    forwarder.stream_classes().mark_video(77);
    let forwarder_addr = unused_loopback_addr();
    let edge_addrs: Vec<SocketAddr> = (0..children).map(|_| unused_loopback_addr()).collect();
    let mut config = CacheMeshConfig::new("dist-a", "uk", forwarder_addr)
        .with_role(MeshRole::Distributor)
        .with_announce_interval(Duration::from_millis(100))
        .with_socket_buffer_bytes(32 << 20)
        .with_class_port_offset(1_000)
        .with_stream_classifier(classify(&forwarder));
    for addr in &edge_addrs {
        config = config.with_peer(*addr);
    }
    let forwarder_mesh = CacheMesh::new(Arc::clone(&forwarder.chunk_cache), config)
        .start()
        .await
        .unwrap();
    let mut edges = Vec::new();
    for addr in &edge_addrs {
        let edge = node().await;
        edge.set_byte_budget(CacheByteBudget {
            playlist: edge_cache_mb * MB,
            node: edge_cache_mb * MB,
        })
        .await;
        edge.stream_classes().mark_video(77);
        let (applied_tx, mut applied_rx) = mpsc::unbounded_channel();
        let mesh = CacheMesh::new(
            Arc::clone(&edge.chunk_cache),
            CacheMeshConfig::new("edge", "uk", *addr)
                .with_peer(forwarder_addr)
                .with_role(MeshRole::Edge)
                .with_announce_interval(Duration::from_millis(100))
                .with_applied_tx(applied_tx)
                .with_socket_buffer_bytes(32 << 20)
                .with_class_port_offset(1_000)
                .with_stream_classifier(classify(&edge)),
        )
        .start()
        .await
        .unwrap();
        // The edge records what the mesh applied, as a node does.
        let recorder = Arc::clone(&edge);
        tokio::spawn(async move {
            while let Some(applied) = applied_rx.recv().await {
                let _ = recorder
                    .observe_mesh_part(applied.stream_id, applied.slot_id, None)
                    .await;
            }
        });
        edges.push((edge, mesh));
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    // The tee thread: frames as symbols, assembled, at 24 fps of real time.
    let (frames_tx, mut frames_rx) = tokio::sync::mpsc::channel::<Bytes>(8);
    let frames = seconds * FPS;
    let tee = std::thread::spawn(move || {
        let start = std::time::Instant::now();
        for frame in 0..frames {
            let due = start + Duration::from_micros(41_667 * frame);
            if let Some(wait) = due.checked_duration_since(std::time::Instant::now()) {
                std::thread::sleep(wait);
            }
            let encoded = encode_sized_part(77, 5, frame, frame_bytes(rate, frame));
            let symbols: Vec<Vec<u8>> = encoded.chunks(1_316).map(<[u8]>::to_vec).collect();
            let mut assembled = Vec::with_capacity(encoded.len());
            for symbol in &symbols {
                assembled.extend_from_slice(symbol);
            }
            drop(symbols);
            if frames_tx.blocking_send(Bytes::from(assembled)).is_err() {
                return;
            }
        }
    });
    println!(
        "forwarder bench: {mbps} Mbit/s, {children} mesh children, {seconds} s, trim every {trim_every} s"
    );
    println!("  s  rss_mb  fwd_cache  edges_cache  live_mb(<4K/<64K/<1M/>=1M)  arenas  arena_sys_mb  arena_free_mb  largest_mb  edge_head  trimmed");
    let mut frame = 0_u64;
    let mut trimmed = 0;
    let mut last_held = 0;
    while let Some(payload) = frames_rx.recv().await {
        forwarder.commit_stream_payload(77, payload).await.unwrap();
        frame += 1;
        if frame.is_multiple_of(FPS) {
            let held = forwarder.chunk_cache.held_bytes();
            if last_held >= held + av_mesh::mem_guard::TRIM_AFTER_RELEASE_BYTES {
                trimmed += av_mesh::mem_guard::trim();
                last_held = held;
            }
            last_held = last_held.max(held);
            if trim_every > 0 && (frame / FPS).is_multiple_of(trim_every) {
                trimmed += av_mesh::mem_guard::trim();
            }
        }
        if frame.is_multiple_of(10 * FPS) {
            let live = alloc_count::live_bytes_by_class();
            let (arenas, system, free, largest) = arena_stats();
            let mut edge_held = 0;
            let mut heads = Vec::new();
            for (edge, _) in &edges {
                edge_held += edge.chunk_cache.held_bytes();
                heads.push(edge.stream_position_for_id(77).await.map_or(0, |(_, last)| last));
            }
            println!(
                "  {:4}  {:6}  {:9}  {:11}  {}/{}/{}/{}  {:6}  {:12}  {:13}  {:10}  {:?}  {}",
                frame / FPS,
                rss(),
                forwarder.chunk_cache.held_bytes() / MB,
                edge_held / MB,
                live[0] / MB,
                live[1] / MB,
                live[2] / MB,
                live[3] / MB,
                arenas,
                system / MB,
                free / MB,
                largest / MB,
                heads.iter().min().zip(heads.iter().max()),
                trimmed / MB,
            );
        }
    }
    tee.join().unwrap();
    let before = rss();
    let (_, _, free_before, _) = arena_stats();
    let returned = av_mesh::mem_guard::trim();
    let (_, _, free_after, _) = arena_stats();
    println!(
        "forwarder bench: end RSS {before} MB; a trim returned {} MB (arena free {} -> {} MB), RSS {} MB",
        returned / MB,
        free_before / MB,
        free_after / MB,
        rss()
    );
    forwarder_mesh.shutdown();
    for (_, mesh) in edges {
        mesh.shutdown();
    }
}

/// A whole forwarder in this process at 4K (mem2): the fast plane's relay
/// ingest (class threads, tee ring, assembly from 1,316-byte RelaySession
/// symbols, commit), `MEM_CHILDREN` compiled children it forwards every
/// symbol to (each drained by a thread), and its cache mesh split by class
/// serving as many edge children (whose caches are kept at
/// `MEM_EDGE_CACHE_MB`, 16). The forwarder's cache is a Linode node's (16
/// playlists, 392 MB). Real time: `MEM_SECONDS` (600), `MEM_MBPS` (247),
/// `MEM_TRIM_EVERY` (0). Every 10 s: RSS, the caches, live bytes by size
/// class, the arenas.
#[test]
#[ignore = "a measurement; run alone"]
fn whole_forwarder_memory_bench() {
    use super::video_throughput::{make_frames_shaped, send_frames_at, FrameShape, Spread};
    use playlists::mesh::{CacheMesh, CacheMeshConfig, CacheMeshRole as MeshRole};
    use std::os::fd::AsRawFd;
    let env = |name: &str, default: u64| {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    };
    let mbps = env("MEM_MBPS", 247);
    let seconds = env("MEM_SECONDS", 600);
    let children = env("MEM_CHILDREN", 5) as usize;
    let edge_cache_mb = env("MEM_EDGE_CACHE_MB", 16);
    let trim_every = env("MEM_TRIM_EVERY", 0);
    // The allocator's thresholds as `--malloc-tune` sets them, or an mmap
    // threshold of MEM_MMAP_KB.
    let mmap_kb = env("MEM_MMAP_KB", 0);
    if env("MEM_TUNE", 1) == 1 {
        let _ = av_mesh::memory::tune_allocator(true);
    }
    if mmap_kb > 0 {
        // SAFETY: mallopt takes plain integers.
        unsafe { libc::mallopt(libc::M_MMAP_THRESHOLD, (mmap_kb * 1024) as i32) };
    }
    let trim_threshold_mb = env("MEM_TRIM_THRESHOLD_MB", 0);
    if trim_threshold_mb > 0 {
        // SAFETY: as above.
        unsafe { libc::mallopt(libc::M_TRIM_THRESHOLD, (trim_threshold_mb << 20) as i32) };
    }
    const OFFSET: u16 = 1_000;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("fwd-rt")
        .enable_all()
        .build()
        .unwrap();
    let node = || async {
        LiveTsCache::with_ring(
            1,
            Duration::from_micros(2_500),
            400,
            12_000,
            8_192,
            16,
            cache_ring_parts(2_500, 400, 12_000, DEFAULT_MIN_PART_US),
        )
        .await
    };
    let classify = |cache: &Arc<LiveTsCache>| {
        let classes = cache.stream_classes().clone();
        Arc::new(move |stream_id| classes.mesh_class(stream_id))
            as playlists::mesh::CacheMeshStreamClassifier
    };
    let rss = || av_mesh::mem_guard::rss_bytes() / MB;
    let start_rss = rss();
    let (relay, compiled, meshes, edges) = rt.block_on(async {
        let mut compiled = Vec::new();
        for _ in 0..children {
            compiled.push(bind_class_pair(OFFSET).await);
        }
        let targets: Vec<RelayForwardEndpoint> = compiled
            .iter()
            .map(|(audio, _)| RelayForwardEndpoint {
                bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                target: audio.local_addr().unwrap(),
                role: RelayForwardRole::All,
            })
            .collect();
        let cache = node().await;
        cache
            .set_byte_budget(CacheByteBudget {
                playlist: 392 * MB,
                node: 392 * MB,
            })
            .await;
        cache.stream_classes().mark_video(1);
        let relay = start_fast_relay_in(
            ParentPath::PromotedSecondary,
            &targets,
            fast_plane::relay::RelayClassConfig {
                port_offset: OFFSET,
                cpus: [None, None],
                realtime: false,
                tee_capacity: fast_plane::relay::DEFAULT_TEE_CAPACITY,
                video_gso: true,
                ..Default::default()
            },
            Arc::clone(&cache),
        )
        .await;
        if let Some(forwarder) = &relay.forwarder {
            forwarder.configure_buffers(32 << 20);
        }
        // The forwarder's cache mesh, serving `children` edges.
        let forwarder_addr = unused_loopback_addr();
        let edge_addrs: Vec<SocketAddr> = (0..children).map(|_| unused_loopback_addr()).collect();
        let mut config = CacheMeshConfig::new("dist-a", "uk", forwarder_addr)
            .with_role(MeshRole::Distributor)
            .with_announce_interval(Duration::from_millis(100))
            .with_socket_buffer_bytes(32 << 20)
            .with_class_port_offset(OFFSET)
            .with_stream_classifier(classify(&cache));
        for addr in &edge_addrs {
            config = config.with_peer(*addr);
        }
        let mut meshes = vec![CacheMesh::new(Arc::clone(&cache.chunk_cache), config)
            .start()
            .await
            .unwrap()];
        let mut edges = Vec::new();
        for addr in &edge_addrs {
            let edge = node().await;
            edges.push(Arc::clone(&edge));
            edge.set_byte_budget(CacheByteBudget {
                playlist: edge_cache_mb * MB,
                node: edge_cache_mb * MB,
            })
            .await;
            edge.stream_classes().mark_video(1);
            let (applied_tx, mut applied_rx) = mpsc::unbounded_channel();
            meshes.push(
                CacheMesh::new(
                    Arc::clone(&edge.chunk_cache),
                    CacheMeshConfig::new("edge", "uk", *addr)
                        .with_peer(forwarder_addr)
                        .with_role(MeshRole::Edge)
                        .with_announce_interval(Duration::from_millis(100))
                        .with_applied_tx(applied_tx)
                        .with_socket_buffer_bytes(32 << 20)
                        .with_class_port_offset(OFFSET)
                        .with_stream_classifier(classify(&edge)),
                )
                .start()
                .await
                .unwrap(),
            );
            tokio::spawn(async move {
                while let Some(applied) = applied_rx.recv().await {
                    let _ = edge
                        .observe_mesh_part(applied.stream_id, applied.slot_id, None)
                        .await;
                }
            });
        }
        (relay, compiled, meshes, edges)
    });
    // Drain the compiled children.
    let stop = Arc::new(AtomicBool::new(false));
    let drains: Vec<_> = compiled
        .into_iter()
        .map(|(audio, video)| {
            let stop = Arc::clone(&stop);
            let video = video.into_std().unwrap();
            let _keep = audio.into_std().unwrap();
            std::thread::spawn(move || {
                let _keep = _keep;
                video.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
                let mut buf = vec![0_u8; 65_536];
                let mut bytes = 0_u64;
                while !stop.load(Ordering::Relaxed) {
                    if let Ok(n) = video.recv(&mut buf) {
                        bytes += n as u64;
                    }
                }
                bytes
            })
        })
        .collect();
    let shape = FrameShape {
        fps: FPS,
        symbol_bytes: 1_316,
        repair_per_sources: 20,
    };
    let bits_per_second = mbps * 1_000_000;
    let fd = relay.secondary_sender.as_raw_fd();
    let _ = socket2::SockRef::from(&relay.secondary_sender).set_send_buffer_size(32 << 20);
    let target = relay.secondary_video.unwrap();
    println!(
        "whole forwarder bench: {mbps} Mbit/s, {children} compiled + {children} mesh children, {seconds} s, trim every {trim_every} s, mmap threshold {} (start RSS {start_rss} MB)",
        if mmap_kb > 0 { format!("{mmap_kb} KiB") } else { "32 MiB".to_owned() }
    );
    println!("     s  rss_mb  fwd_cache_mb  edge_caches_mb  live_mb(<4K/<64K/<1M/>=1M)  arenas  arena_sys_mb  arena_free_mb  largest_arena_mb  trimmed_mb");
    // MEM_GUARD=1: the node's guard, ticking once a second on its own
    // thread, trims for the cache's releases and for the heap's growth.
    let guard_trimmed = Arc::new(AtomicU64::new(0));
    let guard_thread = (env("MEM_GUARD", 0) == 1).then(|| {
        let stop = Arc::clone(&stop);
        let trimmed = Arc::clone(&guard_trimmed);
        let mut caches: Vec<Arc<LiveTsCache>> = edges.clone();
        caches.push(Arc::clone(&relay.cache));
        std::thread::spawn(move || {
            let guard = av_mesh::mem_guard::Guard::new(0, 0);
            let fixed: u64 = caches.iter().map(|cache| cache.ring_index_bytes()).sum::<u64>()
                + av_mesh::memory::locked_bytes();
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_secs(1));
                let held: u64 = caches.iter().map(|cache| cache.chunk_cache.held_bytes()).sum();
                let tick = guard.tick_with_fixed(av_mesh::mem_guard::rss_bytes(), held, fixed);
                if tick.trim {
                    let result = av_mesh::mem_guard::trim_measured();
                    trimmed.fetch_add(result.returned_bytes, Ordering::Relaxed);
                    guard.note_trim_measured(result);
                }
            }
            guard.snapshot(&Default::default(), &Default::default())
        })
    });
    let start = Instant::now() + Duration::from_millis(50);
    let mut trimmed = 0;
    let mut last_held = 0;
    for batch in 0..seconds / 10 {
        let frames = make_frames_shaped(1, batch * 10 * FPS, (10 * FPS) as usize, bits_per_second, shape);
        let at = start + Duration::from_secs(batch * 10);
        send_frames_at(fd, target, &frames, bits_per_second, Spread::Paced, at, FPS, 1_316);
        drop(frames);
        let held = relay.cache.chunk_cache.held_bytes();
        if last_held >= held + av_mesh::mem_guard::TRIM_AFTER_RELEASE_BYTES {
            trimmed += av_mesh::mem_guard::trim();
            last_held = held;
        }
        last_held = last_held.max(held);
        if trim_every > 0 && ((batch + 1) * 10).is_multiple_of(trim_every) {
            trimmed += av_mesh::mem_guard::trim();
        }
        let live = alloc_count::live_bytes_by_class();
        let (arenas, system, free, largest) = arena_stats();
        let edge_held: u64 = edges.iter().map(|edge| edge.chunk_cache.held_bytes()).sum();
        println!(
            "  {:4}  {:6}  {:12}  {:14}  {}/{}/{}/{}  {:6}  {:12}  {:13}  {:16}  {:10}",
            (batch + 1) * 10,
            rss(),
            held / MB,
            edge_held / MB,
            live[0] / MB,
            live[1] / MB,
            live[2] / MB,
            live[3] / MB,
            arenas,
            system / MB,
            free / MB,
            largest / MB,
            (trimmed + guard_trimmed.load(Ordering::Relaxed)) / MB,
        );
    }
    std::thread::sleep(Duration::from_millis(500));
    let before = rss();
    let (_, _, free_before, _) = arena_stats();
    let returned = av_mesh::mem_guard::trim();
    let (_, _, free_after, _) = arena_stats();
    println!(
        "whole forwarder bench: end RSS {before} MB; a trim returned {} MB (arena free {} -> {} MB), RSS {} MB; relay {:?}",
        returned / MB,
        free_before / MB,
        free_after / MB,
        rss(),
        relay.stats().tee_dropped,
    );
    stop.store(true, Ordering::Relaxed);
    if let Some(guard) = guard_thread {
        let snapshot = guard.join().unwrap();
        println!(
            "whole forwarder bench: guard trims {} ({} for growth), last heap free {} -> {} MB",
            snapshot.trims,
            snapshot.growth_trims,
            snapshot.heap_free_before_trim_bytes / MB,
            snapshot.heap_free_after_trim_bytes / MB
        );
    }
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: getrusage writes the struct it is given.
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    println!(
        "whole forwarder bench: process CPU {:.1} s, minor faults {}",
        usage.ru_utime.tv_sec as f64 + usage.ru_stime.tv_sec as f64,
        usage.ru_minflt
    );
    let forwarded: u64 = drains.into_iter().map(|drain| drain.join().unwrap()).sum();
    println!("whole forwarder bench: forwarded {} MB to the compiled children", forwarded / MB);
    for mesh in meshes {
        mesh.shutdown();
    }
    rt.block_on(relay.stop());
}
