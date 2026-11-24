//! The cost of an edge per part (low-latency wave 4, C), counted.
//!
//! Two distributors publish a stream of 1 ms PCM parts (1,000 parts a
//! second) over the cache mesh to one edge. The edge serves LL-HLS readers,
//! each with a window of part requests in flight and a playlist reload
//! every 500 ms as `needletail-reader` does, and UDP clients. The edge (its
//! runtime, its blocking pool and its cut-through thread) runs on threads of
//! its own, and the CPU time of those threads is read from
//! `/proc/self/task/*/schedstat` over the measured seconds. The readers' and
//! the clients' own work is on other threads and is not counted.
//!
//! An HTTP request is the router's work only (no TLS, no QUIC): the edge
//! cost that is ours to remove.
//!
//! Ignored: run it with
//! `cargo test --release edge_cost -- --ignored --nocapture`, with
//! EDGE_COST_READERS, EDGE_COST_UDP, EDGE_COST_WINDOW, EDGE_COST_SECONDS and
//! EDGE_COST_PART_US to change the load, and EDGE_COST_OLD=1 for the edge
//! with every wave-4 C option off. EDGE_COST_PTRACER=1 lets eu-stack sample it.

use super::*;
use av_mesh::client_udp::{Control, ObjectKind as UdpObjectKind, ObjectReceiver, RepairPolicy};
use playlists::mesh::{CacheMesh, CacheMeshConfig, CacheMeshRole as MeshRole, ReplicationMode};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// CPU time in nanoseconds of each thread of this process, with its name.
fn threads_cpu_ns() -> BTreeMap<u64, (String, u64)> {
    let mut threads = BTreeMap::new();
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return threads;
    };
    for task in tasks.flatten() {
        let path = task.path();
        let Some(tid) = path
            .file_name()
            .and_then(|name| name.to_str()?.parse::<u64>().ok())
        else {
            continue;
        };
        let Ok(comm) = std::fs::read_to_string(path.join("comm")) else {
            continue;
        };
        let Some(ns) = std::fs::read_to_string(path.join("schedstat"))
            .ok()
            .and_then(|stat| stat.split_whitespace().next()?.parse::<u64>().ok())
        else {
            continue;
        };
        threads.insert(tid, (comm.trim_end().to_owned(), ns));
    }
    threads
}

/// CPU time of the threads between two readings, by the first group whose
/// prefix each thread's name has, and the edge's busiest threads.
fn cpu_between(
    before: &BTreeMap<u64, (String, u64)>,
    after: &BTreeMap<u64, (String, u64)>,
) -> (BTreeMap<&'static str, u64>, Vec<(u64, String, u64)>) {
    let mut by_group = BTreeMap::new();
    let mut edge_threads = Vec::new();
    for (tid, (comm, ns)) in after {
        let spent = ns - before.get(tid).map_or(0, |(_, ns)| *ns);
        let Some((_, group)) = GROUPS.iter().find(|(prefix, _)| comm.starts_with(prefix)) else {
            continue;
        };
        *by_group.entry(*group).or_insert(0) += spent;
        if *group == "edge" && spent > 0 {
            edge_threads.push((*tid, comm.clone(), spent));
        }
    }
    edge_threads.sort_by_key(|(_, _, spent)| std::cmp::Reverse(*spent));
    (by_group, edge_threads)
}

const GROUPS: [(&str, &str); 4] = [
    ("edge-rt", "edge"),
    ("needletail-cut", "edge"),
    ("dist-rt", "distributors"),
    ("client-rt", "clients"),
];

fn runtime(name: &str, workers: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name(name)
        .enable_all()
        .build()
        .unwrap()
}

/// A 1 ms PCM part as the contributor stamps it: 48 stereo 24-bit samples.
fn pcm_part(stream_id: u64, sequence: u64, part_us: u64) -> Bytes {
    let parts_per_segment = 1_000_000 / part_us;
    let payload: Vec<u8> = (0..288_u64)
        .map(|index| (index * 31 + sequence * 7) as u8)
        .collect();
    let key = media_object::ObjectKey::for_payload(
        "default",
        stream_id.to_string(),
        "opaque-audio",
        0,
        0,
        sequence,
        1,
        &payload,
    )
    .unwrap();
    let metadata = [
        ("duration-ms", (part_us / 1_000).max(1).to_string()),
        ("duration-us", part_us.to_string()),
        ("hls-msn", (sequence / parts_per_segment).to_string()),
        ("hls-part", (sequence % parts_per_segment).to_string()),
        ("hls-part-target-ms", (part_us / 1_000).max(1).to_string()),
        ("hls-part-target-us", part_us.to_string()),
        ("hls-target-duration-ms", "1000".to_string()),
        ("hls-target-duration-us", "1000000".to_string()),
    ];
    let mut builder = MediaObject::builder(key, ObjectKind::Media, payload)
        .with_metadata("container", b"opaque".to_vec())
        .with_metadata("payload-format", b"opaque-part-v1".to_vec())
        .with_metadata("content-type", b"application/octet-stream".to_vec())
        .with_metadata("file-extension", b"bin".to_vec());
    for (name, value) in metadata {
        builder = builder.with_metadata(name, value.into_bytes());
    }
    Bytes::from(media_object::encode(&builder.build().unwrap()).unwrap())
}

struct Bench {
    /// Every wave-4 C option off: the edge from before.
    old: bool,
    /// The cut-through on a thread of its own (--client-udp-cut-through-thread).
    cut_thread: bool,
    readers: usize,
    udp_clients: usize,
    window: usize,
    seconds: u64,
    part_us: u64,
}

struct Measured {
    edge_cpu: f64,
    distributor_cpu: f64,
    parts_published: u64,
    http_parts: u64,
    playlists: u64,
    udp_parts: u64,
    udp_datagrams: u64,
}

fn run_bench(bench: &Bench) -> Measured {
    let edge_rt = runtime("edge-rt", 2);
    let dist_rt = runtime("dist-rt", 2);
    let client_rt = runtime("client-rt", 4);
    let stream_id = 4001_u64;
    let part_us = bench.part_us;
    let (old, cut_thread) = (bench.old, bench.cut_thread);
    let edge_handle = edge_rt.handle().clone();
    let client_handle = client_rt.handle().clone();

    dist_rt.block_on(async move {
        // Two distributors, parents of the edge, as in a region.
        let edge_addr = unused_loopback_addr();
        let mut parents = Vec::new();
        for name in ["dist-a", "dist-b"] {
            let addr = unused_loopback_addr();
            let cache =
                LiveTsCache::with_ring(1, Duration::from_micros(2_500), 400, 12_000, 64, 8, 0)
                    .await;
            let mesh = CacheMesh::new(
                Arc::clone(&cache.chunk_cache),
                CacheMeshConfig::new(name, "uk", addr)
                    .with_peer(edge_addr)
                    .with_role(MeshRole::Distributor)
                    .with_announce_interval(Duration::from_millis(100)),
            )
            .start()
            .await
            .unwrap();
            parents.push((addr, cache, mesh));
        }
        let parent_addrs: Vec<SocketAddr> = parents.iter().map(|(addr, ..)| *addr).collect();

        // The edge, on its own runtime.
        let (router, udp_addr, edge_stats, shutdown_tx) = edge_handle
            .spawn(async move {
                let cache =
                    LiveTsCache::with_ring(1, Duration::from_micros(2_500), 400, 12_000, 64, 8, 0)
                        .await;
                cache.set_hop_journal(Arc::new(HopJournal::new("edge", 262_144)));
                cache.set_playlist_scan_memo(!old);
                let cut = Arc::new(edge_udp::CutThrough::default());
                let observer: Arc<dyn playlists::mesh::CacheMeshReceiveObserver> = if cut_thread {
                    edge_udp::CutThroughLane::start(
                        Arc::clone(&cut),
                        "audio",
                        tokio::runtime::Handle::current(),
                    )
                    .unwrap()
                } else {
                    Arc::clone(&cut) as _
                };
                let (applied_tx, applied_rx) = mpsc::unbounded_channel();
                let mesh = Arc::new(
                    CacheMesh::new(
                        Arc::clone(&cache.chunk_cache),
                        CacheMeshConfig::new("edge-1", "uk", edge_addr)
                            .with_peers(parent_addrs)
                            .with_role(MeshRole::Edge)
                            .with_replication(ReplicationMode::Demand)
                            .with_announce_interval(Duration::from_millis(100))
                            .with_applied_tx(applied_tx)
                            .with_receive_observer(observer),
                    )
                    .start()
                    .await
                    .unwrap(),
                );
                let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh))
                    .with_playlist_demand(PlaylistDemand::new(PlaylistDemandConfig {
                        enabled: true,
                        lease: Duration::from_secs(5),
                        idle: Duration::from_secs(30),
                        warm_parts: 64,
                        max_playlists: MAX_EDGE_PLAYLISTS,
                    }))
                    .with_parents_silent_after(Some(Duration::from_millis(1_500)))
                    .with_liveness_cache(if old {
                        0
                    } else {
                        DEFAULT_EDGE_LIVENESS_CACHE_US
                    })
                    .with_part_wait_due(!old);
                let (shutdown_tx, shutdown_rx) = watch::channel(());
                tokio::spawn(run_mesh_applied_parts(
                    Arc::clone(&cache),
                    router.edge_load.clone(),
                    applied_rx,
                    None,
                    shutdown_rx.clone(),
                ));
                let sockets =
                    edge_udp::EdgeSockets::single(UdpSocket::bind("127.0.0.1:0").await.unwrap());
                let udp_addr = sockets.for_stream(0).local_addr().unwrap();
                let stats: Arc<edge_udp::EdgeUdpStats> = Arc::default();
                let mut config = edge_udp::EdgeUdpConfig::with_defaults(1_200, 64);
                if old {
                    config.batch_send = false;
                    config.inline_complete_bytes = 0;
                    config.gso = false;
                }
                tokio::spawn(edge_udp::run_edge_udp(
                    sockets,
                    router.clone(),
                    config,
                    Arc::clone(&stats),
                    vec![cut],
                    shutdown_rx,
                ));
                (router, udp_addr, stats, shutdown_tx)
            })
            .await
            .unwrap();

        // The publisher: both distributors commit each part at its time.
        let published = Arc::new(AtomicU64::new(0));
        let running = Arc::new(AtomicBool::new(true));
        let publisher = {
            let caches: Vec<Arc<LiveTsCache>> = parents
                .iter()
                .map(|(_, cache, _)| Arc::clone(cache))
                .collect();
            let published = Arc::clone(&published);
            let running = Arc::clone(&running);
            tokio::spawn(async move {
                let start = Instant::now();
                let mut sequence = 0_u64;
                while running.load(Ordering::Relaxed) {
                    tokio::time::sleep_until(
                        (start + Duration::from_micros(part_us * sequence)).into(),
                    )
                    .await;
                    let part = pcm_part(stream_id, sequence, part_us);
                    for cache in &caches {
                        cache
                            .commit_stream_payload(stream_id, part.clone())
                            .await
                            .unwrap();
                    }
                    published.store(sequence, Ordering::Relaxed);
                    sequence += 1;
                }
            })
        };

        // The first reader request subscribes the edge to the playlist.
        let first = {
            let router = router.clone();
            edge_handle
                .spawn(async move {
                    get_for_tests(&router, &format!("/live/{stream_id}/stream.m3u8?sid=warm")).await
                })
                .await
                .unwrap()
        };
        assert_eq!(first.status, StatusCode::OK);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some((_, last)) = router.cache.stream_position_for_id(stream_id).await {
                    if last > 100 {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the edge receives the stream");

        // LL-HLS readers: a window of part requests in flight, each served
        // on the edge runtime as the HTTP server serves it, and a playlist
        // reload every 500 ms.
        let http_parts = Arc::new(AtomicU64::new(0));
        let playlists = Arc::new(AtomicU64::new(0));
        let mut readers = Vec::new();
        for reader in 0..bench.readers {
            let router = Arc::new(router.clone());
            let edge = edge_handle.clone();
            let http_parts = Arc::clone(&http_parts);
            let playlists = Arc::clone(&playlists);
            let running = Arc::clone(&running);
            let window = bench.window;
            let start_at = published.load(Ordering::Relaxed);
            readers.push(client_handle.spawn(async move {
                let request = |path: String| {
                    let router = Arc::clone(&router);
                    edge.spawn(async move { get_for_tests(&router, &path).await })
                };
                let mut inflight = VecDeque::new();
                let mut next = start_at;
                let mut reload_at = Instant::now();
                while running.load(Ordering::Relaxed) {
                    while inflight.len() < window {
                        inflight.push_back(request(format!(
                            "/live/{stream_id}/part{next}.bin?sid=reader-{reader}"
                        )));
                        next += 1;
                    }
                    if Instant::now() >= reload_at {
                        reload_at += Duration::from_millis(500);
                        let reload =
                            request(format!("/live/{stream_id}/stream.m3u8?sid=reader-{reader}"));
                        let playlists = Arc::clone(&playlists);
                        tokio::spawn(async move {
                            if reload
                                .await
                                .is_ok_and(|response| response.status == StatusCode::OK)
                            {
                                playlists.fetch_add(1, Ordering::Relaxed);
                            }
                        });
                    }
                    let response = inflight.pop_front().unwrap().await.unwrap();
                    if response.status == StatusCode::OK {
                        http_parts.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        // UDP clients.
        let udp_parts = Arc::new(AtomicU64::new(0));
        let mut clients = Vec::new();
        for _ in 0..bench.udp_clients {
            let udp_parts = Arc::clone(&udp_parts);
            let running = Arc::clone(&running);
            clients.push(client_handle.spawn(async move {
                let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
                let subscribe = |token| {
                    Control::Subscribe {
                        stream_id,
                        from_part: None,
                        repair: RepairPolicy::default(),
                        token,
                        fast_lane: false,
                        compact: None,
                        others: Vec::new(),
                        resume: false,
                    }
                    .encode()
                };
                let mut buf = vec![0_u8; 65_536];
                client.send_to(&subscribe(None), udp_addr).await.unwrap();
                let token = loop {
                    let (len, _) = client.recv_from(&mut buf).await.unwrap();
                    if let Some(Control::Token { token, .. }) = Control::decode(&buf[..len]) {
                        break token;
                    }
                };
                let mut receiver = ObjectReceiver::new();
                let mut refreshed = Instant::now();
                client
                    .send_to(&subscribe(Some(token)), udp_addr)
                    .await
                    .unwrap();
                while running.load(Ordering::Relaxed) {
                    if refreshed.elapsed() >= Duration::from_secs(1) {
                        refreshed = Instant::now();
                        let _ = client.send_to(&subscribe(Some(token)), udp_addr).await;
                    }
                    let Ok(Ok((len, _))) = tokio::time::timeout(
                        Duration::from_millis(100),
                        client.recv_from(&mut buf),
                    )
                    .await
                    else {
                        continue;
                    };
                    if let Some(object) = receiver.push(av_mesh::client_compact::LegId::FIRST, &buf[..len]) {
                        if object.kind == UdpObjectKind::Part {
                            udp_parts.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }));
        }

        // Settle, then measure.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let before = threads_cpu_ns();
        let published_before = published.load(Ordering::Relaxed);
        let http_before = http_parts.load(Ordering::Relaxed);
        let playlists_before = playlists.load(Ordering::Relaxed);
        let udp_before = udp_parts.load(Ordering::Relaxed);
        // Every datagram sent counts in `datagrams` (a cut-through one in
        // `cut_through_datagrams` as well).
        let datagrams_before = edge_stats.datagrams.load(Ordering::Relaxed);
        let scan_reads_before = router.cache.playlist_scan_reads.load(Ordering::Relaxed);
        let started = Instant::now();
        tokio::time::sleep(Duration::from_secs(bench.seconds)).await;
        println!(
            "  playlist slots read: {}",
            router.cache.playlist_scan_reads.load(Ordering::Relaxed) - scan_reads_before
        );
        let elapsed_ns = started.elapsed().as_nanos() as f64;
        let (after, edge_threads) = cpu_between(&before, &threads_cpu_ns());
        for (tid, name, spent) in edge_threads.iter().take(8) {
            println!(
                "  edge thread {tid} {name}: {:.1}%",
                *spent as f64 * 100.0 / elapsed_ns
            );
        }
        println!("  edge threads that ran: {}", edge_threads.len());
        let measured = Measured {
            edge_cpu: after.get("edge").copied().unwrap_or(0) as f64 / elapsed_ns,
            distributor_cpu: after.get("distributors").copied().unwrap_or(0) as f64 / elapsed_ns,
            parts_published: published.load(Ordering::Relaxed) - published_before,
            http_parts: http_parts.load(Ordering::Relaxed) - http_before,
            playlists: playlists.load(Ordering::Relaxed) - playlists_before,
            udp_parts: udp_parts.load(Ordering::Relaxed) - udp_before,
            udp_datagrams: edge_stats.datagrams.load(Ordering::Relaxed) - datagrams_before,
        };
        for _ in 0..3 {
            let reads = router.cache.playlist_scan_reads.load(Ordering::Relaxed);
            router.cache.playlist_for_stream_id(stream_id).await;
            println!(
                "  one render read {} slots",
                router.cache.playlist_scan_reads.load(Ordering::Relaxed) - reads
            );
        }
        running.store(false, Ordering::Relaxed);
        let _ = publisher.await;
        for reader in readers {
            reader.abort();
        }
        for client in clients {
            client.abort();
        }
        let _ = shutdown_tx.send(());
        for (_, _, mesh) in parents {
            mesh.shutdown();
        }
        measured
    })
}

#[test]
#[ignore = "a measurement: run it alone, in release"]
fn edge_cost_per_part() {
    if std::env::var_os("EDGE_COST_PTRACER").is_some() {
        // Let a sampling profiler that is not this process's parent (such
        // as eu-stack) read the stacks of its threads.
        // SAFETY: prctl with integer arguments only.
        unsafe { libc::prctl(libc::PR_SET_PTRACER, libc::PR_SET_PTRACER_ANY, 0, 0, 0) };
    }
    let bench = Bench {
        old: env_u64("EDGE_COST_OLD", 0) == 1,
        cut_thread: env_u64("EDGE_COST_CUT_THREAD", 0) == 1,
        readers: env_u64("EDGE_COST_READERS", 25) as usize,
        udp_clients: env_u64("EDGE_COST_UDP", 25) as usize,
        window: env_u64("EDGE_COST_WINDOW", 40) as usize,
        seconds: env_u64("EDGE_COST_SECONDS", 5),
        part_us: env_u64("EDGE_COST_PART_US", 1_000),
    };
    let measured = run_bench(&bench);
    let seconds = bench.seconds as f64;
    let parts_per_second = measured.parts_published as f64 / seconds;
    println!(
        "edge cost{}: {} readers (window {}), {} UDP clients, {:.0} parts/s: \
         edge {:.1}% of a CPU ({:.1} us a part), distributors {:.1}%; \
         HTTP parts {:.0}/s ({:.0}% of due), playlists {:.0}/s, \
         UDP parts {:.0}/s ({:.0}% of due), UDP datagrams {:.0}/s",
        if bench.old { " (old)" } else { "" },
        bench.readers,
        bench.window,
        bench.udp_clients,
        parts_per_second,
        measured.edge_cpu * 100.0,
        measured.edge_cpu * 1e6 / parts_per_second.max(1.0),
        measured.distributor_cpu * 100.0,
        measured.http_parts as f64 / seconds,
        measured.http_parts as f64 * 100.0
            / (parts_per_second * seconds * bench.readers as f64).max(1.0),
        measured.playlists as f64 / seconds,
        measured.udp_parts as f64 / seconds,
        measured.udp_parts as f64 * 100.0
            / (parts_per_second * seconds * bench.udp_clients as f64).max(1.0),
        measured.udp_datagrams as f64 / seconds,
    );
}

/// A 1 ms stream with the deploy's window: 12,000 parts of 1 ms on a node
/// of 2.5 ms parts.
async fn millisecond_cache(memo: bool) -> Arc<LiveTsCache> {
    let cache =
        LiveTsCache::with_ring(1, Duration::from_micros(2_500), 400, 12_000, 64, 4, 0).await;
    cache.set_playlist_scan_memo(memo);
    cache
}

async fn commit_pcm_parts(cache: &LiveTsCache, stream_id: u64, parts: std::ops::Range<u64>) {
    for sequence in parts {
        cache
            .commit_stream_payload(stream_id, pcm_part(stream_id, sequence, 1_000))
            .await
            .unwrap();
    }
}

/// A playlist without its programme date and time, which is when the parts
/// reached the cache when they carry no publication time.
fn undated(playlist: &str) -> String {
    playlist
        .lines()
        .filter(|line| !line.starts_with("#EXT-X-PROGRAM-DATE-TIME:"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
async fn a_playlist_rendered_from_the_scan_memo_is_the_playlist_rendered_from_every_slot() {
    let stream_id = 4001;
    let memo = millisecond_cache(true).await;
    let full = millisecond_cache(false).await;
    for (from, to) in [
        (0, 12_500),
        (12_500, 12_501),
        (12_501, 12_640),
        (12_640, 14_100),
    ] {
        commit_pcm_parts(&memo, stream_id, from..to).await;
        commit_pcm_parts(&full, stream_id, from..to).await;
        let rendered = memo.playlist_for_stream_id(stream_id).await;
        assert_eq!(
            undated(&rendered),
            undated(&full.playlist_for_stream_id(stream_id).await)
        );
        assert!(rendered.contains(&format!("part{}.bin", to - 1)), "{to}");
        // Again from the memo alone.
        memo.invalidate_cached_playlist(stream_id);
        assert_eq!(memo.playlist_for_stream_id(stream_id).await, rendered);
    }

    // A slot written again is read again: a part replaced by another
    // stream's part, which is no media of this one, leaves the playlist, as
    // it does from a full render.
    let hole = 14_050_usize;
    for cache in [&memo, &full] {
        cache
            .chunk_cache
            .add_for_stream_id(stream_id, hole, pcm_part(stream_id + 1, hole as u64, 1_000))
            .await
            .unwrap();
        cache.invalidate_cached_playlist(stream_id);
    }
    let rendered = memo.playlist_for_stream_id(stream_id).await;
    assert_eq!(
        undated(&rendered),
        undated(&full.playlist_for_stream_id(stream_id).await)
    );
    assert!(!rendered.contains(&format!("\"part{hole}.bin\"")));
    // A slot released (a 4K stream's ring is trimmed so) leaves it too.
    let released = 14_060_usize;
    for cache in [&memo, &full] {
        assert!(
            cache
                .chunk_cache
                .clear_for_stream_id(stream_id, released)
                .await
        );
        cache.invalidate_cached_playlist(stream_id);
    }
    let rendered = memo.playlist_for_stream_id(stream_id).await;
    assert_eq!(
        undated(&rendered),
        undated(&full.playlist_for_stream_id(stream_id).await)
    );
    assert!(!rendered.contains(&format!("\"part{released}.bin\"")));
}

#[tokio::test]
async fn a_playlist_render_after_one_new_part_reads_one_slot_not_the_window() {
    let stream_id = 4001;
    let memo = millisecond_cache(true).await;
    let full = millisecond_cache(false).await;
    commit_pcm_parts(&memo, stream_id, 0..12_500).await;
    commit_pcm_parts(&full, stream_id, 0..12_500).await;
    memo.playlist_for_stream_id(stream_id).await;
    let mut took = Vec::new();
    for cache in [&memo, &full] {
        let mut spent = Duration::ZERO;
        if std::env::var_os("EDGE_COST_PTRACER").is_some() {
            // SAFETY: prctl with integer arguments only.
            unsafe { libc::prctl(libc::PR_SET_PTRACER, libc::PR_SET_PTRACER_ANY, 0, 0, 0) };
        }
        let renders = env_u64("EDGE_COST_RENDERS", 40);
        for sequence in 12_500..12_500 + renders {
            commit_pcm_parts(cache, stream_id, sequence..sequence + 1).await;
            let reads = cache.playlist_scan_reads.load(Ordering::Relaxed);
            let started = Instant::now();
            let rendered = cache.playlist_for_stream_id(stream_id).await;
            spent += started.elapsed();
            if Arc::ptr_eq(cache, &memo) {
                // The new part, and nothing else.
                assert_eq!(cache.playlist_scan_reads.load(Ordering::Relaxed) - reads, 1);
            }
            assert!(rendered.contains(&format!("part{sequence}.bin")));
        }
        took.push(spent / renders as u32);
    }
    println!(
        "playlist render of a 12,000-part window: memo {:?}, every slot {:?}",
        took[0], took[1]
    );
    // The times are for the record (the runner's other tests share the
    // host): the count of slots read is the test.
}

#[test]
fn a_part_request_ahead_of_the_stream_waits_until_the_part_is_due() {
    let grace = edge_hole_repair_grace(1_000);
    let slice = Duration::from_millis(EDGE_PART_WAIT_SLICE_MS);
    assert_eq!(grace, Duration::from_millis(3));
    // Forty parts of 1 ms ahead of the newest part: due in 40 ms, looked
    // at again 3 ms after that, not every 3 ms.
    assert_eq!(
        part_wait_until_due(Some(1_000), 1_040, 1_000, grace, slice),
        Duration::from_millis(43)
    );
    // Far ahead: a slice at a time, so that lost parents are noticed.
    assert_eq!(
        part_wait_until_due(Some(1_000), 9_000, 1_000, grace, slice),
        slice
    );
    // The next part, a hole behind the newest part, and a stream with no
    // part yet: the grace, as before.
    assert_eq!(
        part_wait_until_due(Some(1_000), 1_001, 1_000, grace, slice),
        Duration::from_millis(4)
    );
    assert_eq!(
        part_wait_until_due(Some(1_000), 990, 1_000, grace, slice),
        grace
    );
    assert_eq!(
        part_wait_until_due(Some(1_000), 1_000, 1_000, grace, slice),
        grace
    );
    assert_eq!(part_wait_until_due(None, 5, 1_000, grace, slice), grace);
    // A stream whose part is not known yet counts parts of 1 us.
    assert_eq!(
        part_wait_until_due(Some(1_000), 1_001, 0, grace, slice),
        Duration::from_micros(3_001)
    );
}

#[test]
fn the_answer_to_whether_the_parents_are_lost_is_kept_for_its_ttl_only() {
    let cache = LivenessCache::new(5_000);
    assert_eq!(cache.get(1_000_000), None);
    cache.set(1_000_000, true);
    assert_eq!(cache.get(1_000_000), Some(true));
    assert_eq!(cache.get(1_004_999), Some(true));
    assert_eq!(cache.get(1_005_000), None);
    cache.set(1_005_000, false);
    assert_eq!(cache.get(1_006_000), Some(false));
    // A TTL of zero asks every time.
    let never = LivenessCache::new(0);
    never.set(1_000_000, true);
    assert_eq!(never.get(1_000_000), None);
}

/// A request for a part 40 parts ahead of a 1 ms stream is answered when
/// the part arrives, whether it waits until the part is due or looks again
/// every hole repair grace.
#[tokio::test]
async fn a_part_request_ahead_of_the_stream_is_answered_on_arrival() {
    let cache = millisecond_cache(true).await;
    let stream_id = 4001;
    commit_pcm_parts(&cache, stream_id, 0..100).await;
    let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
    for due in [true, false] {
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh))
            .with_parents_silent_after(Some(Duration::from_millis(1_500)))
            .with_part_wait_due(due);
        let head = cache.stream_head(stream_id).unwrap();
        let wanted = head + 40;
        let request = {
            let router = router.clone();
            tokio::spawn(async move { router.get_part_repairing_holes(stream_id, wanted).await })
        };
        // The stream goes on at its pace.
        for sequence in head + 1..=wanted {
            tokio::time::sleep(Duration::from_millis(1)).await;
            commit_pcm_parts(&cache, stream_id, sequence..sequence + 1).await;
        }
        let answered = tokio::time::timeout(Duration::from_secs(2), request)
            .await
            .expect("answered when the part arrives")
            .unwrap();
        assert!(matches!(answered, Ok(Some(_))), "due {due}");
    }
}

#[test]
fn the_edge_cost_flags_default_to_the_new_behaviour_and_can_be_turned_off() {
    use clap::Parser;
    let args = Args::try_parse_from(["av-mesh"]).unwrap();
    assert!(args.edge_part_wait_due);
    assert_eq!(args.edge_liveness_cache_us, 5_000);
    assert!(args.playlist_scan_memo);
    assert!(args.client_udp_batch_send);
    assert_eq!(args.client_udp_inline_complete_bytes, 16_384);
    assert!(args.client_udp_gso);
    let args = Args::try_parse_from([
        "av-mesh",
        "--edge-part-wait-due=false",
        "--edge-liveness-cache-us",
        "0",
        "--playlist-scan-memo=false",
        "--client-udp-batch-send=false",
        "--client-udp-inline-complete-bytes",
        "0",
        "--client-udp-gso=false",
    ])
    .unwrap();
    assert!(!args.edge_part_wait_due);
    assert_eq!(args.edge_liveness_cache_us, 0);
    assert!(!args.playlist_scan_memo);
    assert!(!args.client_udp_batch_send);
    assert_eq!(args.client_udp_inline_complete_bytes, 0);
    assert!(!args.client_udp_gso);
}
