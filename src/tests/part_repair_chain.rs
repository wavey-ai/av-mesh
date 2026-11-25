//! 4k-repair bench, in process: a 4K stream at the referee's 247 Mbit/s
//! clip (clips6 2160p240: 24 fps, GOP 48, its first frame sizes, with two
//! loop-point frames of 6 and 3.5 MB) through contributor -> forwarder ->
//! long-haul forwarder -> edge -> viewer, each node an av-mesh fast-plane
//! node (class threads, tee, part repair), each link an impaired UDP proxy
//! (random loss, Gilbert-Elliott bursts, outages; a one-way delay of half
//! the round trip, both ways).
//!
//! "contributor" is an av-mesh node fed over loopback by the test's source,
//! so that its answers and its link repair to the first forwarder are the
//! code that av-mesh runs (av-contrib's own sizing is tested there). The
//! viewer is a fast-plane leaf node (the client lane is av-mesh's client
//! UDP code, not measured here).
//!
//! Links: contributor -> forwarder and long-haul -> edge are in-region
//! (1 ms round trip); forwarder -> long-haul takes the round trip of the
//! case; edge -> viewer the case's up to 30 ms. Every link takes the case's
//! loss, in both directions, each its own random stream; outages come on
//! every link, each link's at its own times.
//!
//! The measurement is ignored: `cargo test --release part_repair_chain --
//! --ignored --nocapture --test-threads 1` (KR_RTTS, KR_CASES, KR_SECONDS).

use super::video_throughput::frame_envelope;
use super::*;
use fast_plane::part_repair::{PartRepair, PartRepairConfig, PartRepairSnapshot};
use socket2::SockRef;
use std::os::fd::AsRawFd;

const OFFSET: u16 = 1_000;
const SYMBOL: u16 = 1_264;
const FPS: u64 = 24;
const STREAM: u64 = 1;
/// The source sends each frame in one burst at the contributor's speed.
const SOURCE_BITS_PER_SECOND: u64 = 550_000_000;

/// Frame sizes (bytes) of clips6 2160p240 (247.4 Mbit/s over its 1,536
/// frames; these first 480: 250.7 Mbit/s), from the referee's index.
static CLIP: [u32; 480] = [
    1_955_493, 1_148_603, 1_022_034, 1_045_374, 1_185_323, 962_654, 947_791, 1_046_390, 1_079_489, 1_103_766, 1_172_838, 1_123_440,
    1_261_579, 1_307_127, 1_202_182, 1_204_859, 1_260_310, 1_266_058, 1_205_038, 1_184_805, 1_006_983, 2_239_418, 1_024_603, 1_218_778,
    1_127_241, 1_246_119, 1_130_123, 1_228_837, 1_063_881, 1_209_011, 1_091_677, 1_230_796, 1_093_852, 1_263_225, 1_067_546, 1_325_026,
    1_184_182, 1_300_871, 1_253_160, 1_436_850, 1_250_779, 1_357_470, 1_340_482, 1_479_591, 1_312_371, 1_406_292, 1_235_115, 1_368_515,
    2_745_694, 1_185_633, 1_008_269, 1_196_856, 1_153_870, 1_294_293, 1_187_919, 1_268_889, 1_154_826, 1_316_842, 1_157_068, 1_297_771,
    1_428_710, 1_551_854, 1_452_843, 1_474_489, 1_363_401, 1_251_790, 1_114_730, 1_142_094, 1_192_490, 1_162_168, 1_208_257, 1_206_654,
    1_226_869, 1_199_900, 1_313_379, 1_189_544, 1_244_908, 1_255_079, 1_255_597, 1_232_543, 1_285_526, 1_188_324, 1_209_300, 1_195_328,
    1_220_604, 1_268_175, 1_243_623, 1_245_232, 1_281_014, 1_266_053, 1_274_841, 1_337_396, 1_253_389, 1_336_108, 1_272_957, 1_281_425,
    1_857_565, 1_367_889, 1_270_175, 1_288_274, 1_299_871, 1_298_380, 1_254_125, 1_259_358, 1_209_659, 1_220_615, 1_247_157, 1_293_074,
    1_378_023, 1_177_584, 1_229_784, 1_336_749, 1_278_268, 1_274_409, 1_215_454, 1_224_234, 1_139_229, 1_199_519, 1_325_239, 1_234_864,
    1_319_378, 1_306_187, 1_246_671, 1_272_247, 1_296_387, 1_346_140, 1_191_665, 1_319_874, 1_281_443, 1_258_592, 1_277_908, 1_281_888,
    1_317_548, 1_308_716, 1_322_683, 1_267_303, 1_225_375, 1_230_420, 1_279_087, 1_301_009, 1_287_853, 1_283_696, 1_368_508, 1_207_054,
    2_079_456, 1_330_556, 1_306_278, 1_295_274, 1_302_568, 1_322_390, 1_363_175, 1_374_724, 1_373_508, 1_329_552, 1_363_526, 1_364_895,
    1_318_598, 1_365_367, 1_322_792, 1_312_435, 1_300_619, 1_125_160, 1_202_651, 1_223_021, 1_237_043, 1_216_591, 1_242_023, 1_220_193,
    1_291_341, 1_319_560, 1_324_343, 1_263_017, 1_292_624, 1_243_721, 1_274_025, 1_239_742, 1_187_798, 1_227_629, 1_218_457, 1_225_503,
    1_288_125, 1_349_349, 1_326_286, 1_314_484, 1_305_529, 1_451_795, 1_248_398, 1_331_248, 1_271_271, 1_018_774, 1_130_951, 1_025_878,
    1_428_879, 798_227, 1_517_992, 1_154_112, 1_311_684, 950_029, 1_823_191, 1_323_987, 1_506_953, 960_054, 1_834_725, 1_231_952,
    1_378_729, 985_087, 1_003_782, 1_049_025, 1_133_711, 1_088_109, 1_096_673, 1_219_575, 1_231_912, 1_146_411, 1_215_366, 1_219_930,
    1_273_565, 1_280_764, 1_325_723, 1_368_435, 1_252_301, 1_349_525, 1_369_066, 1_309_984, 1_334_111, 1_307_577, 1_276_675, 1_229_269,
    1_261_637, 1_200_671, 1_248_768, 1_172_367, 1_191_858, 1_206_661, 1_211_646, 1_225_259, 1_167_307, 1_218_454, 1_213_645, 1_243_986,
    1_953_113, 1_276_572, 1_208_185, 1_123_337, 1_085_744, 1_057_254, 1_183_278, 1_183_838, 1_238_440, 1_222_512, 1_239_045, 1_185_876,
    1_201_774, 1_258_651, 2_055_496, 1_332_307, 1_387_371, 1_390_965, 1_430_177, 1_377_674, 1_428_118, 1_442_135, 1_373_230, 1_339_847,
    1_448_184, 1_103_490, 1_270_330, 1_268_512, 1_349_071, 1_284_127, 1_377_194, 1_329_943, 1_387_042, 1_306_100, 1_339_792, 1_324_664,
    1_388_255, 1_390_704, 1_356_714, 1_377_359, 1_310_542, 1_469_276, 1_165_146, 1_263_929, 1_291_365, 1_392_008, 1_328_065, 1_407_797,
    1_588_320, 1_454_806, 1_265_761, 1_364_248, 1_310_884, 1_257_247, 1_226_095, 1_287_854, 1_233_665, 1_356_879, 1_424_063, 1_204_042,
    1_302_329, 1_304_169, 1_313_667, 1_327_365, 1_292_174, 1_301_559, 1_289_073, 1_272_571, 1_289_185, 1_356_886, 1_348_798, 1_315_517,
    1_339_408, 1_327_947, 1_281_650, 1_423_767, 1_204_485, 1_342_907, 1_368_986, 1_335_356, 1_331_682, 1_325_378, 1_306_053, 1_434_219,
    1_285_230, 1_405_908, 1_413_399, 1_378_573, 1_391_776, 1_411_159, 1_355_241, 1_379_371, 1_552_746, 1_200_319, 1_374_961, 1_308_159,
    2_382_820, 1_295_367, 1_355_895, 1_236_595, 1_330_127, 1_229_743, 1_314_372, 1_263_417, 1_322_548, 1_220_835, 1_308_846, 1_251_383,
    1_341_886, 1_165_993, 1_406_739, 1_134_349, 1_381_224, 1_292_253, 1_440_517, 1_338_306, 1_451_132, 1_304_893, 1_329_737, 1_300_471,
    1_398_322, 1_349_277, 1_402_923, 1_308_087, 1_331_042, 1_290_174, 1_348_027, 1_232_135, 1_348_211, 1_727_241, 1_393_047, 1_508_394,
    1_492_006, 1_441_882, 1_449_076, 1_419_335, 1_428_146, 1_368_241, 1_374_122, 1_392_411, 1_378_237, 1_401_348, 1_378_781, 1_344_221,
    1_536_039, 1_312_799, 1_212_392, 1_379_180, 1_360_898, 1_284_419, 1_297_442, 1_280_576, 1_241_706, 1_326_270, 1_395_759, 1_344_197,
    1_364_107, 1_382_988, 1_321_806, 1_394_149, 1_373_156, 1_274_324, 1_475_640, 1_233_193, 1_381_247, 1_340_330, 1_359_966, 1_350_843,
    1_296_556, 1_358_152, 1_366_439, 1_409_237, 1_332_480, 1_291_169, 1_338_896, 1_303_082, 1_315_130, 1_331_201, 1_252_365, 1_480_414,
    1_176_376, 1_342_542, 1_337_021, 1_333_975, 1_301_419, 1_281_169, 1_340_542, 1_332_846, 1_323_414, 1_309_020, 1_343_718, 1_336_925,
    2_023_150, 1_399_816, 1_271_494, 1_291_426, 1_426_866, 1_186_949, 1_293_959, 1_350_288, 1_344_916, 1_349_148, 1_357_346, 1_358_863,
    1_343_678, 1_331_130, 1_362_813, 1_352_930, 1_324_245, 1_327_393, 1_344_331, 1_321_653, 1_233_488, 1_431_810, 1_190_067, 1_349_659,
    1_313_817, 1_318_908, 1_359_777, 1_308_010, 1_308_847, 1_333_421, 1_309_313, 1_297_742, 1_359_709, 1_394_090, 1_317_166, 1_334_321,
    1_302_717, 1_291_875, 1_431_992, 1_181_926, 1_356_641, 1_346_895, 1_343_668, 1_270_150, 1_353_146, 1_335_540, 1_223_191, 1_507_798,];

// ------------------------------------------------------------------ loss

/// What a link does to the datagrams it carries.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum Loss {
    None,
    /// Each datagram lost with this probability.
    Random(f64),
    /// netem's `gemodel p r` (every datagram lost in the bad state).
    Bursts { p: f64, r: f64 },
    /// Nothing passes for `ms`, every `every_ms`, from `phase_ms` on.
    Outage { ms: u64, every_ms: u64 },
}

impl Loss {
    pub(super) fn name(self) -> String {
        match self {
            Self::None => "0".into(),
            Self::Random(p) => format!("{}%", p * 100.0),
            Self::Bursts { p, r } => format!("ge{}/{}", p * 100.0, r * 100.0),
            Self::Outage { ms, .. } => format!("out{ms}"),
        }
    }
}

struct LossState {
    loss: Loss,
    bad: bool,
    rng: u64,
    start: Instant,
    phase_ms: u64,
}

impl LossState {
    fn new(loss: Loss, seed: u64, start: Instant, phase_ms: u64) -> Self {
        Self {
            loss,
            bad: false,
            rng: seed | 1,
            start,
            phase_ms,
        }
    }

    fn uniform(&mut self) -> f64 {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        (self.rng >> 11) as f64 / (1_u64 << 53) as f64
    }

    fn lost(&mut self, now: Instant) -> bool {
        match self.loss {
            Loss::None => false,
            Loss::Random(p) => self.uniform() < p,
            Loss::Bursts { p, r } => {
                self.bad = if self.bad {
                    self.uniform() >= r
                } else {
                    self.uniform() < p
                };
                self.bad
            }
            Loss::Outage { ms, every_ms } => {
                let at = now.saturating_duration_since(self.start).as_millis() as u64;
                at >= self.phase_ms && (at - self.phase_ms) % every_ms < ms
            }
        }
    }
}

// ------------------------------------------------------------------ links

#[derive(Debug, Default)]
pub(super) struct LinkStats {
    /// Datagrams and bytes the parent handed the link, and those dropped.
    pub(super) offered: AtomicU64,
    pub(super) offered_bytes: AtomicU64,
    pub(super) dropped: AtomicU64,
    pub(super) back: AtomicU64,
    pub(super) back_dropped: AtomicU64,
}

/// An impaired link: the parent sends its child's video to `up_video` (its
/// child target is `up_audio`, the class pair), the child takes its
/// parent to be `down`. What the child sends back to `down` goes to the
/// parent from `up_video`.
pub(super) struct Link {
    up_audio: std::net::UdpSocket,
    up_video: std::net::UdpSocket,
    down: std::net::UdpSocket,
    pub(super) stats: Arc<LinkStats>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

fn class_pair_std(offset: u16) -> (std::net::UdpSocket, std::net::UdpSocket) {
    for _ in 0..64 {
        let audio = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let Some(video_port) = audio.local_addr().unwrap().port().checked_add(offset) else {
            continue;
        };
        if let Ok(video) = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, video_port)) {
            return (audio, video);
        }
    }
    panic!("no free port pair");
}

impl Link {
    pub(super) fn bind() -> Self {
        let (up_audio, up_video) = class_pair_std(OFFSET);
        let down = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        for socket in [&up_video, &down] {
            socket.set_nonblocking(true).unwrap();
            let _ = SockRef::from(socket).set_recv_buffer_size(64 << 20);
            let _ = SockRef::from(socket).set_send_buffer_size(64 << 20);
        }
        Self {
            up_audio,
            up_video,
            down,
            stats: Arc::new(LinkStats::default()),
            stop: Arc::new(AtomicBool::new(false)),
            thread: None,
        }
    }

    /// Where the parent sends (its child target).
    pub(super) fn parent_target(&self) -> SocketAddr {
        self.up_audio.local_addr().unwrap()
    }

    /// The child's parent.
    pub(super) fn child_parent(&self) -> SocketAddr {
        self.down.local_addr().unwrap()
    }

    /// Carry datagrams to `child_video`, `one_way` late, through `loss`.
    pub(super) fn start(
        &mut self,
        child_video: SocketAddr,
        one_way: Duration,
        loss: Loss,
        seed: u64,
        start: Instant,
        phase_ms: u64,
    ) {
        let up = self.up_video.try_clone().unwrap();
        let down = self.down.try_clone().unwrap();
        let stats = Arc::clone(&self.stats);
        let stop = Arc::clone(&self.stop);
        self.thread = Some(
            std::thread::Builder::new()
                .name("kr-link".into())
                .spawn(move || {
                    run_link(up, down, child_video, one_way, loss, seed, start, phase_ms, &stats, &stop)
                })
                .unwrap(),
        );
    }

    pub(super) fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_link(
    up: std::net::UdpSocket,
    down: std::net::UdpSocket,
    child_video: SocketAddr,
    one_way: Duration,
    loss: Loss,
    seed: u64,
    start: Instant,
    phase_ms: u64,
    stats: &LinkStats,
    stop: &AtomicBool,
) {
    let mut forward = LossState::new(loss, seed, start, phase_ms);
    let mut back = LossState::new(loss, seed ^ 0x9e37_79b9_7f4a_7c15, start, phase_ms);
    let mut to_child: VecDeque<(Instant, Vec<u8>)> = VecDeque::new();
    let mut to_parent: VecDeque<(Instant, Vec<u8>)> = VecDeque::new();
    let mut parent: Option<SocketAddr> = None;
    let mut buf = vec![0_u8; 65_536];
    let fds = [up.as_raw_fd(), down.as_raw_fd()];
    while !stop.load(Ordering::Relaxed) {
        let now = Instant::now();
        for _ in 0..4_096 {
            let Ok((len, from)) = up.recv_from(&mut buf) else {
                break;
            };
            parent = Some(from);
            stats.offered.fetch_add(1, Ordering::Relaxed);
            stats.offered_bytes.fetch_add(len as u64, Ordering::Relaxed);
            if forward.lost(now) {
                stats.dropped.fetch_add(1, Ordering::Relaxed);
            } else {
                to_child.push_back((now + one_way, buf[..len].to_vec()));
            }
        }
        for _ in 0..4_096 {
            let Ok((len, _)) = down.recv_from(&mut buf) else {
                break;
            };
            stats.back.fetch_add(1, Ordering::Relaxed);
            if back.lost(now) {
                stats.back_dropped.fetch_add(1, Ordering::Relaxed);
            } else {
                to_parent.push_back((now + one_way, buf[..len].to_vec()));
            }
        }
        let now = Instant::now();
        while to_child.front().is_some_and(|(due, _)| *due <= now) {
            let (_, datagram) = to_child.pop_front().unwrap();
            // A full socket at the child is the child's drop (its kernel
            // counts it), as on a real path.
            let _ = down.send_to(&datagram, child_video);
        }
        if let Some(parent) = parent {
            while to_parent.front().is_some_and(|(due, _)| *due <= now) {
                let (_, datagram) = to_parent.pop_front().unwrap();
                let _ = up.send_to(&datagram, parent);
            }
        }
        let next = [to_child.front(), to_parent.front()]
            .into_iter()
            .flatten()
            .map(|(due, _)| *due)
            .min();
        match next.map(|due| due.saturating_duration_since(Instant::now())) {
            Some(wait) if wait < Duration::from_millis(2) => {
                if !wait.is_zero() {
                    std::thread::sleep(wait.min(Duration::from_micros(100)));
                }
            }
            Some(wait) => {
                let _ = fast_plane::sys::wait_readable(&fds, (wait.as_millis() as i32 - 1).max(1));
            }
            None => {
                let _ = fast_plane::sys::wait_readable(&fds, 5);
            }
        }
    }
}

// ------------------------------------------------------------------ nodes

pub(super) struct ChainNode {
    pub(super) cache: Arc<LiveTsCache>,
    shutdown_tx: watch::Sender<()>,
    task: tokio::task::JoinHandle<Result<()>>,
    readers: Vec<tokio::task::JoinHandle<()>>,
    /// Its parent's symbols come to this (video) socket.
    pub(super) video_in: SocketAddr,
    pub(super) repair: Arc<PartRepair>,
}

impl ChainNode {
    pub(super) fn stats(&self) -> fast_plane::relay::FastPlaneSnapshot {
        self.cache.relay_ingress_snapshot().fast_plane
    }

    pub(super) fn repair_stats(&self) -> PartRepairSnapshot {
        self.repair.stats.snapshot()
    }

    pub(super) async fn stop(self) {
        let _ = self.shutdown_tx.send(());
        let _ = self.task.await;
        for reader in self.readers {
            reader.abort();
        }
    }
}

/// A fast-plane node whose one parent is `parent` (bound promoted, as the
/// deploy binds a node's parents: it admits repair), feeding `child` (a
/// link's parent target), with part repair as `config`.
pub(super) async fn chain_node(
    name: &str,
    parent: SocketAddr,
    child: Option<SocketAddr>,
    config: PartRepairConfig,
    pool_only: bool,
) -> ChainNode {
    let generation = TopologyGeneration::new(FAST_GENERATION).unwrap();
    let subscription_id = SubscriptionId::new(FAST_SUBSCRIPTION).unwrap();
    let mut dispatch = empty_relay_udp_dispatch();
    dispatch
        .bind_controlled_peer(
            parent,
            ControlledRelayParentSession::new(
                1,
                CarrierIdentity {
                    local: NodeId::new(name).unwrap(),
                    peer: NodeId::new("parent").unwrap(),
                    kind: CarrierKind::PrivateUdp,
                    trust_mode: TrustMode::ControlledPrivateNetwork,
                },
                generation,
                subscription_id,
                ParentPath::PromotedSecondary,
            )
            .unwrap(),
        )
        .unwrap();
    let endpoints: Vec<RelayForwardEndpoint> = child
        .into_iter()
        .map(|target| RelayForwardEndpoint {
            bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            target,
            role: RelayForwardRole::All,
        })
        .collect();
    let forwarder = RelayDownstreamForwarder::bind_split(&endpoints, Some(OFFSET))
        .await
        .unwrap();
    if let Some(forwarder) = &forwarder {
        forwarder.configure_buffers(64 << 20);
    }
    let sockets = fast_plane::relay::bind_class_sockets(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        None,
        OFFSET,
        64 << 20,
    )
    .unwrap();
    let video_in = sockets
        .iter()
        .find(|socket| socket.class == fast_plane::TrafficClass::Video)
        .map(|socket| socket.socket.local_addr().unwrap())
        .unwrap();
    for socket in &sockets {
        let _ = SockRef::from(&socket.socket).set_recv_buffer_size(64 << 20);
    }
    let cache = LiveTsCache::new(STREAM, Duration::from_micros(1_000_000 / FPS), 2, 48, 8_192).await;
    cache.set_hop_journal(Arc::new(HopJournal::new(name, 65_536)));
    let trim = config.requests || config.link_repair;
    let repair = PartRepair::start(config, relay_session::RelayLimits::default());
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let task = tokio::spawn(fast_plane::relay::run_relay_class_ingest(
        sockets,
        Arc::clone(&cache),
        shutdown_rx,
        RelayIngestRuntime {
            dispatch,
            secondary_socket: None,
            class_twins: Vec::new(),
            forwarder: forwarder.clone(),
            audio_epochs: None,
            failover_controller: None,
            failover_heartbeat: Duration::from_millis(100),
            link_telemetry: None,
        },
        fast_plane::relay::RelayClassConfig {
            port_offset: OFFSET,
            cpus: [None, None],
            realtime: false,
            loss_report: true,
            video_socket_buffer_ms: 0,
            trim_forwarded_repair: trim,
            trim_to_margin: std::env::var("KR_TRIM_TO_MARGIN").is_ok(),
            // Each node's own pool (KR_DECODE_WORKERS, default 1: a
            // 2-vCPU node's), when part repair is on.
            decode_pool: if trim || pool_only {
                fast_plane::decode_pool::DecodePool::start(
                    std::env::var("KR_DECODE_WORKERS")
                        .ok()
                        .and_then(|value| value.parse().ok())
                        .unwrap_or(1),
                    fast_plane::decode_pool::DECODE_QUEUE_BYTES,
                )
            } else {
                None
            },
            // pool_only: --relay-part-repair false --relay-link-repair
            // false (no part repair at all) with the decode pool: the
            // cheap tee and the tee per lane alone.
            part_repair: (!pool_only).then(|| Arc::clone(&repair)),
            ..Default::default()
        },
    ));
    let readers = forwarder
        .as_ref()
        .map(|forwarder| forwarder.spawn_demand_readers())
        .unwrap_or_default();
    tokio::time::timeout(Duration::from_secs(3), async {
        while cache.relay_ingress_snapshot().fast_plane.threads < 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the fast-plane threads start");
    ChainNode {
        cache,
        shutdown_tx,
        task,
        readers,
        video_in,
        repair,
    }
}

// ------------------------------------------------------------------ frames

pub(super) struct Part {
    pub(super) media_bytes: usize,
    pub(super) sources: usize,
    pub(super) datagrams: Vec<Vec<u8>>,
}

/// The frame sizes of the run: the clip's, from its first frame, with a
/// loop-point frame of 6 MB at `loop_at.0` and one of 3.5 MB at
/// `loop_at.1`.
pub(super) fn frame_sizes(count: usize, loop_at: (usize, usize)) -> Vec<usize> {
    (0..count)
        .map(|index| {
            if index == loop_at.0 {
                6_000_000
            } else if index == loop_at.1 {
                3_500_000
            } else {
                CLIP[index % CLIP.len()] as usize
            }
        })
        .collect()
}

/// av-contrib's `--relay-video-plan-grid` (src/source_first.rs): the symbol
/// trimmed so that K lands on a grid of 16 values an octave, so that the
/// RaptorQ plans of a 4K stream's parts are a few dozen.
fn grid_symbol_size(len: usize, max_symbol_size: u16) -> u16 {
    let size = usize::from(max_symbol_size);
    let Ok(symbols) = u16::try_from(len.div_ceil(size)) else {
        return max_symbol_size;
    };
    let target = if symbols <= 32 {
        symbols
    } else {
        let step = 1_u32 << ((15 - symbols.leading_zeros()) - 4);
        u16::try_from(u32::from(symbols).div_ceil(step) * step).unwrap_or(symbols)
    };
    let trimmed = len.div_ceil(usize::from(target));
    if target == symbols || trimmed == 0 || trimmed > size || len.div_ceil(trimmed) != usize::from(target) {
        return max_symbol_size;
    }
    u16::try_from(trimmed).unwrap_or(max_symbol_size)
}

/// Each frame's source symbols, and `repair(K)` repair symbols after them
/// (the encoder makes one at least), made on every core.
pub(super) fn make_parts(sizes: &[usize], repair: fn(u32) -> u32) -> Vec<Part> {
    let deadline = now_unix_us() + 3_600_000_000;
    let threads = std::thread::available_parallelism()
        .map_or(4, |threads| threads.get())
        .clamp(1, 16);
    let mut parts: Vec<Option<Part>> = (0..sizes.len()).map(|_| None).collect();
    let per_thread = sizes.len().div_ceil(threads).max(1);
    std::thread::scope(|scope| {
        for (chunk, slots) in parts.chunks_mut(per_thread).enumerate() {
            scope.spawn(move || {
                for (offset, slot) in slots.iter_mut().enumerate() {
                    let number = (chunk * per_thread + offset) as u64;
                    let media_bytes = sizes[number as usize];
                    let envelope = frame_envelope(STREAM, number, media_bytes, FPS);
                    let symbol = grid_symbol_size(envelope.len(), SYMBOL);
                    let sources = envelope.len().div_ceil(usize::from(symbol)) as u32;
                    let wanted = repair(sources);
                    let (mut source, repair) = fast_test_symbols(
                        &envelope,
                        relay_session::MediaPriority::VideoDelta,
                        symbol,
                        u16::try_from(sources.max(400)).unwrap(),
                        wanted.max(1),
                        deadline,
                    );
                    let sources = source.len();
                    source.extend(repair.into_iter().take(wanted as usize));
                    *slot = Some(Part {
                        media_bytes,
                        sources,
                        datagrams: source,
                    });
                }
            });
        }
    });
    parts.into_iter().map(Option::unwrap).collect()
}

/// Send each part at its 1/24 s slot from `start`, in one burst at the
/// contributor's speed. Returns when each part's first datagram left.
fn send_parts(socket: &std::net::UdpSocket, target: SocketAddr, parts: &[Part], start: Instant) -> Vec<u64> {
    let mut first = Vec::with_capacity(parts.len());
    let fd = socket.as_raw_fd();
    for (index, part) in parts.iter().enumerate() {
        let due = start + Duration::from_nanos(1_000_000_000 / FPS) * index as u32;
        if let Some(wait) = due.checked_duration_since(Instant::now()) {
            std::thread::sleep(wait);
        }
        first.push(now_unix_us());
        let began = Instant::now();
        let mut bytes = 0_u64;
        let mut at = 0;
        while at < part.datagrams.len() {
            let chunk = (part.datagrams.len() - at).min(32);
            let sent = fast_plane::sys::send_batch(fd, chunk, |index| {
                (&part.datagrams[at + index][..], target)
            })
            .unwrap_or(0);
            if sent == 0 {
                std::thread::sleep(Duration::from_micros(50));
                continue;
            }
            bytes += part.datagrams[at..at + sent]
                .iter()
                .map(|datagram| datagram.len() as u64)
                .sum::<u64>();
            at += sent;
            // At the contributor's speed.
            let due = began + Duration::from_nanos(bytes * 8 * 1_000_000_000 / SOURCE_BITS_PER_SECOND);
            if let Some(wait) = due.checked_duration_since(Instant::now()) {
                if wait > Duration::from_micros(200) {
                    std::thread::sleep(wait);
                }
            }
        }
    }
    first
}

// ------------------------------------------------------------------ runs

#[derive(Debug, Clone, Copy)]
pub(super) struct Case {
    pub(super) rtt_ms: u64,
    pub(super) loss: Loss,
    /// Part repair: asks and answers, and link repair.
    pub(super) requests: bool,
    pub(super) link_repair: bool,
    /// No part repair, the decode pool on (see `chain_node`).
    pub(super) pool_only: bool,
}

pub(super) struct ChainRun {
    pub(super) case: Case,
    /// When each part's first datagram left the source, and when the viewer
    /// decoded it (unix µs).
    pub(super) first_us: Vec<u64>,
    pub(super) decoded_us: HashMap<u64, u64>,
    /// Parts that some node asked a parent for.
    pub(super) asked: std::collections::HashSet<u64>,
    pub(super) measured: std::ops::Range<usize>,
    /// Per link: datagrams and bytes the parent handed it, dropped.
    pub(super) links: Vec<(u64, u64, u64)>,
    pub(super) source_datagrams: u64,
    pub(super) media_bytes: u64,
    pub(super) repair: Vec<(String, PartRepairSnapshot)>,
    /// Each node's first asks, as its requester saw each part then.
    pub(super) ask_notes: Vec<(String, Vec<fast_plane::part_repair::AskNote>)>,
    pub(super) kernel_dropped: u64,
    pub(super) tee_dropped: u64,
    pub(super) fast: Vec<fast_plane::relay::FastPlaneSnapshot>,
    pub(super) seconds: f64,
}

const NODES: [&str; 5] = ["kr-contrib", "kr-forwarder", "kr-longhaul", "kr-edge", "kr-viewer"];

/// One run of `parts` through the chain; `measured` are the parts scored.
pub(super) fn run_chain(parts: &[Part], measured: std::ops::Range<usize>, case: Case) -> ChainRun {
    // Each node on its own runtime of two workers, as on its own 2-vCPU
    // host (the threads still share this machine).
    let runtimes: Vec<tokio::runtime::Runtime> = NODES
        .iter()
        .map(|name| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name(*name)
                .enable_all()
                .build()
                .unwrap()
        })
        .collect();
    let config = |name: &str| PartRepairConfig {
        requests: case.requests,
        link_repair: case.link_repair,
        initial_rtt_us: 50_000,
        window: fast_plane::part_repair::window_for(name),
        deadline_us: 1_000_000,
        regenerate_sources: case.requests,
        store_bytes: fast_plane::part_repair::STORE_BYTES,
    };
    let mut links: Vec<Link> = (0..4).map(|_| Link::bind()).collect();
    let source = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let _ = SockRef::from(&source).set_send_buffer_size(64 << 20);
    let parents = [
        source.local_addr().unwrap(),
        links[0].child_parent(),
        links[1].child_parent(),
        links[2].child_parent(),
        links[3].child_parent(),
    ];
    let children = [
        Some(links[0].parent_target()),
        Some(links[1].parent_target()),
        Some(links[2].parent_target()),
        Some(links[3].parent_target()),
        None,
    ];
    let nodes: Vec<ChainNode> = (0..NODES.len())
        .rev()
        .map(|index| {
            runtimes[index].block_on(chain_node(
                NODES[index],
                parents[index],
                children[index],
                config(NODES[index]),
                case.pool_only,
            ))
        })
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    let start = Instant::now() + Duration::from_millis(100);
    let one_way = |rtt_ms: u64| Duration::from_micros(rtt_ms * 500);
    let rtts = [1, case.rtt_ms, 1, case.rtt_ms.min(30)];
    // KR_LOSSY_LINKS (e.g. "2"): only these links (1-4) take the loss.
    let lossy: Vec<usize> = std::env::var("KR_LOSSY_LINKS")
        .map(|links| links.split(',').filter_map(|link| link.trim().parse().ok()).collect())
        .unwrap_or_else(|_| vec![1, 2, 3, 4]);
    for (index, link) in links.iter_mut().enumerate() {
        link.start(
            nodes[index + 1].video_in,
            one_way(rtts[index]),
            if lossy.contains(&(index + 1)) { case.loss } else { Loss::None },
            0x5eed_0000 + index as u64 * 7_919 + case.rtt_ms,
            start,
            // Each link's outages at their own times.
            1_100 + 700 * index as u64,
        );
    }
    let target = nodes[0].video_in;
    let started = Instant::now();
    let first_us = send_parts(&source, target, parts, start);
    // The last parts' repair: twice the longest round trip and a second.
    std::thread::sleep(Duration::from_millis(1_000 + 2 * case.rtt_ms * 2));
    let viewer = &nodes[4];
    let decoded_us: HashMap<u64, u64> = viewer
        .cache
        .hops()
        .map(|journal| {
            journal
                .events(hop_journal::HopQuery {
                    since_us: None,
                    stream: Some(STREAM),
                })
                .into_iter()
                .filter(|event| event.stage == hop_journal::HopStage::RelayDecoded)
                .map(|event| (event.seq, event.t_us))
                .collect()
        })
        .unwrap_or_default();
    let asked = nodes
        .iter()
        .flat_map(|node| node.repair.asked_parts())
        .collect();
    let link_stats = links
        .iter()
        .map(|link| {
            (
                link.stats.offered.load(Ordering::Relaxed),
                link.stats.offered_bytes.load(Ordering::Relaxed),
                link.stats.dropped.load(Ordering::Relaxed),
            )
        })
        .collect();
    let repair = nodes
        .iter()
        .zip(NODES)
        .map(|(node, name)| (name.to_string(), node.repair_stats()))
        .collect();
    let fast: Vec<_> = nodes.iter().map(ChainNode::stats).collect();
    let ask_notes = nodes
        .iter()
        .zip(NODES)
        .map(|(node, name)| (name.to_string(), node.repair.ask_log.lock().unwrap().clone()))
        .collect();
    let (kernel_dropped, tee_dropped) = nodes.iter().fold((0, 0), |(kernel, tee), node| {
        let stats = node.stats();
        (kernel + stats.kernel_dropped, tee + stats.tee_dropped)
    });
    for link in &mut links {
        link.stop();
    }
    for (node, rt) in nodes.into_iter().zip(&runtimes) {
        rt.block_on(node.stop());
    }
    ChainRun {
        case,
        first_us,
        decoded_us,
        asked,
        measured,
        links: link_stats,
        source_datagrams: parts.iter().map(|part| part.sources as u64).sum(),
        media_bytes: parts.iter().map(|part| part.media_bytes as u64).sum(),
        repair,
        ask_notes,
        kernel_dropped,
        tee_dropped,
        fast,
        seconds: started.elapsed().as_secs_f64(),
    }
}

impl ChainRun {
    /// The measured parts the viewer did not decode.
    pub(super) fn missing(&self) -> Vec<u64> {
        self.measured
            .clone()
            .map(|index| index as u64)
            .filter(|part| !self.decoded_us.contains_key(part))
            .collect()
    }

    /// Source to viewer of each measured part decoded (µs).
    pub(super) fn latency_us(&self, part: u64) -> Option<u64> {
        self.decoded_us
            .get(&part)
            .map(|decoded| decoded.saturating_sub(self.first_us[part as usize]))
    }

    fn total(&self, pick: impl Fn(&PartRepairSnapshot) -> u64) -> u64 {
        self.repair.iter().map(|(_, snapshot)| pick(snapshot)).sum()
    }

    /// Per link: what the parent sent beyond the source symbols, as a share
    /// of them (datagrams), and the wire bytes over the media.
    fn overhead(&self) -> (Vec<f64>, f64) {
        let per_link = self
            .links
            .iter()
            .map(|(offered, _, _)| *offered as f64 / self.source_datagrams as f64 - 1.0)
            .collect();
        let bytes: u64 = self.links.iter().map(|(_, bytes, _)| *bytes).sum();
        let wire = bytes as f64 / (self.links.len() as f64 * self.media_bytes as f64) - 1.0;
        (per_link, wire)
    }

    /// The table's row, against `base` (the no-loss run at the same round
    /// trip) for the added latency.
    pub(super) fn row(&self, base: Option<&ChainRun>) -> String {
        let percentile = |values: &mut Vec<f64>, q: f64| -> f64 {
            if values.is_empty() {
                return f64::NAN;
            }
            values.sort_by(f64::total_cmp);
            values[((values.len() - 1) as f64 * q).round() as usize]
        };
        let mut all = Vec::new();
        let mut asked = Vec::new();
        let mut plain = Vec::new();
        let missing = self.missing();
        for index in self.measured.clone() {
            let part = index as u64;
            let Some(latency) = self.latency_us(part) else {
                continue;
            };
            let reference = base.and_then(|base| base.latency_us(part)).unwrap_or(latency);
            let added = (latency as f64 - reference as f64) / 1_000.0;
            all.push(added);
            if self.asked.contains(&part) {
                asked.push(added);
            } else {
                plain.push(added);
            }
        }
        let (per_link, wire) = self.overhead();
        let dropped: u64 = self.links.iter().map(|(_, _, dropped)| *dropped).sum();
        let offered: u64 = self.links.iter().map(|(offered, _, _)| *offered).sum();
        format!(
            "| {} | {} | {}/{} | {} | {:.1} / {:.1} / {:.1} | {:.1} | {:.1} | {} | {:.1} % | {:.2} % | {} / {} / {} | {} |",
            self.case.rtt_ms,
            self.case.loss.name(),
            self.measured.len() - missing.len(),
            self.measured.len(),
            asked.len(),
            percentile(&mut all.clone(), 0.5),
            percentile(&mut all.clone(), 0.99),
            all.iter().copied().fold(f64::NAN, f64::max),
            percentile(&mut asked.clone(), 0.99),
            percentile(&mut plain.clone(), 0.99),
            per_link
                .iter()
                .map(|share| format!("{:.1}", share * 100.0))
                .collect::<Vec<_>>()
                .join("/"),
            wire * 100.0,
            dropped as f64 * 100.0 / offered.max(1) as f64,
            self.total(|s| s.requests),
            self.total(|s| s.answer_symbols),
            self.total(|s| s.link_repair_symbols),
            if missing.is_empty() {
                format!(
                    "kd {} td {} encode max {} ms, mean {} ms",
                    self.kernel_dropped,
                    self.tee_dropped,
                    self.repair.iter().map(|(_, s)| s.encode_us_max).max().unwrap_or(0) / 1_000,
                    self.total(|s| s.encode_us)
                        / (self.total(|s| s.answers) + self.total(|s| s.link_repair_parts)).max(1)
                        / 1_000
                )
            } else {
                format!("missing {:?} kd {} td {}", &missing[..missing.len().min(8)], self.kernel_dropped, self.tee_dropped)
            },
        )
    }
}

pub(super) const HEADER: &str = "| RTT ms | loss/link | parts whole at viewer | parts asked | added ms p50 / p99 / max | p99 asked | p99 not asked | repair % of sources per link (1/2/3/4) | wire over media | loss seen | asks / answer symbols / link-repair symbols | notes |\n|---|---|---|---|---|---|---|---|---|---|---|---|";

fn env_list(name: &str, default: &str) -> Vec<String> {
    std::env::var(name)
        .unwrap_or_else(|_| default.into())
        .split(',')
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
        .collect()
}

fn loss_of(name: &str) -> Loss {
    match name {
        "0" => Loss::None,
        "1" => Loss::Random(0.01),
        "5" => Loss::Random(0.05),
        "ge" => Loss::Bursts { p: 0.01, r: 0.20 },
        "out20" => Loss::Outage { ms: 20, every_ms: 3_000 },
        "out200" => Loss::Outage { ms: 200, every_ms: 3_000 },
        other => panic!("unknown loss {other}"),
    }
}

/// The parts of a run of `seconds` after 2 s of warm-up (the links' loss
/// is measured by then), with loop-point frames 100 and 200 parts in.
pub(super) fn bench_parts(
    seconds: u64,
    scale: usize,
    repair: fn(u32) -> u32,
) -> (Vec<Part>, std::ops::Range<usize>) {
    let warmup = 2 * FPS as usize;
    let measured = (seconds * FPS) as usize;
    let sizes: Vec<usize> = frame_sizes(warmup + measured + FPS as usize, (warmup + 100, warmup + 200))
        .into_iter()
        .map(|size| size / scale)
        .collect();
    (make_parts(&sizes, repair), warmup..warmup + measured)
}

/// The bench: every case of KR_CASES (0,1,5,ge,out20,out200) at every
/// round trip of KR_RTTS (1,30,75,250), KR_SECONDS (12) measured each.
/// KR_MODE: "repair" (default: part repair and link repair) or "off" (the
/// integrated build's fast plane: neither).
#[test]
#[ignore = "a measurement: run it alone, in release"]
fn part_repair_chain_bench() {
    super::video_throughput::allow_sampling();
    let seconds: u64 = std::env::var("KR_SECONDS").ok().and_then(|v| v.parse().ok()).unwrap_or(12);
    // "integrated": the build before 4k-repair: the contributor's repair
    // (AdaptiveFecController: 5 % of K, at most 16) forwarded as it is, no
    // asks, no link repair, no trim.
    let integrated = std::env::var("KR_MODE").as_deref() == Ok("integrated");
    let on = !integrated && std::env::var("KR_MODE").as_deref() != Ok("off");
    let scale: usize = std::env::var("KR_SCALE").ok().and_then(|v| v.parse().ok()).unwrap_or(1);
    let (parts, measured) = bench_parts(
        seconds,
        scale,
        if integrated { |k| (k / 20).clamp(1, 16) } else { |_| 0 },
    );
    let media: u64 = parts[measured.clone()].iter().map(|part| part.media_bytes as u64).sum();
    println!(
        "4k-repair bench: {} parts, {:.1} Mbit/s of media measured, symbols {} B, part repair {on}",
        parts.len(),
        media as f64 * 8.0 / (measured.len() as f64 / FPS as f64) / 1e6,
        SYMBOL
    );
    println!("{HEADER}");
    let mut out = String::new();
    for rtt in env_list("KR_RTTS", "1,30,75,250") {
        let rtt_ms: u64 = rtt.parse().unwrap();
        let mut base = None;
        for loss in env_list("KR_CASES", "0,1,5,ge,out20,out200") {
            let run = run_chain(
                &parts,
                measured.clone(),
                Case {
                    rtt_ms,
                    loss: loss_of(&loss),
                    requests: on,
                    link_repair: on,
                    pool_only: false,
                },
            );
            let row = run.row(base.as_ref());
            println!("{row}");
            if std::env::var("KR_DEBUG").is_ok() {
                for ((name, _), f) in run.repair.iter().zip(&run.fast) {
                    println!(
                        "  {name}: tee lag max {} ms, tee cpu {:.0} %, tee dropped {}, shed {}, trimmed {}, lane loss {:?}, video received {}, pool decodes {} ({:.0} % cpu, max {} ms, inline {}, failed {})",
                        f.video_tee_lag_max_us / 1_000,
                        f.video_tee_cpu_us as f64 / 1e4 / run.seconds,
                        f.tee_dropped,
                        f.video_tee_parts_shed,
                        f.repair_trimmed,
                        f.video_parent_loss_ppm,
                        f.video_received,
                        f.decode_pool.decodes,
                        f.decode_pool.decode_us as f64 / 1e4 / run.seconds,
                        f.decode_pool.decode_us_max / 1_000,
                        f.decode_pool.inline,
                        f.decode_pool.failures
                    );
                }
                let sources = run.source_datagrams.max(1) as f64;
                for ((name, s), f) in run.repair.iter().zip(&run.fast) {
                    println!(
                        "  cost {name}: tee cpu {:.1} % ({:.0} us/part), pool cpu {:.1} %, repair sent {:.2} % of the stream's sources (answers {} link {} regenerated {}), asks {}",
                        f.video_tee_cpu_us as f64 / 1e4 / run.seconds,
                        f.video_tee_cpu_us as f64 / run.first_us.len().max(1) as f64,
                        f.decode_pool.decode_us as f64 / 1e4 / run.seconds,
                        (s.answer_symbols + s.link_repair_symbols + s.regenerated_symbols) as f64 * 100.0 / sources,
                        s.answer_symbols,
                        s.link_repair_symbols,
                        s.regenerated_symbols,
                        s.requests
                    );
                }
                for (name, notes) in &run.ask_notes {
                    for note in notes.iter().take(12) {
                        println!("  ask {name}: {note:?}");
                    }
                }
                for (name, s) in &run.repair {
                    println!(
                        "  {name}: asks {} retries {} whole {} gap {} recovered {} gave_up {} probes {} heard {} missing_heard {} srtt_max {} wait_max {} | asked_here {} answers {} ans_sym {} waiting {} missing {} busy {} stored {} link_parts {} link_sym {} loss_reports {} ledgers {} encode_max {} regen {}/{}",
                        s.requests, s.retries, s.whole_parts_asked, s.gap_parts, s.recovered_parts, s.gave_up_parts,
                        s.probes, s.answers_heard, s.missing_heard, s.srtt_us_max, s.repair_wait_max_us,
                        s.asks, s.answers, s.answer_symbols, s.answer_waiting, s.answer_missing, s.busy, s.stored_parts,
                        s.link_repair_parts, s.link_repair_symbols, s.child_loss_reports, s.ledgers, s.encode_us_max,
                        s.regenerated_parts, s.regenerated_symbols
                    );
                }
            }
            out.push_str(&row);
            out.push('\n');
            if loss == "0" {
                base = Some(run);
            }
        }
    }
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/4k-repair-bench.md");
    let _ = std::fs::write(&path, format!("{HEADER}\n{out}"));
}

/// Through 2 % random loss and bursts on every link, at a tenth of the
/// rate for 3 s: every part reaches the viewer, by asks and link repair.
#[test]
fn parts_that_links_damage_are_asked_for_and_reach_the_viewer() {
    let (parts, measured) = bench_parts(3, 10, |_| 0);
    for loss in [Loss::Random(0.02), Loss::Bursts { p: 0.01, r: 0.2 }] {
        let run = run_chain(
            &parts,
            measured.clone(),
            Case {
                rtt_ms: 30,
                loss,
                requests: true,
                link_repair: true,
                pool_only: false,
            },
        );
        println!("{HEADER}\n{}", run.row(None));
        assert!(run.missing().is_empty(), "{:?}: missing {:?}", loss, run.missing());
        assert!(run.total(|s| s.requests) > 0, "nothing asked");
        assert!(run.total(|s| s.recovered_parts) > 0, "nothing recovered by an ask");
        assert!(run.total(|s| s.link_repair_symbols) > 0, "no link repair");
    }
}

/// Without part repair, the same links lose parts (the test's control).
#[test]
fn without_part_repair_the_same_links_lose_parts() {
    let (parts, measured) = bench_parts(2, 10, |_| 0);
    let run = run_chain(
        &parts,
        measured,
        Case {
            rtt_ms: 30,
            loss: Loss::Random(0.02),
            requests: false,
            link_repair: false,
            pool_only: false,
        },
    );
    println!("{HEADER}\n{}", run.row(None));
    assert!(!run.missing().is_empty());
}

/// 4k-repair review M2: a stream whose parts straddle one symbol (one-
/// and two-symbol parts in turn), every part intact. A one-symbol part is
/// too small for part repair, but it is a part of the stream: the
/// requester must never take it for a gap, ask for it or give it up.
#[test]
fn a_stream_of_one_and_two_symbol_parts_is_never_asked_for() {
    const DAMAGED: [u64; 3] = [101, 151, 201];
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let source = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let node = chain_node(
            "m2-node",
            source.local_addr().unwrap(),
            None,
            PartRepairConfig {
                window: fast_plane::part_repair::window_for("m2-node"),
                ..PartRepairConfig::default()
            },
            false,
        )
        .await;
        let deadline = now_unix_us() + 3_600_000_000;
        let mut sizes = Vec::new();
        for number in 0..300_u64 {
            // The smallest media that makes a part of 1 (even) or 2 (odd)
            // symbols.
            let wanted = 1 + (number % 2) as usize;
            let (mut datagrams, repair) = (1..)
                .map(|step: usize| {
                    let envelope = frame_envelope(STREAM, number, step * 64, FPS);
                    fast_test_symbols(
                        &envelope,
                        relay_session::MediaPriority::VideoDelta,
                        SYMBOL,
                        400,
                        1,
                        deadline,
                    )
                })
                .find(|(sources, _)| sources.len() >= wanted)
                .unwrap();
            sizes.push(datagrams.len());
            // A one-symbol part comes alone (its copy on the other lane, as
            // a node with one parent sees it); a two-symbol part with one
            // repair symbol.
            if datagrams.len() > 1 {
                datagrams.extend(repair);
            }
            // The control: a few two-symbol parts lose a symbol and their
            // repair (they are asked for; the test's source never answers).
            if DAMAGED.contains(&number) {
                datagrams.truncate(1);
            }
            for datagram in &datagrams {
                source.send_to(datagram, node.video_in).unwrap();
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert!(sizes.iter().filter(|k| **k == 1).count() >= 140, "{sizes:?}");
        assert!(sizes.iter().filter(|k| **k == 2).count() >= 140, "{sizes:?}");
        // Past the media deadline (1 s): any ask or give-up has happened.
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        let stats = node.repair_stats();
        assert!(node.stats().video_received >= 590, "{:?}", node.stats().video_received);
        let mut asked = node.repair.asked_parts();
        asked.sort_unstable();
        asked.dedup();
        assert_eq!(asked, DAMAGED, "only the damaged parts are asked for: {stats:?}");
        assert_eq!(stats.gap_parts, 0, "{stats:?}");
        assert_eq!(stats.gave_up_parts, DAMAGED.len() as u64, "{stats:?}");
        node.stop().await;
    });
}

/// The cheap tee alone, as a lab may run it before the full repair:
/// --relay-part-repair false --relay-link-repair false
/// --relay-trim-forwarded-repair false, the decode pool on (its default).
/// A part that comes with all its sources is their bytes (no RaptorQ), a
/// damaged one is decoded on the pool from the contributor's repair, and
/// nothing asks, answers or adds link repair.
#[test]
fn the_cheap_tee_alone_without_asks_or_link_repair() {
    // The contributor's repair as before 4k-repair: 5 % of K, at most 16.
    let (parts, measured) = bench_parts(2, 10, |k| (k / 20).clamp(1, 16));
    for (loss, damaged) in [(Loss::None, false), (Loss::Random(0.002), true)] {
        let run = run_chain(
            &parts,
            measured.clone(),
            Case {
                rtt_ms: 30,
                loss,
                requests: false,
                link_repair: false,
                pool_only: true,
            },
        );
        println!("{HEADER}\n{}", run.row(None));
        assert!(run.missing().is_empty(), "{loss:?}: missing {:?}", run.missing());
        assert_eq!(run.total(|s| s.requests), 0, "{loss:?}: asked");
        assert_eq!(run.total(|s| s.asks), 0, "{loss:?}: answered");
        assert_eq!(run.total(|s| s.link_repair_symbols), 0, "{loss:?}: link repair");
        let decodes: u64 = run.fast.iter().map(|f| f.decode_pool.decodes).sum();
        let inline: u64 = run.fast.iter().map(|f| f.decode_pool.inline).sum();
        if damaged {
            assert!(decodes > 0, "a damaged part was decoded on the pool");
        } else {
            assert_eq!(decodes + inline, 0, "a whole part took RaptorQ");
        }
        for f in &run.fast[1..] {
            assert!(f.decode_pool.workers > 0, "the pool is on");
        }
    }
}
