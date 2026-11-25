//! Assignment of a reader to the LL-HLS caches of its region.
//!
//! The entry point of a region answers one question for each reader: which
//! cache serves this playlist to this session, and which caches follow if that
//! one fails. The answer is a ranked list of caches. The first is the
//! assignment and the rest are the failover order. A Multivariant Playlist
//! carries the list as one Content Steering pathway for each cache.
//!
//! The choice is a pure function of the caches that the region advertises and
//! the session identifier. Every node of a region that sees the same
//! advertisements gives the same answer, so any node can be an entry point and
//! no node holds assignment state for the region.
//!
//! Rules, in order:
//!
//! 1. A pinned reader gets exactly the named cache, whatever its load.
//! 2. A reader that already uses a cache keeps it while that cache is healthy
//!    and not draining, even when the cache admits no new session.
//! 3. A new reader goes to a cache that already holds the playlist and has
//!    room below the spread target.
//! 4. When every holder is above the spread target, the reader goes to a cache
//!    that does not hold the playlist yet. That cache subscribes to the
//!    playlist, which is how a copy is added when demand arrives.
//! 5. When every cache is above the spread target, the reader goes to any
//!    cache that still admits sessions, by free capacity.
//!
//! Within a rule the choice is a weighted rendezvous hash of the session, so
//! sessions spread in proportion to free capacity and a session keeps its
//! order of caches while the set of caches is stable.
//!
//! A UDP listener may hold two legs (two subscriptions on two edges) and a
//! spare (a token at a third edge). The leg it depends on follows the rules
//! above. A redundant leg (the second of a join, or any leg of a listener
//! that holds one already) goes only to a cache that stays under the
//! rebalance target with it, and never spills. The spare is a cache that
//! holds the playlist, with no load gate: it carries nothing until a hedge.
//!
//! Two instances on one physical host stop together (measured on 23 Nov), so
//! two legs on one host are not two paths. A redundant leg goes to an edge
//! whose host tag (`--host-tag`) differs from those of the listener's other
//! legs when both are set, and the spare prefers a third host. When no edge
//! on another host is eligible the choice is made as if no tag were set, and
//! the answer says so (`second_leg_same_host`).

use av_mesh::client_udp::{ShortGrant, MAX_PATHS};
use serde::Serialize;

/// Load at or above this, in thousandths of the limit of the cache, means the
/// cache admits no new session.
pub const FULL_LOAD_PERMILLE: u32 = 1_000;

/// One cache of a region as the entry point sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheCandidate {
    pub node_id: String,
    pub region: String,
    /// Base URL for playback, for example `https://edge-1.example/live`.
    pub playback_base_url: Option<String>,
    /// The cache serves readers. Distributors and relays do not.
    pub serves_readers: bool,
    /// The advertisement of the cache is current.
    pub fresh: bool,
    pub draining: bool,
    /// The cache reports that it admits no new session.
    pub overloaded: bool,
    /// Load in thousandths of the admission limit of the cache. 1,000 is the
    /// point at which the cache stops admitting sessions.
    pub load_permille: u32,
    /// Newest part of the playlist that the cache holds, if it holds it.
    pub head_part: Option<u64>,
    /// Bit rate that the cache can still admit, when it advertises it.
    pub free_bps: Option<u64>,
    /// What one more session of the playlist costs this cache, in bits per
    /// second: its bit rate, or the share of the cache that its requests
    /// take. `None` uses the bit rate of the playlist in the request.
    pub session_charge_bps: Option<u64>,
    /// When the cache wrote the advertisement that this record comes from.
    pub updated_unix_ms: u64,
    /// Rank of the cache for the standby copy of the playlist
    /// (`standby_rank`). The caches after the first are the failover order
    /// of a reader, and with this they are in the order in which the edges
    /// take standby copies: the first alternate is the edge that holds the
    /// copy. `None` orders the alternates by the session, like the first.
    pub failover_rank: Option<u64>,
    /// Load, in thousandths of the cache's limit, of the readers that this
    /// entry point has sent to the cache since the cache last advertised
    /// (wave 9 C4): counted in its headroom, so that readers asking at once
    /// (a drained edge's movers) are not all weighed against the same
    /// stale advertisement.
    pub pending_permille: u32,
    /// The physical host the cache runs on, as its advertisement names it
    /// (`--host-tag`); `None` when it names none.
    pub host: Option<String>,
    /// Its room for heavy sessions of the playlist (place240), with what
    /// this entry placed there since its advertisement; `None` for a cache
    /// that advertises none (an older node).
    pub heavy: Option<HeavyRoom>,
}

/// A cache's room for heavy sessions (place240, `heavy::HeavyAdvert`): CPU
/// in thousandths of one CPU, egress in bits a second.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeavyRoom {
    /// What heavy sessions may commit: the budget less the small
    /// sessions' reserve.
    pub cpu_budget: u32,
    /// What they commit: the larger of the model's cost and the measured
    /// CPU, and the sessions placed since the advertisement.
    pub cpu_used: u32,
    pub bps_max: u64,
    pub bps_used: u64,
    /// The cache pays the playlist's tee already (it holds it heavy, or a
    /// session placed since pays it).
    pub holds: bool,
    /// The cache says it is in distress: no heavy session goes there.
    pub distress: bool,
}

/// What one heavy session costs (place240): its rate, its CPU on a cache
/// that holds the playlist and the tee's on one that does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeavySession {
    pub bps: u64,
    pub cpu: u32,
    pub stream_cpu: u32,
    /// The rate is measured: with no room anywhere the session is refused.
    /// An estimate (no cache has measured the playlist yet) is placed by
    /// room but never refused.
    pub refuse: bool,
}

impl HeavyRoom {
    /// What the session costs this cache.
    pub fn cost(&self, session: &HeavySession) -> u32 {
        session
            .cpu
            .saturating_add(if self.holds { 0 } else { session.stream_cpu })
    }

    /// The session fits the cache's budget and egress, and the cache is
    /// not in distress.
    pub fn fits(&self, session: &HeavySession) -> bool {
        !self.distress
            && self.cpu_used.saturating_add(self.cost(session)) <= self.cpu_budget
            && self.bps_used.saturating_add(session.bps) <= self.bps_max
    }

    /// How full the cache is with the session (thousandths): the larger of
    /// its CPU and its egress, each against its limit.
    pub fn fill(&self, session: &HeavySession) -> u32 {
        let cpu = u64::from(self.cpu_used.saturating_add(self.cost(session))) * 1_000
            / u64::from(self.cpu_budget.max(1));
        let bps = u128::from(self.bps_used.saturating_add(session.bps)) * 1_000
            / u128::from(self.bps_max.max(1));
        cpu.max(bps.min(u128::from(u64::MAX)) as u64)
            .min(u64::from(u32::MAX)) as u32
    }
}

impl CacheCandidate {
    fn holds_playlist(&self) -> bool {
        self.head_part.is_some()
    }

    /// The advertised load and what this entry point has sent the cache
    /// since (wave 9 C4).
    fn effective_load_permille(&self) -> u32 {
        self.load_permille.saturating_add(self.pending_permille)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssignmentPolicy {
    /// Load below which a cache that holds the playlist takes new readers
    /// before another cache adds a copy.
    pub spread_permille: u32,
    /// Caches listed after the assigned cache, as the failover order.
    pub max_alternates: usize,
    /// A cache whose playlist is this many parts behind the newest copy in the
    /// region is stalled for that playlist. Zero disables the check.
    pub max_head_lag_parts: u64,
    /// A moving reader (a UDP client that left an edge: drain, kill,
    /// rebalance) is ranked by each cache's headroom alone, the caches
    /// under `rebalance_target_permille` first (wave 9 C4,
    /// `--assignment-move-headroom`): the movers of a drained or lost edge
    /// spread over the survivors in proportion to their headroom, as a
    /// spill does, not by the spread tiers, which send them all to the
    /// caches under 60 %. `false`: a mover is ranked as a new reader.
    pub move_headroom: bool,
    /// The load above which an edge sheds UDP listeners to an edge with
    /// room (wave 9 C1, `--rebalance-target-permille`); a mover goes to a
    /// cache under it first.
    pub rebalance_target_permille: u32,
    /// The rebalance's margin (`--client-udp-rebalance-margin-permille`):
    /// a shedding edge counts room only at edges under `target - margin`
    /// and fills them up to `target - margin / 2`, so a rebalanced
    /// listener goes only where it keeps the receiver under that (wave 9
    /// review).
    pub rebalance_margin_permille: u32,
    /// A playlist whose regional bit rate is above this is given one leg
    /// whatever the listener asks (`--assignment-two-path-max-bps`): a
    /// second copy of a 4K feed is not worth an edge fault.
    pub two_path_max_bps: u64,
    /// Heavy sessions (place240): caches whose fill with the session is
    /// within this (thousandths of their budget) of the emptiest are
    /// comparable, and the session's rendezvous hash chooses among them
    /// (`--heavy-place-band-permille`). One 240 Mbit/s session is 400 of a
    /// 2-vCPU edge's egress: joins to it never share a band.
    pub heavy_band_permille: u32,
}

/// Default of `--heavy-place-band-permille`.
pub const DEFAULT_HEAVY_BAND_PERMILLE: u32 = 50;

/// Default of `--assignment-two-path-max-bps`: 1 ms PCM is 2.6 Mbit/s a
/// leg, 4K is hundreds.
pub const DEFAULT_TWO_PATH_MAX_BPS: u64 = 8_000_000;

/// Default of `--rebalance-target-permille`.
pub const DEFAULT_REBALANCE_TARGET_PERMILLE: u32 = 800;

impl Default for AssignmentPolicy {
    fn default() -> Self {
        Self {
            spread_permille: 600,
            max_alternates: 2,
            max_head_lag_parts: 0,
            move_headroom: true,
            rebalance_target_permille: DEFAULT_REBALANCE_TARGET_PERMILLE,
            rebalance_margin_permille: DEFAULT_REBALANCE_MARGIN_PERMILLE,
            two_path_max_bps: DEFAULT_TWO_PATH_MAX_BPS,
            heavy_band_permille: DEFAULT_HEAVY_BAND_PERMILLE,
        }
    }
}

/// Default of `--client-udp-rebalance-margin-permille`.
pub const DEFAULT_REBALANCE_MARGIN_PERMILLE: u32 = 150;

impl AssignmentPolicy {
    /// The load up to which a rebalance fills a receiver.
    pub fn rebalance_receiver_permille(&self) -> u32 {
        self.rebalance_target_permille
            .saturating_sub(self.rebalance_margin_permille / 2)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AssignmentRequest<'a> {
    pub region: &'a str,
    /// Stable identifier of the playback session, for example the CMCD `sid`.
    pub session: Option<&'a str>,
    /// Cache that an operator or a test names for this reader.
    pub pin: Option<&'a str>,
    /// Cache that the reader uses now, for example `_HLS_pathway`.
    pub current: Option<&'a str>,
    /// Caches this entry point gave the session lately, that it has neither
    /// left nor kept (B 3.6): a repeat is given them again, each only where
    /// the rule of its place in the answer would still take it.
    pub remembered: &'a [String],
    /// Bit rate of the playlist that the reader asks for, when a cache of the
    /// region has measured it. A new reader is assigned only to a cache that
    /// has this much room.
    pub playlist_bps: Option<u64>,
    /// The reader is moving: a UDP client that names caches it has just
    /// left (wave 9 C4).
    pub moving: bool,
    /// Movers that this entry point placed lately, per cache (wave 9 C4):
    /// a mover is apportioned against them.
    pub recent_moves: &'a [(String, u32)],
    /// The mover left an edge that is live, not draining and over the
    /// rebalance target (`MOVE rebalance`, wave 9 C1): only the caches that
    /// stay under [`AssignmentPolicy::rebalance_receiver_permille`] with it
    /// take it while one can.
    pub rebalancing: bool,
    /// What one UDP listener costs an edge of the region (permille: the
    /// median of load over UDP subscribers among the edges with a few),
    /// when known: movers fill the survivors up to a common level.
    pub listener_permille: Option<f64>,
    /// The region's entry points that answer assignments (0 is taken as
    /// 1): each sees only its own movers, so the region's are reckoned as
    /// its own times this when the level is raised.
    pub entry_points: u32,
    /// Legs wanted (`p=`, 0 to [`MAX_PATHS`]); 1 for an LL-HLS reader.
    pub paths: u8,
    /// A spare is wanted (`s=1`).
    pub spare: bool,
    /// The listener holds a leg already (`h=` not empty): every leg of the
    /// answer is redundant.
    pub redundant: bool,
    /// The host tags of the legs the listener holds: a leg of the answer
    /// goes to another host where one is eligible.
    pub held_hosts: &'a [String],
    /// The spare the listener holds (`k=`): with `spare`, it is the spare
    /// again while its edge passes the redundant leg's gate; over the gate,
    /// another under it is named if one is, else it is kept.
    pub spare_held: Option<&'a str>,
    /// The cache that holds the leg whose place a mover asks for (a hedge
    /// at the listener's spare that took a dead leg's place): it keeps the
    /// place while its share of the movers is not yet taken, so that only
    /// the hedges over their share move again.
    pub place_at: Option<&'a str>,
    /// The host tags of the caches the listener has just left (`skip`):
    /// a redundant leg and the spare go elsewhere where they can, as they
    /// do for the hosts of its legs, but this never shares a host as
    /// `second_leg_same_host` counts it (hosttag review, finding 5: the
    /// dead leg's sibling on its stopped host still looks fresh).
    pub left_hosts: &'a [String],
    /// The session is heavy (place240): it is placed by what it costs each
    /// cache, not by the tiers.
    pub heavy: Option<HeavySession>,
}

impl Default for AssignmentRequest<'_> {
    /// A reader that wants one cache: the LL-HLS reader, a one-path client.
    fn default() -> Self {
        Self {
            region: "",
            session: None,
            pin: None,
            current: None,
            remembered: &[],
            playlist_bps: None,
            moving: false,
            recent_moves: &[],
            rebalancing: false,
            listener_permille: None,
            entry_points: 0,
            paths: 1,
            spare: false,
            redundant: false,
            held_hosts: &[],
            spare_held: None,
            place_at: None,
            left_hosts: &[],
            heavy: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignmentReason {
    /// The reader named this cache.
    Pinned,
    /// The reader already uses this cache.
    Sticky,
    /// The cache holds the playlist and is below the spread target.
    HoldsPlaylist,
    /// Every holder is above the spread target; this cache adds a copy.
    NewCopy,
    /// Every cache is above the spread target; this one has the most room.
    Spill,
    /// Failover order after the assigned cache.
    Alternate,
    /// A redundant leg: the listener's other leg delivers without it.
    SecondPath,
    /// The spare: the listener holds a token here and hedges to it.
    Spare,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AssignedCache {
    pub node_id: String,
    pub playback_base_url: String,
    pub reason: AssignmentReason,
    pub load_permille: u32,
    pub holds_playlist: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Assignment {
    /// The legs granted first, then the spare, then the failover order.
    pub caches: Vec<AssignedCache>,
    pub pinned: bool,
    /// The first `legs` caches are legs.
    pub legs: u8,
    /// The cache after the legs is the spare.
    pub spare: bool,
    /// A redundant leg shares its host with another leg of the listener:
    /// no edge on another host was eligible.
    pub second_leg_same_host: bool,
    /// Why fewer legs than asked were granted (B Y2, the lead's decision):
    /// `why=` of `ASSIGNED`, `"why"` of `/api/assignment`.
    pub short: Option<ShortGrant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignmentError {
    /// The pinned cache is not a reader cache of the region.
    UnknownPinnedCache,
    /// No cache of the region can take the reader now.
    NoCacheAvailable,
    /// A listener that holds a leg asked for another and no cache of the
    /// region takes one (B 3.6: nothing spills), with the reason that
    /// `ASSIGNED` carries (`why=`, B Y2).
    NoLegGranted(ShortGrant),
    /// A heavy session that no cache of the region has room for (place240
    /// rule 2): the busy answer.
    HeavyFull,
}

impl std::fmt::Display for AssignmentError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownPinnedCache => {
                formatter.write_str("the pinned cache is not a reader cache of this region")
            }
            Self::NoCacheAvailable => {
                formatter.write_str("no cache of this region can take a new reader now")
            }
            Self::HeavyFull => formatter
                .write_str("no cache of this region has room for this heavy session now"),
            Self::NoLegGranted(why) => write!(
                formatter,
                "no cache of this region can take another leg of this listener now ({})",
                why.word()
            ),
        }
    }
}

impl std::error::Error for AssignmentError {}

/// Rank the caches of the region for one reader.
pub fn assign(
    request: AssignmentRequest<'_>,
    policy: AssignmentPolicy,
    candidates: &[CacheCandidate],
) -> Result<Assignment, AssignmentError> {
    let regional = candidates
        .iter()
        .filter(|cache| {
            cache.region == request.region
                && cache.serves_readers
                && cache.playback_base_url.is_some()
        })
        .collect::<Vec<_>>();

    if let Some(pin) = request.pin {
        let cache = regional
            .iter()
            .find(|cache| cache.node_id == pin)
            .ok_or(AssignmentError::UnknownPinnedCache)?;
        return Ok(Assignment {
            caches: vec![assigned(cache, AssignmentReason::Pinned)],
            pinned: true,
            legs: 1,
            spare: false,
            second_leg_same_host: false,
            short: None,
        });
    }

    let newest_head = regional
        .iter()
        .filter(|cache| cache.fresh && !cache.draining)
        .filter_map(|cache| cache.head_part)
        .max();
    let stalled = |cache: &CacheCandidate| {
        policy.max_head_lag_parts > 0
            && match (cache.head_part, newest_head) {
                (Some(head), Some(newest)) => {
                    newest.saturating_sub(head) > policy.max_head_lag_parts
                }
                _ => false,
            }
    };
    // A cache can keep a reader while it is healthy. It can take a new reader
    // while it also admits sessions.
    let healthy = |cache: &CacheCandidate| cache.fresh && !cache.draining && !stalled(cache);
    // A cache with some room can still be too full for this playlist: a 4K
    // reader does not fit where an audio reader does.
    let has_room = |cache: &CacheCandidate| match (
        cache.free_bps,
        cache.session_charge_bps.or(request.playlist_bps),
    ) {
        (Some(free), Some(needed)) => free >= needed,
        _ => true,
    };
    let admits = |cache: &CacheCandidate| {
        healthy(cache)
            && !cache.overloaded
            && cache.load_permille < FULL_LOAD_PERMILLE
            && has_room(cache)
    };

    // Place240: a heavy session goes by what it costs each cache.
    if let Some(heavy) = request.heavy {
        if let Some(answer) = assign_heavy(request, policy, &regional, heavy, &healthy, &admits) {
            return answer;
        }
    }

    let session = request.session.unwrap_or("");
    let moving = request.moving && policy.move_headroom;
    let rebalancing = moving && request.rebalancing;
    let mut ranked = regional
        .iter()
        .copied()
        .filter(|cache| admits(cache))
        .map(|cache| {
            let load = cache.effective_load_permille();
            let tier = if moving {
                // Wave 9 C4: by headroom alone; a listener that an edge
                // over its target sent away goes first where it keeps the
                // receiver under the level the shedder counted room to
                // (wave 9 review: under the target alone, a receiver at 780
                // took a share and passed 900).
                u8::from(
                    rebalancing
                        && f64::from(load) + request.listener_permille.unwrap_or(0.0)
                            >= f64::from(policy.rebalance_receiver_permille()),
                )
            } else if cache.holds_playlist() && load < policy.spread_permille {
                0_u8
            } else if !cache.holds_playlist() && load < policy.spread_permille {
                1
            } else {
                2
            };
            (tier, rendezvous_score(session, cache), cache)
        })
        .collect::<Vec<_>>();
    if rebalancing {
        // A rebalanced listener goes only to a receiver under the level, or
        // nowhere: it stays where it was (B 3.6). Tier 1 handed a leg to an
        // edge over the level.
        ranked.retain(|(tier, _, _)| *tier == 0);
    }
    ranked.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| right.1.total_cmp(&left.1))
            .then_with(|| left.2.node_id.cmp(&right.2.node_id))
    });

    // Legs granted: one for a playlist over the two-path bit rate, and
    // none more to a listener that holds a leg of it (B 3.9: no second edge
    // is asked to send video; review of B Y1, finding 7).
    let wanted = if request
        .playlist_bps
        .is_some_and(|bps| bps > policy.two_path_max_bps)
    {
        if request.redundant {
            0
        } else {
            request.paths.min(1)
        }
    } else {
        request.paths.min(MAX_PATHS)
    };
    // The level a redundant leg keeps its cache under, and the level that
    // movers fill the survivors up to.
    let listener_permille = request.listener_permille.unwrap_or(0.0);
    let (gate, level) = if rebalancing {
        (
            policy.rebalance_receiver_permille(),
            Level {
                at: policy.rebalance_receiver_permille(),
                listener_permille: None,
                movers_scale: 1.0,
            },
        )
    } else {
        (
            policy.rebalance_target_permille,
            Level {
                at: policy.rebalance_target_permille,
                listener_permille: request.listener_permille,
                movers_scale: f64::from(request.entry_points.max(1)),
            },
        )
    };

    // A redundant leg's gate: the cache admits (and, in a rebalance, is a
    // receiver under the level) and stays under the gate with the
    // listener counted.
    let passes_gate = |ranked: &[(u8, f64, &CacheCandidate)], cache: &CacheCandidate| {
        ranked.iter().any(|(_, _, other)| other.node_id == cache.node_id)
            && f64::from(cache.effective_load_permille()) + listener_permille < f64::from(gate)
    };
    // The caches the reader uses or was given lately keep it, each only
    // where the rule of its place would take it now (review of B Z3): the
    // leg it depends on while the cache is healthy, as before, and in a
    // rebalance only on a receiver under the level; a redundant leg only
    // through the gate. A memory of a cache the listener has since left
    // gave it a leg over the level.
    let mut stickies: Vec<&CacheCandidate> = Vec::new();
    for name in request
        .current
        .into_iter()
        .chain(request.remembered.iter().map(String::as_str))
    {
        if let Some(cache) = regional.iter().copied().find(|cache| cache.node_id == name) {
            if !stickies.iter().any(|sticky| sticky.node_id == cache.node_id) {
                stickies.push(cache);
            }
        }
    }
    let mut caches: Vec<AssignedCache> = Vec::new();
    if wanted > 0 && !request.redundant {
        let sticky = stickies.iter().copied().find(|cache| {
            healthy(cache)
                && (!rebalancing
                    || ranked.iter().any(|(_, _, other)| other.node_id == cache.node_id))
        });
        if let Some(cache) = sticky {
            caches.push(assigned(cache, AssignmentReason::Sticky));
        }
    }
    if wanted > 0 && caches.is_empty() && !request.redundant {
        // The leg the listener depends on: a new reader by the tiers, a
        // mover by headroom.
        if moving && !ranked.is_empty() {
            // Wave 9 C4: the movers that this entry point placed lately are
            // apportioned over the first tier by headroom (largest
            // remainder): the one whose share is furthest behind takes
            // this one, so that a drained edge's few dozen movers follow
            // the headroom-weighted shares closely rather than by chance (a
            // weighted rendezvous of 36 movers put 5 where 6.9 were due,
            // 1/1.37 of the share). A rebalanced listener fills receivers
            // up to the rebalance's level; a drained or lost edge's movers
            // fill the survivors up to the target, or higher if they do
            // not fit under it.
            let first_tier = ranked[0].0;
            let pool = ranked
                .iter()
                .enumerate()
                .filter(|(_, (tier, _, _))| *tier == first_tier)
                .map(|(index, (_, _, cache))| (index, *cache))
                .collect::<Vec<_>>();
            if let Some(index) = apportion(&pool, request.recent_moves, level) {
                let chosen = ranked.remove(index);
                ranked.insert(0, chosen);
            }
        }
        if ranked.is_empty() {
            // Nothing takes the listener: the caller spills it, or sends
            // it to the failover region.
            return Err(AssignmentError::NoCacheAvailable);
        }
        let (tier, _, cache) = ranked.remove(0);
        let reason = match tier {
            _ if moving && cache.holds_playlist() => AssignmentReason::HoldsPlaylist,
            _ if moving => AssignmentReason::NewCopy,
            0 => AssignmentReason::HoldsPlaylist,
            1 => AssignmentReason::NewCopy,
            _ => AssignmentReason::Spill,
        };
        caches.push(assigned(cache, reason));
    }
    // Redundant legs: only where the cache stays under the gate with the
    // listener counted, and never a spill. An edge on a host that no leg of
    // the listener is on first; when none is eligible, the same choice
    // among them all. The host ranks above a remembered leg: a repeat of an
    // answer that put both legs on one host moves its second leg once an
    // edge on another host passes the gate, and the remembered leg's
    // pending charge lapses at its next advertisement.
    let mut second_leg_same_host = false;
    'legs: while caches.len() < usize::from(wanted) {
        for apart in [true, false] {
            let eligible = |cache: &CacheCandidate| {
                !caches.iter().any(|leg| leg.node_id == cache.node_id)
                    && passes_gate(&ranked, cache)
                    && (!apart
                        || !shares_host(cache, &caches, request.held_hosts)
                            && !on_left_host(cache, request.left_hosts))
            };
            if let Some(cache) = stickies.iter().copied().find(|cache| eligible(cache)) {
                second_leg_same_host |= shares_host(cache, &caches, request.held_hosts);
                caches.push(assigned(cache, AssignmentReason::Sticky));
                continue 'legs;
            }
            let pool = ranked
                .iter()
                .enumerate()
                .filter(|(_, (_, _, cache))| eligible(cache))
                .map(|(index, (_, _, cache))| (index, *cache))
                .collect::<Vec<_>>();
            let chosen = if moving {
                // The legs of this answer count as movers placed already, so
                // that two replacements follow the shares as two movers do.
                let mut moves = request.recent_moves.to_vec();
                for leg in &caches {
                    match moves
                        .iter_mut()
                        .find(|(node_id, _)| *node_id == leg.node_id)
                    {
                        Some(entry) => entry.1 += 1,
                        None => moves.push((leg.node_id.clone(), 1)),
                    }
                }
                apportion_at(&pool, &moves, level, request.place_at)
            } else {
                // Another holder by the session's weighted rendezvous, else the
                // edge first in line for the standby copy: a small stream is
                // then held on two edges, its holder and its standby.
                pool.iter()
                    .filter(|(_, cache)| cache.holds_playlist())
                    .max_by(|left, right| {
                        rendezvous_score(session, left.1)
                            .total_cmp(&rendezvous_score(session, right.1))
                            .then_with(|| right.1.node_id.cmp(&left.1.node_id))
                    })
                    .or_else(|| {
                        pool.iter().max_by(|left, right| {
                            left.1
                                .failover_rank
                                .cmp(&right.1.failover_rank)
                                .then_with(|| {
                                    rendezvous_score(session, left.1)
                                        .total_cmp(&rendezvous_score(session, right.1))
                                })
                                .then_with(|| right.1.node_id.cmp(&left.1.node_id))
                        })
                    })
                    .map(|(index, _)| *index)
            };
            if let Some(index) = chosen {
                let (_, _, cache) = ranked.remove(index);
                second_leg_same_host |= shares_host(cache, &caches, request.held_hosts);
                caches.push(assigned(cache, AssignmentReason::SecondPath));
                continue 'legs;
            }
        }
        break;
    }
    let legs = u8::try_from(caches.len()).unwrap_or(MAX_PATHS);
    let other_serving = regional
        .iter()
        .any(|cache| healthy(cache) && !caches.iter().any(|leg| leg.node_id == cache.node_id));
    let short = short_grant(request.paths, wanted, legs, other_serving);
    // A listener that holds a leg and is granted nothing hears why, as one
    // granted fewer legs than asked does (review of B Y2, finding 1).
    let none_named = match short {
        Some(why) if request.redundant => AssignmentError::NoLegGranted(why),
        _ => AssignmentError::NoCacheAvailable,
    };
    if rebalancing && legs == 0 && wanted > 0 {
        // No receiver under the level: no cache of the region is named.
        // A request for a spare alone is answered as any.
        return Err(none_named);
    }
    // The spare (B 3.6, the lead's decision of 24 Nov): a holder of the
    // playlist by the session's rendezvous weighted by its headroom under
    // the redundant leg's gate (as a leg is placed), else a cache that
    // admits. A spare carries nothing until a hedge, and a hedge is a leg,
    // so an edge's share of a dead edge's hedges follows its headroom to the
    // gate, not to full load (review of B Y5, finding 4: by `FULL_LOAD -
    // load` the edge with least headroom took 1.65 times its share). Only
    // when no cache of a pass is under the gate is one over it named, by
    // its headroom to full load; its edge refuses a hedge when overloaded
    // (edge rule 9) and counts one it takes over the gate.
    let mut spare = false;
    if request.spare {
        let free = |cache: &&CacheCandidate| {
            !caches.iter().any(|leg| leg.node_id == cache.node_id)
        };
        let room = |cache: &CacheCandidate| {
            f64::from(gate) - f64::from(cache.effective_load_permille()) - listener_permille
        };
        fn best<'c>(
            session: &str,
            pool: Vec<&'c CacheCandidate>,
            under: bool,
            room: &dyn Fn(&CacheCandidate) -> f64,
        ) -> Option<&'c CacheCandidate> {
            pool.into_iter()
                .filter(|cache| (room(cache) > 0.0) == under)
                .map(|cache| {
                    let score = if under {
                        weighted_rendezvous(session, cache, room(cache))
                    } else {
                        rendezvous_score(session, cache)
                    };
                    (score, cache)
                })
                .max_by(|left, right| {
                    left.0
                        .total_cmp(&right.0)
                        .then_with(|| right.1.node_id.cmp(&left.1.node_id))
                })
                .map(|(_, cache)| cache)
        }
        // The spare the listener holds, if it may still be one: free (not a
        // leg of this answer), a holder that is healthy or a cache that
        // admits.
        let held_spare = request.spare_held.and_then(|name| {
            regional.iter().copied().find(|cache| {
                cache.node_id == name
                    && free(cache)
                    && ((cache.holds_playlist() && healthy(cache)) || admits(cache))
            })
        });
        // Under the gate before over it; a holder (a warm copy) before a
        // cache that admits and must take a copy first, whatever the host
        // (hosttag review, finding 4: a hedge to a cold edge waits about a
        // second for its copy, longer than a sibling's stop costs); among
        // either, a host that no leg is on (nor the caches it left) first.
        // A held spare under the gate is kept; over it, the passes under the
        // gate look for another, and it is kept when none is.
        let unders: &[bool] = if held_spare.is_some() {
            &[true]
        } else {
            &[true, false]
        };
        let not_held = |cache: &&CacheCandidate| {
            held_spare.is_none_or(|held| held.node_id != cache.node_id)
        };
        let kept = held_spare.filter(|cache| room(cache) > 0.0);
        let chosen = kept
            .or_else(|| {
                unders.iter().find_map(|under| {
                    [true, false].into_iter().find_map(|holders| {
                        [true, false].into_iter().find_map(|apart| {
                            let pool = regional
                                .iter()
                                .copied()
                                .filter(free)
                                .filter(not_held)
                                .filter(|cache| {
                                    !apart
                                        || !shares_host(cache, &caches, request.held_hosts)
                                            && !on_left_host(cache, request.left_hosts)
                                })
                                .filter(|cache| {
                                    if holders {
                                        cache.holds_playlist() && healthy(cache)
                                    } else {
                                        admits(cache)
                                    }
                                })
                                .collect::<Vec<_>>();
                            best(session, pool, *under, &room)
                        })
                    })
                })
            })
            .or(held_spare);
        if let Some(cache) = chosen {
            ranked.retain(|(_, _, other)| other.node_id != cache.node_id);
            caches.push(assigned(cache, AssignmentReason::Spare));
            spare = true;
        }
    }
    // The caches after these are where the reader goes when its cache
    // fails. A cache that holds the playlist comes first, and then the
    // caches in the order in which they take the standby copy, so that the
    // reader finds a warm copy at its first alternate. The readers of one
    // playlist share this order. Admission at the alternate sends on the
    // readers that do not fit.
    ranked.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| right.2.failover_rank.cmp(&left.2.failover_rank))
            .then_with(|| right.1.total_cmp(&left.1))
            .then_with(|| left.2.node_id.cmp(&right.2.node_id))
    });
    let named = caches.len();
    for (_tier, _, cache) in ranked {
        if caches.len() >= named + policy.max_alternates {
            break;
        }
        if caches.iter().any(|chosen| chosen.node_id == cache.node_id) {
            continue;
        }
        caches.push(assigned(cache, AssignmentReason::Alternate));
    }
    if caches.is_empty() {
        return Err(none_named);
    }
    Ok(Assignment {
        caches,
        pinned: false,
        legs,
        spare,
        second_leg_same_host,
        short,
    })
}

/// A heavy session's answer (place240 rules 1 and 2), or `None` to place it
/// as any other (an estimated rate with no room anywhere).
///
/// The rule: the leg it depends on is the cache it uses (or was given
/// lately) while that cache is healthy and not in distress, as before.
/// Otherwise the caches that admit sessions and that the session fits (its
/// CPU cost, the tee's too where the cache does not hold the playlist yet,
/// within the cache's heavy budget; its rate within the cache's heavy
/// egress; the cache not in distress) are ranked by their fill with it (the
/// larger of CPU and egress against their limits, the sessions this entry
/// placed since their advertisements counted at once); among those within
/// `heavy_band_permille` of the emptiest, the session's rendezvous hash
/// chooses, so that a session that asks again finds the same cache while
/// the room is level. A region where a healthy cache advertises no room
/// (an older node, a roll in progress) is placed as before. With no cache
/// it fits, a measured session is
/// refused ([`AssignmentError::HeavyFull`]). One leg; the spare and the
/// failover order are the next caches it fits, emptiest first.
fn assign_heavy(
    request: AssignmentRequest<'_>,
    policy: AssignmentPolicy,
    regional: &[&CacheCandidate],
    heavy: HeavySession,
    healthy: &dyn Fn(&CacheCandidate) -> bool,
    admits: &dyn Fn(&CacheCandidate) -> bool,
) -> Option<Result<Assignment, AssignmentError>> {
    // A region where a cache that could serve advertises no room (an
    // older node, a roll in progress) is placed as before.
    if regional
        .iter()
        .any(|cache| healthy(cache) && cache.heavy.is_none())
    {
        return None;
    }
    let session = request.session.unwrap_or("");
    let fill = |cache: &CacheCandidate| {
        cache
            .heavy
            .map_or(cache.effective_load_permille(), |room| room.fill(&heavy))
    };
    // (A cache without room here is unhealthy, and not admitted.)
    let distressed = |cache: &CacheCandidate| cache.heavy.is_some_and(|room| room.distress);
    let mut pool: Vec<&CacheCandidate> = regional
        .iter()
        .copied()
        .filter(|cache| admits(cache) && cache.heavy.is_none_or(|room| room.fits(&heavy)))
        .collect();
    // The emptiest within the band, by the session's hash (salted for the
    // spare, so that it is not the leg's order).
    let choose = |pool: &[&CacheCandidate], salt: &str| -> Option<usize> {
        let least = pool.iter().map(|cache| fill(cache)).min()?;
        let band = least.saturating_add(policy.heavy_band_permille);
        pool.iter()
            .enumerate()
            .filter(|(_, cache)| fill(cache) <= band)
            .max_by(|(_, left), (_, right)| {
                let score = |cache: &CacheCandidate| {
                    if session.is_empty() {
                        0
                    } else {
                        fnv1a64(format!("{session}{salt}").as_bytes(), cache.node_id.as_bytes())
                    }
                };
                score(left)
                    .cmp(&score(right))
                    .then_with(|| fill(right).cmp(&fill(left)))
                    .then_with(|| right.node_id.cmp(&left.node_id))
            })
            .map(|(index, _)| index)
    };
    let wanted = if request.redundant {
        0
    } else {
        request.paths.min(1)
    };
    let mut caches: Vec<AssignedCache> = Vec::new();
    if wanted > 0 {
        let sticky = request
            .current
            .into_iter()
            .chain(request.remembered.iter().map(String::as_str))
            .find_map(|name| {
                regional
                    .iter()
                    .copied()
                    .find(|cache| cache.node_id == name && healthy(cache) && !distressed(cache))
            });
        match sticky {
            Some(cache) => caches.push(assigned(cache, AssignmentReason::Sticky)),
            None => {
                let Some(index) = choose(&pool, "") else {
                    return heavy.refuse.then_some(Err(AssignmentError::HeavyFull));
                };
                let cache = pool.remove(index);
                let reason = if cache.holds_playlist() {
                    AssignmentReason::HoldsPlaylist
                } else {
                    AssignmentReason::NewCopy
                };
                caches.push(assigned(cache, reason));
            }
        }
        pool.retain(|cache| caches.iter().all(|leg| leg.node_id != cache.node_id));
    }
    let legs = u8::try_from(caches.len()).unwrap_or(MAX_PATHS);
    let mut spare = false;
    if request.spare {
        if let Some(index) = choose(&pool, "/spare") {
            let cache = pool.remove(index);
            caches.push(assigned(cache, AssignmentReason::Spare));
            spare = true;
        }
    }
    pool.sort_by(|left, right| {
        fill(left)
            .cmp(&fill(right))
            .then_with(|| left.node_id.cmp(&right.node_id))
    });
    for cache in pool.into_iter().take(policy.max_alternates) {
        caches.push(assigned(cache, AssignmentReason::Alternate));
    }
    let other_serving = regional.iter().any(|cache| {
        healthy(cache) && !caches.iter().take(usize::from(legs)).any(|leg| leg.node_id == cache.node_id)
    });
    let short = short_grant(request.paths, wanted, legs, other_serving);
    if caches.is_empty() {
        return Some(Err(match short {
            Some(why) if request.redundant => AssignmentError::NoLegGranted(why),
            _ => AssignmentError::NoCacheAvailable,
        }));
    }
    Some(Ok(Assignment {
        caches,
        pinned: false,
        legs,
        spare,
        second_leg_same_host: false,
        short,
    }))
}

/// Why a request for `asked` legs was granted `legs` (B Y2, the lead's
/// decision): the bit rate cut the legs `wanted`; else no other edge of the
/// region serves the stream (`other_serving`: fresh and not draining); else
/// none passed the gate. `None` for a full grant.
fn short_grant(asked: u8, wanted: u8, legs: u8, other_serving: bool) -> Option<ShortGrant> {
    if wanted < asked.min(MAX_PATHS) {
        Some(ShortGrant::Bitrate)
    } else if legs < wanted {
        Some(if other_serving {
            ShortGrant::Gate
        } else {
            ShortGrant::OneEdge
        })
    } else {
        None
    }
}

/// A UDP client's assignment when [`assign`] finds no cache with room for
/// it (wave 8 local proofs): the caches of the region that are fresh, not
/// draining, not overloaded and under full load are ranked again without
/// the room check, and the first is a spill. `free_bps` counts the readers
/// that entry points sent since the cache last advertised, each charged an
/// LL-HLS reader's share (a 1 ms playlist's request rate: ~2 Mbit/s a
/// reader where a 1 ms Opus UDP client sends ~0.8), so a crowd of UDP
/// clients that starts within one advertisement interval used up every
/// edge's estimate after ~40 each, and the rest were answered with the
/// failover region alone: London listeners subscribed in New York. A UDP
/// edge's own admission (`--client-udp-admit-by-load`) refuses a client it
/// cannot take, and the client asks again with that edge in its skip set;
/// the failover region is for a region that cannot serve, not for an
/// estimate that ran out. `Err` when no cache of the region is healthy.
pub fn assign_udp_in_region(
    request: AssignmentRequest<'_>,
    policy: AssignmentPolicy,
    candidates: &[CacheCandidate],
) -> Result<Assignment, AssignmentError> {
    if request.pin.is_some() {
        return assign(request, policy, candidates);
    }
    let serving = |cache: &CacheCandidate| {
        cache.region == request.region
            && cache.serves_readers
            && cache.playback_base_url.is_some()
            && cache.fresh
            && !cache.draining
    };
    let healthy = |cache: &CacheCandidate| {
        serving(cache) && !cache.overloaded && cache.load_permille < FULL_LOAD_PERMILLE
    };
    // Every healthy cache by the session's weighted rendezvous score alone
    // (its weight the cache's own headroom), not by the tiers of `assign`:
    // a crowd that spills is spread over the region, not all sent to the
    // one cache that holds the playlist under the spread target (a slot
    // that has just joined and warmed took all 60 of a lab crowd and
    // overloaded).
    let session = request.session.unwrap_or("");
    let mut ranked = candidates
        .iter()
        .filter(|cache| healthy(cache))
        .map(|cache| (rendezvous_score(session, cache), cache))
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .0
            .total_cmp(&left.0)
            .then_with(|| left.1.node_id.cmp(&right.1.node_id))
    });
    let mut caches = Vec::new();
    if let Some(current) = request.current {
        if let Some((_, cache)) = ranked.iter().find(|(_, cache)| cache.node_id == current) {
            caches.push(assigned(cache, AssignmentReason::Sticky));
        }
    }
    for (_, cache) in &ranked {
        if caches.len() > policy.max_alternates {
            break;
        }
        if caches.iter().any(|chosen| chosen.node_id == cache.node_id) {
            continue;
        }
        let reason = if caches.is_empty() {
            AssignmentReason::Spill
        } else {
            AssignmentReason::Alternate
        };
        caches.push(assigned(cache, reason));
    }
    let Some(spilled) = caches.first().map(|cache| cache.node_id.clone()) else {
        return Err(AssignmentError::NoCacheAvailable);
    };
    // One leg, and why not the legs asked: as `assign` reckons it (review
    // of B Y2, finding 8).
    let wanted = if request
        .playlist_bps
        .is_some_and(|bps| bps > policy.two_path_max_bps)
    {
        request.paths.min(1)
    } else {
        request.paths.min(MAX_PATHS)
    };
    let other_serving = candidates
        .iter()
        .any(|cache| serving(cache) && cache.node_id != spilled);
    Ok(Assignment {
        caches,
        pinned: false,
        legs: 1,
        spare: false,
        second_leg_same_host: false,
        short: short_grant(request.paths, wanted, 1, other_serving),
    })
}

fn assigned(cache: &CacheCandidate, reason: AssignmentReason) -> AssignedCache {
    AssignedCache {
        node_id: cache.node_id.clone(),
        playback_base_url: cache.playback_base_url.clone().unwrap_or_default(),
        reason,
        load_permille: cache.load_permille,
        holds_playlist: cache.holds_playlist(),
        host: cache.host.clone(),
    }
}

/// Whether a cache is on the host of a cache the listener has just left.
fn on_left_host(cache: &CacheCandidate, left_hosts: &[String]) -> bool {
    cache
        .host
        .as_deref()
        .is_some_and(|host| left_hosts.iter().any(|left| left == host))
}

/// Whether a cache shares a physical host with a leg already chosen or
/// held: both name a host and it is the same. A cache or a leg without a
/// tag shares nothing.
fn shares_host(cache: &CacheCandidate, legs: &[AssignedCache], held_hosts: &[String]) -> bool {
    cache.host.as_deref().is_some_and(|host| {
        held_hosts.iter().any(|held| held == host)
            || legs.iter().any(|leg| leg.host.as_deref() == Some(host))
    })
}

/// The load that movers fill the survivors up to (see [`apportion`]).
#[derive(Debug, Clone, Copy)]
pub struct Level {
    /// Permille: the rebalance target for a drain's or a kill's movers,
    /// the receivers' level for a rebalance.
    pub at: u32,
    /// What one listener costs (permille), when known: the level rises
    /// above `at` to where the movers fit.
    pub listener_permille: Option<f64>,
    /// The region's movers per mover this entry point placed (its entry
    /// points; at least 1).
    pub movers_scale: f64,
}

/// The level at which `amount` permille poured over caches at `loads` fills
/// them evenly: sum(max(level - load, 0)) = amount.
pub fn fill_level(loads: &[f64], amount: f64) -> f64 {
    let mut sorted = loads.to_vec();
    sorted.sort_by(f64::total_cmp);
    let mut below = 0.0;
    for (index, load) in sorted.iter().enumerate() {
        below += load;
        let level = (amount + below) / (index + 1) as f64;
        if sorted.get(index + 1).is_none_or(|next| level <= *next) {
            return level;
        }
    }
    amount
}

/// Each cache's headroom to `level` (see [`apportion`]): its share of the
/// movers is its headroom over the pool's. The level is `level.at`, raised
/// to where `movers` listeners at `level.listener_permille` each fill the
/// caches evenly when they do not fit under it; with no cache under the
/// level, the headroom to full load (1,000) as before.
pub fn headroom_to_level(loads: &[f64], movers: f64, level: Level) -> Vec<f64> {
    let mut at = f64::from(level.at);
    if let Some(cost) = level.listener_permille.filter(|cost| *cost > 0.0) {
        at = at.max(fill_level(loads, movers * level.movers_scale.max(1.0) * cost));
    }
    let to_level = loads
        .iter()
        .map(|load| (at - load).max(0.0))
        .collect::<Vec<_>>();
    if to_level.iter().sum::<f64>() >= 1.0 {
        to_level
    } else {
        loads
            .iter()
            .map(|load| (f64::from(FULL_LOAD_PERMILLE) - load).max(1.0))
            .collect()
    }
}

/// Wave 9 C4: which of `pool` (index into the ranking, cache; in ranking
/// order) a mover goes to, so that the movers placed lately
/// (`recent_moves`: cache, count) and this one follow each cache's share of
/// the pool's headroom: the cache whose due share less what it took is the
/// largest. Ties go to the earlier in the ranking (the session's
/// rendezvous). `None` for an empty pool.
///
/// The headroom is to a level (wave 9 review), not to full load: with the
/// headroom to 1,000, an edge at 776 permille (its LL-HLS readers' CPU)
/// took a drain's movers and shed them again as over its target.
fn apportion(
    pool: &[(usize, &CacheCandidate)],
    recent_moves: &[(String, u32)],
    level: Level,
) -> Option<usize> {
    apportion_at(pool, recent_moves, level, None)
}

/// [`apportion`], where the mover holds its place at `place_at` already (a
/// hedge leg): that cache keeps it while its due share less what it took
/// is above zero, so that the hedges of a dead edge move only where their
/// edge is over its share.
fn apportion_at(
    pool: &[(usize, &CacheCandidate)],
    recent_moves: &[(String, u32)],
    level: Level,
    place_at: Option<&str>,
) -> Option<usize> {
    let taken = |cache: &CacheCandidate| {
        recent_moves
            .iter()
            .filter(|(node_id, _)| *node_id == cache.node_id)
            .map(|(_, count)| f64::from(*count))
            .sum::<f64>()
    };
    let total: f64 = pool.iter().map(|(_, cache)| taken(cache)).sum::<f64>() + 1.0;
    let loads = pool
        .iter()
        .map(|(_, cache)| f64::from(cache.load_permille))
        .collect::<Vec<_>>();
    let rooms = headroom_to_level(&loads, total, level);
    let room_of = |cache: &CacheCandidate| {
        pool.iter()
            .position(|(_, other)| other.node_id == cache.node_id)
            .map_or(0.0, |at| rooms[at])
    };
    let headroom = |cache: &CacheCandidate| room_of(cache);
    let whole: f64 = rooms.iter().sum();
    let mut best: Option<(f64, usize)> = None;
    for (index, cache) in pool {
        let deficit = headroom(cache) / whole * total - taken(cache);
        if place_at == Some(cache.node_id.as_str()) && deficit > 1e-9 {
            return Some(*index);
        }
        if best.is_none_or(|(most, _)| deficit > most + 1e-9) {
            best = Some((deficit, *index));
        }
    }
    best.map(|(_, index)| index)
}

/// Rank of an edge for the standby copy of a playlist. Every node computes
/// the same rank from the two names, so the edges of a region agree on who
/// takes the copy, and the entry points on where a reader goes next, without
/// a word between them.
pub fn standby_rank(node_id: &str, stream_id: u64) -> u64 {
    // FNV-1a, which is the same in every build of every node.
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in stream_id
        .to_be_bytes()
        .iter()
        .chain(node_id.as_bytes().iter())
    {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // The last bytes of similar names differ little: mix once more.
    hash ^ (hash >> 29)
}

/// A node's tag (B rule R1): how a listener's other legs (`o=`) are kept
/// at an edge, eight bytes each, and matched against the region's nodes.
/// FNV-1a of the node id, the same on every node and in every build; never
/// 0, which stands for no node.
pub fn node_tag(node_id: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in node_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash.max(1)
}

/// Weighted rendezvous score of a cache for a session. The cache with the
/// highest score wins. The weight is the free capacity of the cache, so a
/// cache with twice the room takes about twice the new sessions.
///
/// A request without a session identifier has no hash to spread it, so the
/// score is the free capacity itself and the least loaded cache wins.
fn rendezvous_score(session: &str, cache: &CacheCandidate) -> f64 {
    let free = f64::from(
        FULL_LOAD_PERMILLE
            .saturating_sub(cache.effective_load_permille())
            .max(1),
    );
    if session.is_empty() {
        return free;
    }
    let hash = fnv1a64(session.as_bytes(), cache.node_id.as_bytes());
    // Map the hash into (0, 1). The top 53 bits fit an f64 exactly.
    let unit = ((hash >> 11) as f64 + 0.5) / (1_u64 << 53) as f64;
    -free / unit.ln()
}

/// A weighted rendezvous score of a cache for a session with the weight
/// given (at least 1): the cache with the highest wins, each in proportion
/// to its weight.
fn weighted_rendezvous(session: &str, cache: &CacheCandidate, weight: f64) -> f64 {
    let weight = weight.max(1.0);
    if session.is_empty() {
        return weight;
    }
    let hash = fnv1a64(session.as_bytes(), cache.node_id.as_bytes());
    let unit = ((hash >> 11) as f64 + 0.5) / (1_u64 << 53) as f64;
    -weight / unit.ln()
}

/// FNV-1a over the session and the cache name, with a final avalanche. The
/// result must be the same on every node and in every build, so it does not
/// use the standard library hasher.
fn fnv1a64(session: &[u8], node_id: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in session.iter().chain(&[0xff]).chain(node_id) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    hash ^ (hash >> 33)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn cache(node_id: &str, load_permille: u32, head_part: Option<u64>) -> CacheCandidate {
        CacheCandidate {
            node_id: node_id.into(),
            region: "london".into(),
            playback_base_url: Some(format!("https://{node_id}.example/live")),
            serves_readers: true,
            fresh: true,
            draining: false,
            overloaded: false,
            load_permille,
            head_part,
            free_bps: None,
            session_charge_bps: None,
            updated_unix_ms: 0,
            failover_rank: None,
            pending_permille: 0,
            host: None,
            heavy: None,
        }
    }

    /// Wave 9 C4: the movers of a drained edge spread over the survivors
    /// in proportion to their headroom (the spread tiers sent them all to
    /// the survivors under 60 %), apportioned against the movers placed
    /// before them; a rebalanced listener goes to none over the rebalance
    /// target while one under it has room.
    #[test]
    fn movers_spread_over_the_survivors_by_headroom() {
        let caches = [
            cache("edge-2", 290, Some(900)),
            cache("edge-3", 720, Some(900)),
            cache("edge-4", 520, Some(900)),
        ];
        // `count` movers, each told of those placed before it.
        let place = |caches: &[CacheCandidate], policy: AssignmentPolicy, count: usize| {
            let mut placed: Vec<(String, u32)> = Vec::new();
            for index in 0..count {
                let session = format!("mover-{index}");
                let moving = AssignmentRequest {
                    moving: true,
                    recent_moves: &placed,
                    ..request(&session)
                };
                let first = assign(moving, policy, caches).unwrap().caches[0]
                    .node_id
                    .clone();
                match placed.iter_mut().find(|(node_id, _)| *node_id == first) {
                    Some(entry) => entry.1 += 1,
                    None => placed.push((first, 1)),
                }
            }
            placed
                .into_iter()
                .collect::<HashMap<String, u32>>()
        };
        // The wave 9 local F1, by headroom to the target (wave 9 review):
        // 510 / 80 / 280 of 870 and 36 movers: 21.1 / 3.3 / 11.6 due; within
        // one of each.
        let moved = place(&caches, AssignmentPolicy::default(), 36);
        for (name, due) in [("edge-2", 21.10), ("edge-3", 3.31), ("edge-4", 11.59)] {
            let got = f64::from(moved.get(name).copied().unwrap_or(0));
            assert!((got - due).abs() <= 1.0, "{name}: {moved:?}");
        }
        // As a new reader (the flag off), edge-3 above the spread target
        // gets none.
        let off = AssignmentPolicy {
            move_headroom: false,
            ..AssignmentPolicy::default()
        };
        let moved = place(&caches, off, 36);
        assert!(!moved.contains_key("edge-3"), "{moved:?}");
        // A survivor over the target takes none of a drain's movers while
        // the others have room under it...
        let mut over = caches.clone();
        over[1].load_permille = 850;
        let moved = place(&over, AssignmentPolicy::default(), 36);
        assert!(!moved.contains_key("edge-3"), "{moved:?}");
        // ...but a listener that an edge over the target sent away goes
        // only under the target while a cache there has room.
        let mut placed: Vec<(String, u32)> = Vec::new();
        for index in 0..36 {
            let session = format!("rebalanced-{index}");
            let request = AssignmentRequest {
                moving: true,
                rebalancing: true,
                recent_moves: &placed,
                ..request(&session)
            };
            let first = assign(request, AssignmentPolicy::default(), &over).unwrap().caches[0]
                .node_id
                .clone();
            assert_ne!(first, "edge-3");
            match placed.iter_mut().find(|(node_id, _)| *node_id == first) {
                Some(entry) => entry.1 += 1,
                None => placed.push((first, 1)),
            }
        }
        // A rebalanced listener goes only where it keeps the receiver under
        // the receivers' level (725 with the default margin), counted with
        // what it costs: edge-3 at 720 is under the target but not a
        // receiver (wave 9 review), edge-2 at 290 and edge-4 at 520 are.
        for index in 0..40 {
            let session = format!("rebalanced-k-{index}");
            let request = AssignmentRequest {
                moving: true,
                rebalancing: true,
                listener_permille: Some(16.0),
                ..request(&session)
            };
            let first = assign(request, AssignmentPolicy::default(), &caches).unwrap().caches[0]
                .node_id
                .clone();
            assert_ne!(first, "edge-3", "{session}");
        }
        // ...unless they do not fit under it: at 40 permille a listener, 36
        // movers fill the three to 1,033, and edge-3 takes 183 of 1,440 of
        // the room (4.6).
        let place_with = |caches: &[CacheCandidate], count: usize, cost: f64| {
            let mut placed: Vec<(String, u32)> = Vec::new();
            for index in 0..count {
                let session = format!("mover-{index}");
                let moving = AssignmentRequest {
                    moving: true,
                    recent_moves: &placed,
                    listener_permille: Some(cost),
                    ..request(&session)
                };
                let first = assign(moving, AssignmentPolicy::default(), caches)
                    .unwrap()
                    .caches[0]
                    .node_id
                    .clone();
                match placed.iter_mut().find(|(node_id, _)| *node_id == first) {
                    Some(entry) => entry.1 += 1,
                    None => placed.push((first, 1)),
                }
            }
            placed.into_iter().collect::<HashMap<String, u32>>()
        };
        let moved = place_with(&over, 36, 40.0);
        assert!((3..=6).contains(&moved["edge-3"]), "{moved:?}");
        // At 10 permille they fit under the target: edge-3 takes none.
        let moved = place_with(&over, 36, 10.0);
        assert!(!moved.contains_key("edge-3"), "{moved:?}");
        // Every cache over the target: still assigned, by headroom.
        let all_over = [
            cache("edge-2", 900, Some(900)),
            cache("edge-3", 820, Some(900)),
        ];
        let moved = place(&all_over, AssignmentPolicy::default(), 14);
        assert_eq!(moved.get("edge-3"), Some(&9), "{moved:?}");
        assert_eq!(moved.get("edge-2"), Some(&5), "{moved:?}");
    }

    /// The legs of an answer, by name.
    fn legs(assignment: &Assignment) -> Vec<&str> {
        assignment.caches[..usize::from(assignment.legs)]
            .iter()
            .map(|cache| cache.node_id.as_str())
            .collect()
    }

    /// B 3.6: the second leg of a join goes to another holder of the
    /// playlist by the session's weighted rendezvous (by headroom), else to
    /// the edge first in line for the standby copy.
    #[test]
    fn the_second_path_is_another_holder_by_headroom_else_the_standby_copy() {
        fn two(session: &str) -> AssignmentRequest<'_> {
            AssignmentRequest {
                paths: 2,
                ..request(session)
            }
        }
        let caches = [
            cache("edge-1", 100, Some(9)),
            cache("edge-2", 100, Some(9)),
            cache("edge-3", 400, Some(9)),
            CacheCandidate {
                failover_rank: Some(50),
                ..cache("edge-4", 0, None)
            },
        ];
        let mut second = HashMap::<String, usize>::new();
        for index in 0..3_000 {
            let session = format!("listener-{index}");
            let assignment = assign(two(&session), AssignmentPolicy::default(), &caches).unwrap();
            assert_eq!(assignment.legs, 2);
            assert!(!assignment.spare);
            assert_eq!(assignment.caches[1].reason, AssignmentReason::SecondPath);
            assert_ne!(assignment.caches[0].node_id, assignment.caches[1].node_id);
            assert!(assignment.caches[1].holds_playlist, "{assignment:?}");
            // The same answer each time it asks.
            assert_eq!(
                assignment,
                assign(two(&session), AssignmentPolicy::default(), &caches).unwrap()
            );
            *second
                .entry(assignment.caches[1].node_id.clone())
                .or_default() += 1;
        }
        // Free capacity 900 / 900 / 600: edge-3 is the second leg of
        // 0.75 x 0.4 = 30 % of listeners, each of the others of 35 %.
        let share = second["edge-3"] as f64 / 3_000.0;
        assert!(share > 0.26 && share < 0.34, "{second:?}");
        assert!(!second.contains_key("edge-4"), "{second:?}");
        // One holder: the second leg is the edge first for the standby
        // copy, for every listener.
        let ranked = [
            cache("edge-1", 100, Some(9)),
            CacheCandidate {
                failover_rank: Some(10),
                ..cache("edge-2", 0, None)
            },
            CacheCandidate {
                failover_rank: Some(30),
                ..cache("edge-3", 300, None)
            },
            CacheCandidate {
                failover_rank: Some(20),
                ..cache("edge-4", 0, None)
            },
        ];
        for index in 0..50 {
            let session = format!("listener-{index}");
            let assignment = assign(two(&session), AssignmentPolicy::default(), &ranked).unwrap();
            assert_eq!(legs(&assignment), ["edge-1", "edge-3"], "{session}");
        }
        // The standby edge over the target: the next in line.
        let mut busy = ranked.clone();
        busy[2].load_permille = 810;
        let assignment = assign(two("listener-1"), AssignmentPolicy::default(), &busy).unwrap();
        assert_eq!(legs(&assignment), ["edge-1", "edge-4"]);
    }

    fn on_host(host: &str, cache: CacheCandidate) -> CacheCandidate {
        CacheCandidate {
            host: Some(host.into()),
            ..cache
        }
    }

    /// The host of a cache of an answer.
    fn host_of<'a>(caches: &'a [CacheCandidate], node_id: &str) -> Option<&'a str> {
        caches
            .iter()
            .find(|cache| cache.node_id == node_id)
            .and_then(|cache| cache.host.as_deref())
    }

    /// D 5 (seam to B): of three edges two share a host; a listener's two
    /// legs are on two hosts, a redundant leg avoids the host of the leg it
    /// holds, the spare prefers a third host, and an edge on another host
    /// over the gate leaves the same-host choice standing, said so.
    #[test]
    fn the_second_path_is_on_another_host_when_one_is_eligible() {
        fn two(session: &str) -> AssignmentRequest<'_> {
            AssignmentRequest {
                paths: 2,
                ..request(session)
            }
        }
        let caches = [
            on_host("host-a", cache("edge-1", 100, Some(9))),
            on_host("host-a", cache("edge-2", 100, Some(9))),
            on_host("host-b", cache("edge-3", 100, Some(9))),
        ];
        let mut first = HashMap::<String, usize>::new();
        for index in 0..600 {
            let session = format!("listener-{index}");
            let assignment = assign(two(&session), AssignmentPolicy::default(), &caches).unwrap();
            let legs = legs(&assignment);
            assert_eq!(legs.len(), 2);
            assert!(legs.contains(&"edge-3"), "{session}: {assignment:?}");
            assert!(!assignment.second_leg_same_host);
            assert_eq!(assignment.caches[0].host, host_of(&caches, legs[0]).map(str::to_owned));
            *first.entry(legs[0].to_owned()).or_default() += 1;
        }
        // The first leg is chosen as before: the host rule moves only the
        // second.
        assert_eq!(first.len(), 3, "{first:?}");
        // A listener that holds a leg on host-a: its new leg is edge-3.
        let held = ["host-a".to_owned()];
        let only_free = [caches[1].clone(), caches[2].clone()];
        for index in 0..100 {
            let session = format!("listener-{index}");
            let redundant = AssignmentRequest {
                paths: 1,
                redundant: true,
                held_hosts: &held,
                ..request(&session)
            };
            let assignment = assign(redundant, AssignmentPolicy::default(), &only_free).unwrap();
            assert_eq!(legs(&assignment), ["edge-3"], "{session}");
            assert!(!assignment.second_leg_same_host);
        }
        // The spare: a third host when there is one.
        let mut spread = caches.to_vec();
        spread.push(on_host("host-c", cache("edge-4", 100, Some(9))));
        for index in 0..600 {
            let session = format!("listener-{index}");
            let ask = AssignmentRequest {
                spare: true,
                ..two(&session)
            };
            let assignment = assign(ask, AssignmentPolicy::default(), &spread).unwrap();
            assert!(assignment.spare);
            let leg_hosts = legs(&assignment)
                .iter()
                .map(|leg| host_of(&spread, leg))
                .collect::<Vec<_>>();
            assert_ne!(leg_hosts[0], leg_hosts[1], "{session}: {assignment:?}");
            let spare = &assignment.caches[2];
            assert!(
                !leg_hosts.contains(&host_of(&spread, &spare.node_id)),
                "{session}: {assignment:?}"
            );
        }
        // The edge on the other host over the gate: the second leg shares
        // a host, and the answer says so.
        let mut busy = caches.clone();
        busy[2].load_permille = 810;
        let assignment = assign(two("listener-1"), AssignmentPolicy::default(), &busy).unwrap();
        let mut legs_busy = legs(&assignment);
        legs_busy.sort_unstable();
        assert_eq!(legs_busy, ["edge-1", "edge-2"]);
        assert!(assignment.second_leg_same_host);
    }

    /// D 5 against B 3.5, 3.8 (integration 2): after an edge dies, the
    /// replacement leg (a mover that holds its other leg, apportioned by
    /// headroom) and the new spare in the same answer avoid the surviving
    /// leg's host when another is eligible, even where the edge that shares
    /// it has the most headroom; and the spare a join is given (the hedge's
    /// target when either leg dies) is on neither leg's host.
    #[test]
    fn a_replacement_leg_and_the_spare_avoid_the_surviving_leg_s_host() {
        // edge-1 died (host-a); edge-2 is the leg that survives (host-b).
        // edge-3 shares host-b and is the emptiest; edge-4 (host-c) and
        // edge-5 (host-d) are on hosts of their own.
        let caches = [
            on_host("host-b", cache("edge-2", 300, Some(9))),
            on_host("host-b", cache("edge-3", 50, Some(9))),
            on_host("host-c", cache("edge-4", 400, Some(9))),
            on_host("host-d", cache("edge-5", 450, Some(9))),
        ];
        let held = ["host-b".to_owned()];
        for index in 0..200 {
            let session = format!("listener-{index}");
            let replacement = AssignmentRequest {
                paths: 1,
                spare: true,
                redundant: true,
                moving: true,
                listener_permille: Some(5.0),
                held_hosts: &held,
                ..request(&session)
            };
            let assignment = assign(
                replacement,
                AssignmentPolicy::default(),
                &caches[1..],
            )
            .unwrap();
            let leg = legs(&assignment)[0];
            assert_ne!(
                host_of(&caches, leg),
                Some("host-b"),
                "{session}: {assignment:?}"
            );
            assert!(!assignment.second_leg_same_host, "{assignment:?}");
            assert!(assignment.spare, "{assignment:?}");
            let spare = &assignment.caches[1];
            assert_eq!(spare.reason, AssignmentReason::Spare);
            let spare_host = host_of(&caches, &spare.node_id);
            assert_ne!(spare_host, Some("host-b"), "{session}: {assignment:?}");
            assert_ne!(spare_host, host_of(&caches, leg), "{session}: {assignment:?}");
        }
        // Only the surviving leg's host has room under the gate: the
        // replacement is made there all the same, and the answer says so.
        let mut full = caches.clone();
        full[2].load_permille = 900;
        full[3].load_permille = 900;
        let replacement = AssignmentRequest {
            paths: 1,
            redundant: true,
            moving: true,
            listener_permille: Some(5.0),
            held_hosts: &held,
            ..request("listener-1")
        };
        let assignment = assign(replacement, AssignmentPolicy::default(), &full[1..]).unwrap();
        assert_eq!(legs(&assignment), ["edge-3"]);
        assert!(assignment.second_leg_same_host);
        // A join's spare (where a hedge goes when either leg dies) is on
        // neither leg's host while a third host has a holder.
        let join = [
            on_host("host-a", cache("edge-1", 100, Some(9))),
            on_host("host-b", cache("edge-2", 100, Some(9))),
            on_host("host-b", cache("edge-3", 50, Some(9))),
            on_host("host-c", cache("edge-4", 600, Some(9))),
        ];
        for index in 0..300 {
            let session = format!("listener-{index}");
            let ask = AssignmentRequest {
                paths: 2,
                spare: true,
                ..request(&session)
            };
            let assignment = assign(ask, AssignmentPolicy::default(), &join).unwrap();
            let leg_hosts: Vec<_> = legs(&assignment)
                .iter()
                .map(|leg| host_of(&join, leg))
                .collect();
            if leg_hosts[0] == leg_hosts[1] {
                continue;
            }
            let spare = &assignment.caches[2];
            assert!(
                !leg_hosts.contains(&host_of(&join, &spare.node_id)),
                "{session}: {assignment:?}"
            );
        }
    }

    /// D 5: every edge on one host gives the choice made without tags, and
    /// says that the second leg shares the host; tags absent (or on one
    /// side only) change nothing.
    #[test]
    fn the_second_path_without_another_host_is_the_present_choice() {
        let untagged = [
            cache("edge-1", 100, Some(9)),
            cache("edge-2", 200, Some(9)),
            CacheCandidate {
                failover_rank: Some(40),
                ..cache("edge-3", 0, None)
            },
        ];
        let tagged = |hosts: [Option<&str>; 3]| {
            untagged
                .iter()
                .zip(hosts)
                .map(|(cache, host)| CacheCandidate {
                    host: host.map(str::to_owned),
                    ..cache.clone()
                })
                .collect::<Vec<_>>()
        };
        let one_host = tagged([Some("host-a"); 3]);
        let distinct = tagged([Some("host-a"), Some("host-b"), Some("host-c")]);
        let one_side = tagged([Some("host-a"), None, None]);
        for index in 0..300 {
            let session = format!("listener-{index}");
            let ask = AssignmentRequest {
                paths: 2,
                spare: true,
                ..request(&session)
            };
            let plain = assign(ask, AssignmentPolicy::default(), &untagged).unwrap();
            assert_eq!(plain.legs, 2);
            assert!(!plain.second_leg_same_host);
            let names = |assignment: &Assignment| {
                assignment
                    .caches
                    .iter()
                    .map(|cache| (cache.node_id.clone(), cache.reason))
                    .collect::<Vec<_>>()
            };
            let same = assign(ask, AssignmentPolicy::default(), &one_host).unwrap();
            assert_eq!(names(&same), names(&plain), "{session}");
            assert!(same.second_leg_same_host, "{session}");
            for caches in [&distinct, &one_side] {
                let assignment = assign(ask, AssignmentPolicy::default(), caches).unwrap();
                assert_eq!(names(&assignment), names(&plain), "{session}");
                assert!(!assignment.second_leg_same_host, "{session}");
            }
        }
        // A held leg on that one host: the new leg stands, said so.
        let held = ["host-a".to_owned()];
        let redundant = AssignmentRequest {
            redundant: true,
            held_hosts: &held,
            ..request("listener-1")
        };
        let assignment = assign(redundant, AssignmentPolicy::default(), &one_host).unwrap();
        assert_eq!(assignment.legs, 1);
        assert!(assignment.second_leg_same_host);
        // Without a tag on the candidates a held host restricts nothing.
        let assignment = assign(redundant, AssignmentPolicy::default(), &untagged).unwrap();
        assert_eq!(assignment.legs, 1);
        assert!(!assignment.second_leg_same_host);
    }

    /// B 3.6: a redundant leg only where its cache stays under the
    /// rebalance target with the listener counted, and never a spill.
    #[test]
    fn a_redundant_leg_is_not_given_over_the_target() {
        let caches = [cache("edge-1", 300, Some(9)), cache("edge-2", 790, Some(9))];
        let two = AssignmentRequest {
            paths: 2,
            listener_permille: Some(16.0),
            ..request("listener")
        };
        let assignment = assign(two, AssignmentPolicy::default(), &caches).unwrap();
        assert_eq!(legs(&assignment), ["edge-1"]);
        // edge-2 is still the failover order.
        assert_eq!(assignment.caches[1].node_id, "edge-2");
        assert_eq!(assignment.caches[1].reason, AssignmentReason::Alternate);
        // At 790 with no cost known it fits under 800.
        let unknown = AssignmentRequest {
            listener_permille: None,
            ..two
        };
        let assignment = assign(unknown, AssignmentPolicy::default(), &caches).unwrap();
        assert_eq!(legs(&assignment), ["edge-1", "edge-2"]);
        // What this entry point sent it since its advert counts.
        let mut pending = caches.clone();
        pending[1].load_permille = 700;
        pending[1].pending_permille = 100;
        let assignment = assign(unknown, AssignmentPolicy::default(), &pending).unwrap();
        assert_eq!(legs(&assignment), ["edge-1"]);
        // An edge that admits nobody gives no leg however light.
        let mut overloaded = caches.clone();
        overloaded[1].load_permille = 100;
        overloaded[1].overloaded = true;
        let assignment = assign(unknown, AssignmentPolicy::default(), &overloaded).unwrap();
        assert_eq!(legs(&assignment), ["edge-1"]);
        // A listener that holds edge-1 and asks for a second leg: none,
        // and nothing spills.
        let held = [caches[1].clone()];
        let redundant = AssignmentRequest {
            paths: 1,
            redundant: true,
            ..two
        };
        let assignment = assign(redundant, AssignmentPolicy::default(), &held).unwrap();
        assert_eq!(assignment.legs, 0);
        assert!(assignment
            .caches
            .iter()
            .all(|cache| cache.reason == AssignmentReason::Alternate));
        let full = [cache("edge-2", FULL_LOAD_PERMILLE, Some(9))];
        assert_eq!(
            assign(redundant, AssignmentPolicy::default(), &full),
            Err(AssignmentError::NoLegGranted(ShortGrant::Gate))
        );
        // Under the target, the held listener's leg is a second path.
        let assignment = assign(
            AssignmentRequest {
                listener_permille: None,
                ..redundant
            },
            AssignmentPolicy::default(),
            &held,
        )
        .unwrap();
        assert_eq!(legs(&assignment), ["edge-2"]);
        assert_eq!(assignment.caches[0].reason, AssignmentReason::SecondPath);
    }

    /// B 3.6: the spare is a holder of the playlist that is not a leg, by
    /// the session, with no load gate; else a cache that admits.
    #[test]
    fn the_spare_is_a_holder_that_is_not_a_leg() {
        let caches = [
            cache("edge-1", 100, Some(9)),
            cache("edge-2", 100, Some(9)),
            cache("edge-3", 100, Some(9)),
            cache("edge-4", 0, None),
        ];
        fn ask(session: &str) -> AssignmentRequest<'_> {
            AssignmentRequest {
                paths: 2,
                spare: true,
                ..request(session)
            }
        }
        let mut spares = HashMap::<String, usize>::new();
        for index in 0..600 {
            let session = format!("listener-{index}");
            let assignment = assign(ask(&session), AssignmentPolicy::default(), &caches).unwrap();
            assert_eq!(assignment.legs, 2);
            assert!(assignment.spare);
            let spare = &assignment.caches[2];
            assert_eq!(spare.reason, AssignmentReason::Spare);
            assert!(spare.holds_playlist, "{assignment:?}");
            assert!(!legs(&assignment).contains(&spare.node_id.as_str()));
            *spares.entry(spare.node_id.clone()).or_default() += 1;
        }
        // Spread over the holders by the session.
        assert_eq!(spares.len(), 3, "{spares:?}");
        assert!(spares.values().all(|count| *count > 120), "{spares:?}");
        // A holder over the redundant leg's gate is passed over for a cache
        // under it (the lead's decision of 24 Nov: a hedge there would be
        // over the gate), and is the spare when no cache under the gate is
        // left: a holder that admits nobody is still a spare then.
        let mut overloaded = caches.clone();
        overloaded[2].overloaded = true;
        overloaded[2].load_permille = 1_200;
        let assignment = assign(ask("listener-1"), AssignmentPolicy::default(), &overloaded).unwrap();
        let mut held = legs(&assignment);
        held.sort_unstable();
        assert_eq!(held, ["edge-1", "edge-2"]);
        assert_eq!(assignment.caches[2].node_id, "edge-4");
        assert_eq!(assignment.caches[2].reason, AssignmentReason::Spare);
        let assignment = assign(
            ask("listener-1"),
            AssignmentPolicy::default(),
            &overloaded[..3],
        )
        .unwrap();
        assert_eq!(assignment.caches[2].node_id, "edge-3");
        assert_eq!(assignment.caches[2].reason, AssignmentReason::Spare);
        // Two holders, both legs: the spare is a cache that admits.
        let two_holders = [caches[0].clone(), caches[1].clone(), caches[3].clone()];
        let assignment = assign(ask("listener-1"), AssignmentPolicy::default(), &two_holders).unwrap();
        assert_eq!(assignment.caches[2].node_id, "edge-4");
        assert_eq!(assignment.caches[2].reason, AssignmentReason::Spare);
        // A spare alone, beside the legs a listener holds.
        let spare_only = AssignmentRequest {
            paths: 0,
            redundant: true,
            ..ask("listener-1")
        };
        let assignment = assign(spare_only, AssignmentPolicy::default(), &caches).unwrap();
        assert_eq!(assignment.legs, 0);
        assert!(assignment.spare);
        assert_eq!(assignment.caches[0].reason, AssignmentReason::Spare);
        assert!(assignment.caches[0].holds_playlist);
    }

    /// The lead's decision after integration 2: a request that names the
    /// spare it holds (`k=`) keeps it while its edge passes the redundant
    /// leg's gate; over the gate another under it is named; with none under
    /// it, the held one is kept. A draining edge, or one that no longer
    /// admits, is never kept nor named (av-mesh-fix-3, task 5).
    #[test]
    fn a_held_spare_is_kept_under_the_gate_and_replaced_over_it() {
        let caches = [
            cache("edge-1", 100, Some(9)),
            cache("edge-2", 100, Some(9)),
            cache("edge-3", 700, Some(9)),
            cache("edge-4", 300, Some(9)),
        ];
        let held = ["edge-1".to_owned()];
        let ask = |session: &'static str, spare_held: &'static str| AssignmentRequest {
            paths: 1,
            spare: true,
            redundant: true,
            spare_held: Some(spare_held),
            listener_permille: Some(5.0),
            ..request(session)
        };
        let leg_and_spare = |assignment: &Assignment| {
            (
                legs(assignment)[0].to_owned(),
                assignment.caches[usize::from(assignment.legs)].node_id.clone(),
            )
        };
        let others = [caches[1].clone(), caches[2].clone(), caches[3].clone()];
        let _ = held;
        for index in 0..200 {
            let session: &'static str = Box::leak(format!("keep-{index}").into_boxed_str());
            // Under the gate: kept (unless it is the leg named).
            let assignment = assign(ask(session, "edge-3"), AssignmentPolicy::default(), &others).unwrap();
            let (leg, spare) = leg_and_spare(&assignment);
            if leg != "edge-3" {
                assert_eq!(spare, "edge-3", "{session}: {assignment:?}");
            }
        }
        // edge-3 over the gate (800 with the listener): another under it.
        let mut over = others.clone();
        over[1].load_permille = 800;
        for index in 0..200 {
            let session: &'static str = Box::leak(format!("over-{index}").into_boxed_str());
            let assignment = assign(ask(session, "edge-3"), AssignmentPolicy::default(), &over).unwrap();
            let (leg, spare) = leg_and_spare(&assignment);
            assert_ne!(spare, "edge-3", "{session}: {assignment:?}");
            assert_ne!(spare, leg);
        }
        // Every other edge over the gate too: the held one is kept.
        let mut all_over = over.clone();
        all_over[0].load_permille = 850;
        all_over[2].load_permille = 900;
        let spare_only = AssignmentRequest {
            paths: 0,
            ..ask("all-over", "edge-3")
        };
        let assignment = assign(spare_only, AssignmentPolicy::default(), &all_over).unwrap();
        assert_eq!(assignment.caches[0].node_id, "edge-3");
        // A draining held spare is replaced, and never named.
        let mut draining = others.clone();
        draining[1].draining = true;
        for index in 0..200 {
            let session: &'static str = Box::leak(format!("drain-{index}").into_boxed_str());
            for spare_held in [Some("edge-3"), None] {
                let asked = AssignmentRequest {
                    spare_held,
                    ..ask(session, "edge-3")
                };
                let assignment = assign(asked, AssignmentPolicy::default(), &draining).unwrap();
                assert!(
                    assignment.caches.iter().take(usize::from(assignment.legs) + 1).all(|cache| cache.node_id != "edge-3"),
                    "{session}: {assignment:?}"
                );
            }
        }
    }

    /// A hedge's place asked for as a mover's (`a=`, the lead's decision
    /// after integration 2): its edge keeps it while that edge has taken
    /// less than its share of the movers, and the place goes by the shares
    /// once it has taken that, so that only hedges over their share move.
    #[test]
    fn a_hedge_keeps_its_place_while_its_edge_is_under_its_share() {
        let caches = [
            cache("edge-1", 200, Some(9)),
            cache("edge-3", 200, Some(9)),
            cache("edge-4", 600, Some(9)),
        ];
        let ask = |moves: &'static [(String, u32)], at: &'static str| AssignmentRequest {
            paths: 1,
            redundant: true,
            moving: true,
            recent_moves: moves,
            place_at: Some(at),
            ..request("hedge")
        };
        // Nothing placed yet: every edge is under its share.
        for at in ["edge-1", "edge-3", "edge-4"] {
            let assignment = assign(ask(&[], at), AssignmentPolicy::default(), &caches).unwrap();
            assert_eq!(legs(&assignment), [at]);
        }
        // edge-4 (a fifth of the headroom) has taken 3 of 9: over its share,
        // the place goes to the edge furthest behind.
        let moves: &'static [(String, u32)] = Box::leak(Box::new([
            ("edge-1".to_owned(), 3),
            ("edge-3".to_owned(), 3),
            ("edge-4".to_owned(), 3),
        ]));
        let assignment = assign(ask(moves, "edge-4"), AssignmentPolicy::default(), &caches).unwrap();
        assert_ne!(legs(&assignment), ["edge-4"]);
        let assignment = assign(ask(moves, "edge-1"), AssignmentPolicy::default(), &caches).unwrap();
        assert_eq!(legs(&assignment), ["edge-1"]);
    }

    /// Hosttag review, finding 4: a warm holder on a leg's host is the
    /// spare before a cold edge (one that must take a copy first) on a
    /// third host; among holders a third host still comes first.
    #[test]
    fn the_spare_is_a_warm_holder_before_a_cold_edge_on_a_third_host() {
        let caches = [
            on_host("host-a", cache("holder", 100, Some(9))),
            on_host("host-a", cache("standby", 100, Some(9))),
            on_host("host-b", cache("cold", 100, None)),
        ];
        for index in 0..200 {
            let session = format!("one-path-{index}");
            let ask = AssignmentRequest {
                paths: 1,
                spare: true,
                ..request(&session)
            };
            let assignment = assign(ask, AssignmentPolicy::default(), &caches).unwrap();
            let spare = &assignment.caches[usize::from(assignment.legs)];
            assert_eq!(spare.reason, AssignmentReason::Spare);
            assert!(spare.holds_playlist, "{session}: {assignment:?}");
        }
        // A holder on a third host is still first among holders.
        let mut three = caches.to_vec();
        three.push(on_host("host-c", cache("far-holder", 100, Some(9))));
        three.push(on_host("host-d", cache("farther-holder", 100, Some(9))));
        for index in 0..200 {
            let session = format!("two-path-{index}");
            let ask = AssignmentRequest {
                paths: 2,
                spare: true,
                ..request(&session)
            };
            let assignment = assign(ask, AssignmentPolicy::default(), &three).unwrap();
            let leg_hosts = legs(&assignment)
                .iter()
                .map(|leg| host_of(&three, leg))
                .collect::<Vec<_>>();
            let spare = &assignment.caches[usize::from(assignment.legs)];
            assert!(spare.holds_playlist, "{session}: {assignment:?}");
            assert!(
                !leg_hosts.contains(&host_of(&three, &spare.node_id)),
                "{session}: {assignment:?}"
            );
        }
    }

    /// Hosttag review, finding 5: a replacement leg (and the spare) avoid
    /// the host of the edge the listener left where another is eligible: a
    /// stopped host's other edge still looks fresh. With none eligible the
    /// choice is as before, and it is not counted as a second leg on the
    /// host of a leg.
    #[test]
    fn a_replacement_leg_avoids_the_host_of_the_edge_it_left() {
        let caches = [
            on_host("host-a", cache("a2", 100, Some(9))),
            on_host("host-b", cache("b1", 100, Some(9))),
            on_host("host-c", cache("c1", 100, Some(9))),
        ];
        let held = ["host-b".to_owned()];
        let left = ["host-a".to_owned()];
        let free = [caches[0].clone(), caches[2].clone()];
        for index in 0..200 {
            let session = format!("replace-{index}");
            let ask = AssignmentRequest {
                paths: 1,
                redundant: true,
                moving: true,
                held_hosts: &held,
                left_hosts: &left,
                ..request(&session)
            };
            let assignment = assign(ask, AssignmentPolicy::default(), &free).unwrap();
            assert_eq!(legs(&assignment), ["c1"], "{session}: {assignment:?}");
            assert!(!assignment.second_leg_same_host);
        }
        // c1 over the gate: a2, the dead edge's sibling, as before.
        let mut full = free.clone();
        full[1].load_permille = 900;
        let ask = AssignmentRequest {
            paths: 1,
            redundant: true,
            moving: true,
            held_hosts: &held,
            left_hosts: &left,
            ..request("replace-full")
        };
        let assignment = assign(ask, AssignmentPolicy::default(), &full).unwrap();
        assert_eq!(legs(&assignment), ["a2"]);
        assert!(!assignment.second_leg_same_host);
    }

    /// B 3.6: a region with one serving edge gives one leg and no spare.
    #[test]
    fn one_serving_edge_gives_one_path_and_no_spare() {
        let caches = [
            cache("edge-1", 100, Some(9)),
            CacheCandidate {
                draining: true,
                ..cache("edge-2", 100, Some(9))
            },
        ];
        let assignment = assign(
            AssignmentRequest {
                paths: 2,
                spare: true,
                ..request("listener")
            },
            AssignmentPolicy::default(),
            &caches,
        )
        .unwrap();
        assert_eq!(assignment.legs, 1);
        assert!(!assignment.spare);
        assert_eq!(assignment.caches.len(), 1);
        assert_eq!(first(&assignment), ("edge-1", AssignmentReason::HoldsPlaylist));
        // A playlist over the two-path bit rate gets one leg and its spare.
        let caches = [cache("edge-1", 100, Some(9)), cache("edge-2", 100, Some(9))];
        let video = AssignmentRequest {
            paths: 2,
            spare: true,
            playlist_bps: Some(DEFAULT_TWO_PATH_MAX_BPS + 1),
            ..request("viewer")
        };
        let assignment = assign(video, AssignmentPolicy::default(), &caches).unwrap();
        assert_eq!(assignment.legs, 1);
        assert!(assignment.spare);
        assert_eq!(assignment.caches[1].reason, AssignmentReason::Spare);
        // An LL-HLS reader (the defaults) is answered as before.
        let assignment = assign(request("reader"), AssignmentPolicy::default(), &caches).unwrap();
        assert_eq!((assignment.legs, assignment.spare), (1, false));
        assert!(assignment.caches[1..]
            .iter()
            .all(|cache| cache.reason == AssignmentReason::Alternate));
    }

    /// B Y2 (the lead's decision): an answer that grants fewer legs than
    /// asked names why: no other edge serves, the bit rate, or the gate.
    #[test]
    fn a_short_grant_names_its_reason() {
        let two = |session| AssignmentRequest {
            paths: 2,
            spare: true,
            ..request(session)
        };
        let policy = AssignmentPolicy::default();
        let alone = [
            cache("edge-1", 100, Some(9)),
            CacheCandidate {
                draining: true,
                ..cache("edge-2", 100, Some(9))
            },
        ];
        let short = |request, caches: &[CacheCandidate]| {
            let assignment = assign(request, policy, caches).unwrap();
            (assignment.legs, assignment.short)
        };
        assert_eq!(short(two("a"), &alone), (1, Some(ShortGrant::OneEdge)));
        let video = AssignmentRequest {
            playlist_bps: Some(DEFAULT_TWO_PATH_MAX_BPS + 1),
            ..two("b")
        };
        let pair = [cache("edge-1", 100, Some(9)), cache("edge-2", 100, Some(9))];
        assert_eq!(short(video, &pair), (1, Some(ShortGrant::Bitrate)));
        let full = [cache("edge-1", 100, Some(9)), cache("edge-2", 850, Some(9))];
        assert_eq!(short(two("c"), &full), (1, Some(ShortGrant::Gate)));
        // Two legs granted, or one asked: no reason.
        assert_eq!(short(two("d"), &pair), (2, None));
        assert_eq!(short(request("e"), &pair), (1, None));

        // A listener that holds a leg (the held edge is never a candidate)
        // and asks for one more: none granted, and why (review of B Y2,
        // finding 1). The other edge drains: no other edge serves.
        let held = |session| AssignmentRequest {
            redundant: true,
            ..request(session)
        };
        let draining = [CacheCandidate {
            draining: true,
            ..cache("edge-2", 100, Some(9))
        }];
        assert_eq!(
            assign(held("f"), policy, &draining),
            Err(AssignmentError::NoLegGranted(ShortGrant::OneEdge))
        );
        // The other edge serves at full load: it fails the gate.
        let at_full = [cache("edge-2", FULL_LOAD_PERMILLE, Some(9))];
        assert_eq!(
            assign(held("g"), policy, &at_full),
            Err(AssignmentError::NoLegGranted(ShortGrant::Gate))
        );
        // Nothing serves a listener that holds nothing: no reason, the
        // caller spills it or sends it to the failover region.
        assert_eq!(
            assign(two("h"), policy, &draining),
            Err(AssignmentError::NoCacheAvailable)
        );

        // A spill names its reason as `assign` does (finding 8).
        let spill = |request, caches: &[CacheCandidate]| {
            assign_udp_in_region(request, policy, caches).unwrap().short
        };
        assert_eq!(spill(two("i"), &pair), Some(ShortGrant::Gate));
        assert_eq!(spill(two("j"), &alone), Some(ShortGrant::OneEdge));
        let video = AssignmentRequest {
            playlist_bps: Some(DEFAULT_TWO_PATH_MAX_BPS + 1),
            ..two("k")
        };
        assert_eq!(spill(video, &pair), Some(ShortGrant::Bitrate));
        assert_eq!(spill(request("l"), &pair), None);
    }

    /// B 3.6: a rebalanced listener goes only to a receiver that stays
    /// under the rebalance's level with it counted, or nowhere: no other
    /// tier, no cache of the region named.
    #[test]
    fn a_rebalanced_listener_gets_a_receiver_under_the_level_or_none() {
        let caches = [
            cache("edge-2", 290, Some(9)),
            cache("edge-3", 720, Some(9)),
            cache("edge-4", 520, Some(9)),
        ];
        fn rebalanced(session: &str) -> AssignmentRequest<'_> {
            AssignmentRequest {
                moving: true,
                rebalancing: true,
                listener_permille: Some(16.0),
                spare: true,
                ..request(session)
            }
        }
        for index in 0..40 {
            let session = format!("rebalanced-{index}");
            let assignment = assign(rebalanced(&session), AssignmentPolicy::default(), &caches).unwrap();
            assert_eq!(assignment.legs, 1);
            assert_ne!(assignment.caches[0].node_id, "edge-3", "{session}");
            // A listener that holds another leg: the same rule.
            let held = AssignmentRequest {
                redundant: true,
                ..rebalanced(&session)
            };
            let assignment = assign(held, AssignmentPolicy::default(), &caches).unwrap();
            assert_eq!(assignment.legs, 1);
            assert_ne!(assignment.caches[0].node_id, "edge-3", "{session}");
        }
        // No receiver under 725 with the listener: nothing at all, with a
        // held leg or without, and no spare.
        let full = [cache("edge-3", 720, Some(9)), cache("edge-5", 750, Some(9))];
        assert_eq!(
            assign(rebalanced("rebalanced-1"), AssignmentPolicy::default(), &full),
            Err(AssignmentError::NoCacheAvailable)
        );
        assert_eq!(
            assign(
                AssignmentRequest {
                    redundant: true,
                    ..rebalanced("rebalanced-1")
                },
                AssignmentPolicy::default(),
                &full
            ),
            Err(AssignmentError::NoLegGranted(ShortGrant::Gate))
        );
        // A drain's mover is still placed there: over the level, under full.
        let drained = AssignmentRequest {
            rebalancing: false,
            ..rebalanced("mover-1")
        };
        assert_eq!(
            assign(drained, AssignmentPolicy::default(), &full)
                .unwrap()
                .legs,
            1
        );
        // A request for a spare alone is answered as any (review of B Z3).
        let spare_only = AssignmentRequest {
            paths: 0,
            redundant: true,
            ..rebalanced("rebalanced-1")
        };
        let assignment = assign(spare_only, AssignmentPolicy::default(), &full).unwrap();
        assert_eq!((assignment.legs, assignment.spare), (0, true));
    }

    /// Review of B Z3: a cache the reader uses or was given lately keeps it
    /// only where the rule of its place would take it now. In a rebalance,
    /// a sticky over the receiver level is no leg; a redundant leg passes
    /// the gate, sticky or not; the leg a reader depends on stays while its
    /// cache is healthy, as before.
    #[test]
    fn a_sticky_cache_is_kept_only_where_its_place_would_take_it() {
        let caches = [cache("edge-2", 720, Some(9)), cache("edge-3", 300, Some(9))];
        let remembered = vec!["edge-2".to_owned()];
        let rebalanced = AssignmentRequest {
            moving: true,
            rebalancing: true,
            listener_permille: Some(16.0),
            ..request("give-up")
        };
        // 720 + 16 is over the receiver level, 725.
        for request in [
            AssignmentRequest {
                current: Some("edge-2"),
                ..rebalanced
            },
            AssignmentRequest {
                remembered: &remembered,
                ..rebalanced
            },
        ] {
            let assignment = assign(request, AssignmentPolicy::default(), &caches).unwrap();
            assert_eq!(first(&assignment), ("edge-3", AssignmentReason::HoldsPlaylist));
            // With no receiver at all, no leg rather than the sticky.
            let assignment = assign(request, AssignmentPolicy::default(), &caches[..1]);
            assert_eq!(assignment, Err(AssignmentError::NoCacheAvailable));
        }
        // A redundant leg: the remembered cache under the gate is kept, the
        // one over it is not.
        let held = AssignmentRequest {
            redundant: true,
            listener_permille: Some(16.0),
            remembered: &remembered,
            ..request("held")
        };
        let assignment = assign(held, AssignmentPolicy::default(), &caches).unwrap();
        assert_eq!(first(&assignment), ("edge-2", AssignmentReason::Sticky));
        let over = [cache("edge-2", 790, Some(9)), cache("edge-3", 300, Some(9))];
        let assignment = assign(held, AssignmentPolicy::default(), &over).unwrap();
        assert_eq!(first(&assignment), ("edge-3", AssignmentReason::SecondPath));
        // Two remembered legs are the two legs of a repeat.
        let both = vec!["edge-3".to_owned(), "edge-2".to_owned()];
        let repeat = AssignmentRequest {
            paths: 2,
            remembered: &both,
            ..request("repeat")
        };
        let assignment = assign(repeat, AssignmentPolicy::default(), &caches).unwrap();
        assert_eq!(assignment.legs, 2);
        assert_eq!(
            [&assignment.caches[0], &assignment.caches[1]]
                .map(|leg| (leg.node_id.as_str(), leg.reason)),
            [
                ("edge-3", AssignmentReason::Sticky),
                ("edge-2", AssignmentReason::Sticky)
            ]
        );
    }

    /// B 3.6: a mover that asks for two legs gets two edges, and the legs
    /// of many movers follow the survivors' headroom as single movers do.
    #[test]
    fn two_replacements_are_apportioned_to_two_edges() {
        let caches = [
            cache("edge-2", 300, Some(9)),
            cache("edge-3", 300, Some(9)),
            cache("edge-4", 500, Some(9)),
            cache("edge-5", 600, None),
        ];
        let mut placed: Vec<(String, u32)> = Vec::new();
        for index in 0..20 {
            let session = format!("mover-{index}");
            let request = AssignmentRequest {
                moving: true,
                paths: 2,
                recent_moves: &placed,
                ..request(&session)
            };
            let assignment = assign(request, AssignmentPolicy::default(), &caches).unwrap();
            let names = legs(&assignment)
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            assert_eq!(names.len(), 2, "{assignment:?}");
            assert_ne!(names[0], names[1]);
            for name in names {
                match placed.iter_mut().find(|(node_id, _)| *node_id == name) {
                    Some(entry) => entry.1 += 1,
                    None => placed.push((name, 1)),
                }
            }
        }
        // Headroom to 800: 500 / 500 / 300 / 200 of 1,500 and 40 legs.
        let placed = placed.into_iter().collect::<HashMap<String, u32>>();
        for (name, due) in [
            ("edge-2", 13.33),
            ("edge-3", 13.33),
            ("edge-4", 8.0),
            ("edge-5", 5.33),
        ] {
            let got = f64::from(placed.get(name).copied().unwrap_or(0));
            assert!((got - due).abs() <= 1.5, "{name}: {placed:?}");
        }
    }

    #[test]
    fn the_fill_level_spreads_an_amount_evenly_over_the_lowest() {
        assert!((fill_level(&[394.0, 553.0, 747.0], 658.0) - 784.0).abs() < 1e-9);
        assert!((fill_level(&[300.0, 700.0], 100.0) - 400.0).abs() < 1e-9);
        let rooms = headroom_to_level(
            &[394.0, 553.0, 747.0],
            47.0,
            Level {
                at: 800,
                listener_permille: Some(14.0),
                movers_scale: 1.0,
            },
        );
        assert_eq!(rooms, vec![406.0, 247.0, 53.0]);
        // Two entry points, each with 30 of 60 movers at 15: the level is
        // where 60 fit over 724 and 524 (1,074), not 30 (849).
        let rooms = headroom_to_level(
            &[724.0, 524.0],
            30.0,
            Level {
                at: 800,
                listener_permille: Some(15.0),
                movers_scale: 2.0,
            },
        );
        assert_eq!(rooms, vec![350.0, 550.0]);
        // Nothing under the level: the headroom to full load.
        let rooms = headroom_to_level(
            &[900.0, 820.0],
            1.0,
            Level {
                at: 800,
                listener_permille: None,
                movers_scale: 1.0,
            },
        );
        assert_eq!(rooms, vec![100.0, 180.0]);
    }

    /// The readers an entry point sent to a cache since its advertisement
    /// count in its headroom for new readers (wave 9 C4).
    #[test]
    fn pending_readers_count_in_the_headroom_of_new_readers() {
        let mut caches = vec![cache("edge-1", 300, None), cache("edge-2", 300, None)];
        caches[0].pending_permille = 400;
        let mut first = HashMap::<String, usize>::new();
        for index in 0..2_000 {
            let session = format!("new-{index}");
            let assignment =
                assign_udp_in_region(request(&session), AssignmentPolicy::default(), &caches)
                    .unwrap();
            *first
                .entry(assignment.caches[0].node_id.clone())
                .or_default() += 1;
        }
        // Headroom 300 against 700: about 30 %.
        let share = first["edge-1"] as f64 / 2_000.0;
        assert!(share > 0.25 && share < 0.35, "{first:?}");
    }

    #[test]
    fn a_udp_client_spills_within_its_region_when_the_room_estimates_ran_out() {
        // Every edge's free capacity is used up by readers sent to it since
        // it last advertised; none is overloaded or full by its own count.
        let caches = (1..=4)
            .map(|index| CacheCandidate {
                free_bps: Some(0),
                session_charge_bps: Some(2_000_000),
                ..cache(&format!("edge-{index}"), 300, Some(900))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            assign(request("load-7"), AssignmentPolicy::default(), &caches),
            Err(AssignmentError::NoCacheAvailable)
        );
        let mut first = HashMap::<String, usize>::new();
        for index in 0..200 {
            let session = format!("load-{index}");
            let assignment =
                assign_udp_in_region(request(&session), AssignmentPolicy::default(), &caches)
                    .unwrap();
            assert_eq!(assignment.caches[0].reason, AssignmentReason::Spill);
            assert_eq!(assignment.caches.len(), 3, "the first and two alternates");
            *first
                .entry(assignment.caches[0].node_id.clone())
                .or_default() += 1;
        }
        // Spread over the region by the session, as before.
        assert_eq!(first.len(), 4, "{first:?}");
        assert!(first.values().all(|count| *count > 25), "{first:?}");
        // One cache that holds the playlist under the spread target (a slot
        // that has just warmed) takes its share by headroom, not the crowd.
        let mut joined = caches.clone();
        for cache in joined.iter_mut() {
            cache.load_permille = 770;
            cache.head_part = Some(900);
        }
        joined[3].load_permille = 0;
        joined[3].head_part = None;
        let mut first = HashMap::<String, usize>::new();
        for index in 0..400 {
            let session = format!("load-{index}");
            let assignment =
                assign_udp_in_region(request(&session), AssignmentPolicy::default(), &joined)
                    .unwrap();
            *first
                .entry(assignment.caches[0].node_id.clone())
                .or_default() += 1;
        }
        let share = first["edge-4"] as f64 / 400.0;
        // Weights 1000 against 3 x 230: about 59%.
        assert!(share > 0.45 && share < 0.75, "{first:?}");
        // A cache that says it is overloaded, full, draining or stale is
        // still left out: with none left, no assignment (the failover region).
        let mut sick = caches.clone();
        sick[0].overloaded = true;
        sick[1].load_permille = FULL_LOAD_PERMILLE;
        sick[2].draining = true;
        sick[3].fresh = false;
        assert_eq!(
            assign_udp_in_region(request("load-1"), AssignmentPolicy::default(), &sick),
            Err(AssignmentError::NoCacheAvailable)
        );
        sick[3].fresh = true;
        let assignment =
            assign_udp_in_region(request("load-1"), AssignmentPolicy::default(), &sick).unwrap();
        assert_eq!(assignment.caches[0].node_id, "edge-4");
        // A cache with room is assigned as before, its reason unchanged.
        let mut roomy = caches.clone();
        roomy[2].free_bps = Some(50_000_000);
        let assignment = assign(request("load-1"), AssignmentPolicy::default(), &roomy).unwrap();
        assert_eq!(assignment.caches[0].node_id, "edge-3");
        assert_eq!(assignment.caches[0].reason, AssignmentReason::HoldsPlaylist);
    }

    #[test]
    fn the_first_alternate_of_every_reader_is_the_standby_edge() {
        // No edge holds the playlist yet. The edges rank for its standby
        // copy in the order edge-3, edge-1, edge-4, edge-2.
        let ranks = [
            ("edge-1", 30),
            ("edge-2", 10),
            ("edge-3", 40),
            ("edge-4", 20),
        ];
        let caches = ranks
            .iter()
            .map(|(name, rank)| CacheCandidate {
                failover_rank: Some(*rank),
                ..cache(name, 100, None)
            })
            .collect::<Vec<_>>();
        let mut first = HashMap::<String, usize>::new();
        for index in 0..400 {
            let session = format!("session-{index}");
            let assignment =
                assign(request(&session), AssignmentPolicy::default(), &caches).unwrap();
            let names = assignment
                .caches
                .iter()
                .map(|cache| cache.node_id.as_str())
                .collect::<Vec<_>>();
            *first.entry(names[0].to_owned()).or_default() += 1;
            // The alternates are the other edges in standby order.
            let expected = ["edge-3", "edge-1", "edge-4", "edge-2"]
                .into_iter()
                .filter(|name| *name != names[0])
                .take(2)
                .collect::<Vec<_>>();
            assert_eq!(&names[1..], expected.as_slice(), "{session}");
        }
        // The first cache of a reader is still spread by its session.
        assert_eq!(first.len(), 4);
        assert!(first.values().all(|count| *count > 60), "{first:?}");
        // An edge that holds the playlist comes before the standby order.
        let mut held = caches.clone();
        held[1].head_part = Some(900);
        let assignment = assign(request("session-7"), AssignmentPolicy::default(), &held).unwrap();
        assert_eq!(assignment.caches[0].node_id, "edge-2");
        assert_eq!(assignment.caches[1].node_id, "edge-3");
    }

    #[test]
    fn a_cache_is_judged_by_what_a_session_costs_that_cache() {
        // An Opus playlist is 96 kbit/s. Its readers ask for 200 parts a
        // second, which costs a small cache 2.2 Mbit/s of its capacity.
        let mut caches = vec![
            cache("edge-a", 900, Some(10)),
            cache("edge-b", 100, Some(10)),
        ];
        caches[0].free_bps = Some(1_000_000);
        caches[0].session_charge_bps = Some(2_244_444);
        caches[1].free_bps = Some(80_000_000);
        caches[1].session_charge_bps = Some(2_244_444);
        let assignment = assign(
            AssignmentRequest {
                playlist_bps: Some(96_000),
                ..request("reader")
            },
            AssignmentPolicy::default(),
            &caches,
        )
        .unwrap();
        // edge-a has room for the bits and not for the requests.
        assert_eq!(assignment.caches[0].node_id, "edge-b");
        assert!(assignment
            .caches
            .iter()
            .all(|cache| cache.node_id != "edge-a"));
    }

    fn request(session: &str) -> AssignmentRequest<'_> {
        AssignmentRequest {
            region: "london",
            session: Some(session),
            pin: None,
            current: None,
            remembered: &[],
            playlist_bps: None,
            moving: false,
            recent_moves: &[],
            rebalancing: false,
            listener_permille: None,
            entry_points: 0,
            paths: 1,
            spare: false,
            redundant: false,
            held_hosts: &[],
            spare_held: None,
            place_at: None,
            left_hosts: &[],
            heavy: None,
        }
    }

    fn first(assignment: &Assignment) -> (&str, AssignmentReason) {
        (
            assignment.caches[0].node_id.as_str(),
            assignment.caches[0].reason,
        )
    }

    #[test]
    fn reader_goes_to_a_cache_that_holds_the_playlist() {
        let caches = [
            cache("edge-1", 100, Some(500)),
            cache("edge-2", 0, None),
            cache("edge-3", 0, None),
        ];
        for session in ["a", "b", "c", "d", "e"] {
            let assignment =
                assign(request(session), AssignmentPolicy::default(), &caches).unwrap();
            assert_eq!(
                first(&assignment),
                ("edge-1", AssignmentReason::HoldsPlaylist)
            );
            assert_eq!(assignment.caches.len(), 3);
            assert!(assignment.caches[1..]
                .iter()
                .all(|cache| cache.reason == AssignmentReason::Alternate));
        }
    }

    #[test]
    fn a_copy_is_added_when_the_holders_pass_the_spread_target() {
        let caches = [
            cache("edge-1", 650, Some(500)),
            cache("edge-2", 0, None),
            cache("edge-3", 700, None),
        ];
        let assignment = assign(request("a"), AssignmentPolicy::default(), &caches).unwrap();
        assert_eq!(first(&assignment), ("edge-2", AssignmentReason::NewCopy));
    }

    #[test]
    fn readers_spill_by_free_capacity_when_every_cache_is_busy() {
        let caches = [
            cache("edge-1", 900, Some(500)),
            cache("edge-2", 700, Some(500)),
        ];
        let mut chosen: HashMap<String, usize> = HashMap::new();
        for index in 0..2_000 {
            let session = format!("session-{index}");
            let assignment =
                assign(request(&session), AssignmentPolicy::default(), &caches).unwrap();
            assert_eq!(assignment.caches[0].reason, AssignmentReason::Spill);
            *chosen
                .entry(assignment.caches[0].node_id.clone())
                .or_default() += 1;
        }
        // Free capacity is 100 and 300, so about one reader in four goes to
        // the busier cache.
        let busier = chosen["edge-1"];
        assert!(
            (350..=650).contains(&busier),
            "edge-1 took {busier} of 2000"
        );
    }

    #[test]
    fn sessions_spread_evenly_over_equal_caches_and_keep_their_order() {
        let caches = [
            cache("edge-1", 100, Some(9)),
            cache("edge-2", 100, Some(9)),
            cache("edge-3", 100, Some(9)),
            cache("edge-4", 100, Some(9)),
        ];
        let mut chosen: HashMap<String, usize> = HashMap::new();
        for index in 0..4_000 {
            let session = format!("session-{index}");
            let assignment =
                assign(request(&session), AssignmentPolicy::default(), &caches).unwrap();
            let again = assign(request(&session), AssignmentPolicy::default(), &caches).unwrap();
            assert_eq!(assignment, again);
            *chosen
                .entry(assignment.caches[0].node_id.clone())
                .or_default() += 1;
        }
        for (node_id, readers) in chosen {
            assert!(
                (850..=1_150).contains(&readers),
                "{node_id} took {readers} of 4000"
            );
        }
    }

    #[test]
    fn only_the_readers_of_a_failed_cache_move() {
        let all = [
            cache("edge-1", 100, Some(9)),
            cache("edge-2", 100, Some(9)),
            cache("edge-3", 100, Some(9)),
        ];
        let mut failed = all.clone();
        failed[1].fresh = false;
        let mut moved = 0;
        for index in 0..600 {
            let session = format!("session-{index}");
            let before = assign(request(&session), AssignmentPolicy::default(), &all).unwrap();
            let after = assign(request(&session), AssignmentPolicy::default(), &failed).unwrap();
            assert!(after.caches.iter().all(|cache| cache.node_id != "edge-2"));
            if before.caches[0].node_id == "edge-2" {
                moved += 1;
                // The reader moves to the cache that was its first alternate.
                assert_eq!(after.caches[0].node_id, before.caches[1].node_id);
            } else {
                assert_eq!(after.caches[0].node_id, before.caches[0].node_id);
            }
        }
        assert!((150..=250).contains(&moved), "{moved} of 600 readers moved");
    }

    #[test]
    fn a_reader_keeps_its_cache_while_the_cache_is_healthy() {
        let mut caches = [cache("edge-1", 100, Some(9)), cache("edge-2", 100, Some(9))];
        let sticky = AssignmentRequest {
            current: Some("edge-2"),
            ..request("a")
        };
        // The cache admits no new session, but it keeps this reader.
        caches[1].overloaded = true;
        caches[1].load_permille = 1_100;
        let assignment = assign(sticky, AssignmentPolicy::default(), &caches).unwrap();
        assert_eq!(first(&assignment), ("edge-2", AssignmentReason::Sticky));
        assert_eq!(assignment.caches[1].node_id, "edge-1");
        // A new reader does not get the full cache.
        let assignment = assign(request("b"), AssignmentPolicy::default(), &caches).unwrap();
        assert!(assignment
            .caches
            .iter()
            .all(|cache| cache.node_id != "edge-2"));

        // The cache drains: the reader moves.
        caches[1].draining = true;
        let assignment = assign(sticky, AssignmentPolicy::default(), &caches).unwrap();
        assert_eq!(
            first(&assignment),
            ("edge-1", AssignmentReason::HoldsPlaylist)
        );
        assert_eq!(assignment.caches.len(), 1);
    }

    #[test]
    fn a_pinned_reader_gets_exactly_the_named_cache() {
        let mut caches = [cache("edge-1", 100, Some(9)), cache("edge-2", 1_200, None)];
        caches[1].overloaded = true;
        let pinned = AssignmentRequest {
            pin: Some("edge-2"),
            ..request("a")
        };
        let assignment = assign(pinned, AssignmentPolicy::default(), &caches).unwrap();
        assert!(assignment.pinned);
        assert_eq!(assignment.caches.len(), 1);
        assert_eq!(first(&assignment), ("edge-2", AssignmentReason::Pinned));

        let unknown = AssignmentRequest {
            pin: Some("edge-9"),
            ..request("a")
        };
        assert_eq!(
            assign(unknown, AssignmentPolicy::default(), &caches).unwrap_err(),
            AssignmentError::UnknownPinnedCache
        );
    }

    #[test]
    fn caches_of_another_region_and_non_reader_nodes_are_never_assigned() {
        let mut other_region = cache("ny-edge-1", 0, Some(9));
        other_region.region = "newyork".into();
        let mut distributor = cache("lon-dist-a", 0, Some(9));
        distributor.serves_readers = false;
        let mut no_address = cache("edge-2", 0, Some(9));
        no_address.playback_base_url = None;
        let caches = [
            other_region,
            distributor,
            no_address,
            cache("edge-1", 400, None),
        ];
        let assignment = assign(request("a"), AssignmentPolicy::default(), &caches).unwrap();
        assert_eq!(assignment.caches.len(), 1);
        assert_eq!(first(&assignment), ("edge-1", AssignmentReason::NewCopy));

        let pinned_elsewhere = AssignmentRequest {
            pin: Some("ny-edge-1"),
            ..request("a")
        };
        assert_eq!(
            assign(pinned_elsewhere, AssignmentPolicy::default(), &caches).unwrap_err(),
            AssignmentError::UnknownPinnedCache
        );
    }

    #[test]
    fn a_stalled_or_full_region_is_reported() {
        let policy = AssignmentPolicy {
            max_head_lag_parts: 40,
            ..AssignmentPolicy::default()
        };
        // edge-1 stopped receiving the playlist 100 parts ago.
        let caches = [
            cache("edge-1", 100, Some(400)),
            cache("edge-2", 100, Some(500)),
        ];
        let assignment = assign(request("a"), policy, &caches).unwrap();
        assert_eq!(assignment.caches.len(), 1);
        assert_eq!(assignment.caches[0].node_id, "edge-2");

        let mut full = caches.clone();
        full[1].load_permille = 1_000;
        assert_eq!(
            assign(request("a"), policy, &full).unwrap_err(),
            AssignmentError::NoCacheAvailable
        );
    }

    #[test]
    fn a_new_reader_goes_only_to_a_cache_with_room_for_its_playlist() {
        // Each cache admits 85 Mbit/s. Two have 14 Mbit/s left and one has
        // 24 Mbit/s left. All three still admit sessions.
        let mut caches = [
            cache("edge-1", 835, Some(9)),
            cache("edge-2", 835, Some(9)),
            cache("edge-3", 718, Some(9)),
        ];
        caches[0].free_bps = Some(14_000_000);
        caches[1].free_bps = Some(14_000_000);
        caches[2].free_bps = Some(24_000_000);
        for index in 0..50 {
            let session = format!("viewer-{index}");
            // A 16 Mbit/s 4K reader fits only on the third cache.
            let video = AssignmentRequest {
                playlist_bps: Some(16_000_000),
                ..request(&session)
            };
            let assignment = assign(video, AssignmentPolicy::default(), &caches).unwrap();
            assert_eq!(assignment.caches.len(), 1);
            assert_eq!(assignment.caches[0].node_id, "edge-3");
            // A 2 Mbit/s audio reader fits on all three.
            let audio = AssignmentRequest {
                playlist_bps: Some(2_000_000),
                ..request(&session)
            };
            let assignment = assign(audio, AssignmentPolicy::default(), &caches).unwrap();
            assert_eq!(assignment.caches.len(), 3);
        }
        // A reader that is already on a cache without room keeps it.
        let sticky = AssignmentRequest {
            playlist_bps: Some(16_000_000),
            current: Some("edge-1"),
            ..request("viewer-0")
        };
        let assignment = assign(sticky, AssignmentPolicy::default(), &caches).unwrap();
        assert_eq!(first(&assignment), ("edge-1", AssignmentReason::Sticky));
        // No cache has room: the region is full for this playlist.
        caches[2].free_bps = Some(10_000_000);
        let video = AssignmentRequest {
            playlist_bps: Some(16_000_000),
            ..request("viewer-0")
        };
        assert_eq!(
            assign(video, AssignmentPolicy::default(), &caches).unwrap_err(),
            AssignmentError::NoCacheAvailable
        );
    }

    #[test]
    fn a_reader_without_a_session_goes_to_the_least_loaded_cache() {
        let caches = [
            cache("edge-1", 300, Some(9)),
            cache("edge-2", 200, Some(9)),
            cache("edge-3", 400, Some(9)),
        ];
        let anonymous = AssignmentRequest {
            region: "london",
            ..AssignmentRequest::default()
        };
        let assignment = assign(anonymous, AssignmentPolicy::default(), &caches).unwrap();
        let order = assignment
            .caches
            .iter()
            .map(|cache| cache.node_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(order, ["edge-2", "edge-1", "edge-3"]);
    }

    // Place240: heavy sessions.

    /// A 2-vCPU edge's heavy room as cmpd8's edges were at a step's start:
    /// the stream held (its tee paid), nothing else heavy.
    fn heavy_room() -> HeavyRoom {
        HeavyRoom {
            cpu_budget: 1_500,
            cpu_used: 200 + 650,
            bps_max: 600_000_000,
            bps_used: 0,
            holds: true,
            distress: false,
        }
    }

    /// A UDP viewer of a 240 Mbit/s stream (the defaults of `heavy`).
    const VIEWER_240: HeavySession = HeavySession {
        bps: 240_000_000,
        cpu: 100,
        stream_cpu: 650,
        refuse: true,
    };

    fn heavy_edges(count: usize) -> Vec<CacheCandidate> {
        // cmpd8 b at 00:21:23: every edge at load 23 to 36.
        (0..count)
            .map(|index| CacheCandidate {
                heavy: Some(heavy_room()),
                ..cache(&format!("edge-{}", index + 1), 23 + 13 * index as u32 % 14, Some(100))
            })
            .collect()
    }

    /// The joins of `sessions`, one after another as an entry answers them
    /// in one advertisement interval: each placed session counts at its
    /// edge at once, as the entry's pending charges do (`heavy` off: the
    /// advertisement as it was for every join, as before place240).
    fn join_all(edges: &[CacheCandidate], sessions: &[String], heavy: bool) -> Vec<String> {
        let mut edges = edges.to_vec();
        let mut placed = Vec::new();
        for session in sessions {
            let ask = AssignmentRequest {
                playlist_bps: Some(240_000_000),
                heavy: heavy.then_some(VIEWER_240),
                ..request(session)
            };
            let answer = assign(ask, AssignmentPolicy::default(), &edges).unwrap();
            let node = answer.caches[0].node_id.clone();
            assert_eq!(answer.legs, 1);
            if let Some(room) = edges
                .iter_mut()
                .find(|edge| edge.node_id == node)
                .and_then(|edge| edge.heavy.as_mut())
            {
                room.cpu_used += room.cost(&VIEWER_240);
                room.bps_used += VIEWER_240.bps;
                room.holds = true;
            }
            placed.push(node);
        }
        placed
    }

    fn counts(placed: &[String], edges: usize) -> Vec<usize> {
        (1..=edges)
            .map(|index| {
                placed
                    .iter()
                    .filter(|node| **node == format!("edge-{index}"))
                    .count()
            })
            .collect()
    }

    /// Sessions that the load-weighted hash alone puts on one edge, as it
    /// put cmpd8 b's viewers (so that the test fails without the rule).
    fn sessions_the_hash_puts_together(edges: &[CacheCandidate], wanted: usize) -> Vec<String> {
        let mut by_edge: HashMap<String, Vec<String>> = HashMap::new();
        for index in 0..10_000 {
            let session = format!("viewer-{index}");
            let placed = join_all(edges, std::slice::from_ref(&session), false);
            let group = by_edge.entry(placed[0].clone()).or_default();
            group.push(session);
            if group.len() == wanted {
                return group.clone();
            }
        }
        panic!("no {wanted} sessions share an edge");
    }

    /// Place240 rule 1: two and four viewers of a 240 Mbit/s stream that
    /// join together spread evenly over two and three edges by committed
    /// bytes and cost, where the load-weighted hash put them all on one.
    #[test]
    fn simultaneous_heavy_joins_spread_evenly_by_committed_cost() {
        for (edges, joins, expected) in [
            (2, 2, vec![1, 1]),
            (2, 4, vec![2, 2]),
            (3, 2, vec![0, 1, 1]),
            (3, 4, vec![1, 1, 2]),
        ] {
            let region = heavy_edges(edges);
            let sessions = sessions_the_hash_puts_together(&region, joins);
            let before = counts(&join_all(&region, &sessions, false), edges);
            assert_eq!(before.iter().max(), Some(&joins), "{edges} edges: the hash put {before:?}");
            let mut after = counts(&join_all(&region, &sessions, true), edges);
            after.sort_unstable();
            assert_eq!(after, expected, "{edges} edges, {joins} joins");
        }
    }

    /// The same joins in any order from an entry that has counted them
    /// spread the same way, and a session that asks again while the edges'
    /// room is level gets its edge again (the rendezvous hash among
    /// comparable edges).
    #[test]
    fn a_heavy_session_asking_again_keeps_its_edge_while_the_room_is_level() {
        let region = heavy_edges(3);
        for index in 0..200 {
            let session = format!("again-{index}");
            let first = join_all(&region, std::slice::from_ref(&session), true);
            let again = join_all(&region, std::slice::from_ref(&session), true);
            assert_eq!(first, again);
        }
        // Level within the band: a small difference does not move it.
        let mut nudged = region.clone();
        nudged[0].heavy.as_mut().unwrap().cpu_used += 20;
        let mut kept = 0;
        for index in 0..200 {
            let session = format!("again-{index}");
            kept += usize::from(
                join_all(&region, std::slice::from_ref(&session), true)
                    == join_all(&nudged, std::slice::from_ref(&session), true),
            );
        }
        assert_eq!(kept, 200, "a 20-permille change is within the band");
    }

    /// Place240 rule 1: an edge whose committed cost would pass its budget
    /// with the session takes none while another has room, though it is the
    /// least loaded; one whose egress would pass its limit likewise.
    #[test]
    fn heavy_joins_to_an_edge_at_budget_are_sent_elsewhere() {
        let mut region = heavy_edges(3);
        region[0].load_permille = 0;
        // Two viewers and a reader at 240 already (cmpd8 b's new-edge-1).
        region[0].heavy.as_mut().unwrap().cpu_used = 200 + 650 + 2 * 100 + 240 + 300;
        region[1].heavy.as_mut().unwrap().bps_used = 480_000_000;
        for index in 0..100 {
            let placed = join_all(&region, &[format!("busy-{index}")], true);
            assert_eq!(placed, vec!["edge-3".to_owned()]);
        }
        // An edge that does not hold the stream pays its tee too.
        let mut cold = heavy_edges(2);
        cold[0].heavy = Some(HeavyRoom {
            holds: false,
            cpu_used: 200 + 650,
            ..heavy_room()
        });
        assert!(!cold[0].heavy.unwrap().fits(&VIEWER_240));
        assert_eq!(join_all(&cold, &["cold".to_owned()], true), vec!["edge-2".to_owned()]);
    }

    /// Place240 rule 2: with no edge of the region that fits, a heavy
    /// session of a measured rate is refused; one of an estimated rate is
    /// placed as before; a small session is never refused by it.
    #[test]
    fn a_heavy_session_with_no_room_in_the_region_is_refused() {
        let mut region = heavy_edges(2);
        for edge in &mut region {
            edge.heavy.as_mut().unwrap().cpu_used = 1_450;
        }
        let ask = AssignmentRequest {
            playlist_bps: Some(240_000_000),
            heavy: Some(VIEWER_240),
            ..request("late")
        };
        assert_eq!(
            assign(ask, AssignmentPolicy::default(), &region),
            Err(AssignmentError::HeavyFull)
        );
        let estimated = AssignmentRequest {
            heavy: Some(HeavySession {
                refuse: false,
                ..VIEWER_240
            }),
            ..ask
        };
        assert!(assign(estimated, AssignmentPolicy::default(), &region).is_ok());
        let audio = AssignmentRequest {
            playlist_bps: Some(2_600_000),
            ..request("audio")
        };
        assert!(assign(audio, AssignmentPolicy::default(), &region).is_ok());
    }

    /// Place240 rule 4: an edge in distress takes no heavy session, and a
    /// heavy session that uses it is not kept there; one that uses a
    /// healthy edge keeps it.
    #[test]
    fn an_edge_in_distress_takes_and_keeps_no_heavy_session() {
        let mut region = heavy_edges(2);
        region[0].heavy.as_mut().unwrap().distress = true;
        region[0].load_permille = 0;
        for index in 0..50 {
            let session = format!("d-{index}");
            assert_eq!(join_all(&region, std::slice::from_ref(&session), true), vec!["edge-2".to_owned()]);
            let current = AssignmentRequest {
                current: Some("edge-1"),
                heavy: Some(VIEWER_240),
                ..request(&session)
            };
            let answer = assign(current, AssignmentPolicy::default(), &region).unwrap();
            assert_eq!(answer.caches[0].node_id, "edge-2");
            let kept = AssignmentRequest {
                current: Some("edge-2"),
                heavy: Some(VIEWER_240),
                ..request(&session)
            };
            let answer = assign(kept, AssignmentPolicy::default(), &region).unwrap();
            assert_eq!(first(&answer), ("edge-2", AssignmentReason::Sticky));
        }
    }

    /// Place240: a 1 ms audio crowd's placement does not change when the
    /// edges advertise heavy work (the rule is for heavy sessions alone):
    /// every one of 2,000 listeners gets exactly what the function gave
    /// before, on the same inputs, and the spread is the old spread.
    #[test]
    fn an_audio_crowds_placement_is_what_it_was() {
        let mut old_region = vec![
            cache("edge-1", 120, Some(100)),
            cache("edge-2", 300, Some(100)),
            cache("edge-3", 450, Some(100)),
        ];
        old_region[1].pending_permille = 40;
        let mut new_region = old_region.clone();
        new_region[0].heavy = Some(HeavyRoom {
            cpu_used: 1_400,
            ..heavy_room()
        });
        new_region[1].heavy = Some(HeavyRoom {
            distress: true,
            ..heavy_room()
        });
        new_region[2].heavy = Some(heavy_room());
        let mut old_counts = [0_usize; 3];
        let mut new_counts = [0_usize; 3];
        for index in 0..2_000 {
            let session = format!("pcm-{index}");
            for paths in [1, 2] {
                let ask = AssignmentRequest {
                    playlist_bps: Some(2_600_000),
                    paths,
                    spare: paths == 2,
                    ..request(&session)
                };
                let old = assign(ask, AssignmentPolicy::default(), &old_region);
                let new = assign(ask, AssignmentPolicy::default(), &new_region);
                assert_eq!(old, new, "{session} p={paths}");
                if let (Ok(old), Ok(new), 1) = (old, new, paths) {
                    let at = |answer: &Assignment| {
                        answer.caches[0].node_id[5..].parse::<usize>().unwrap() - 1
                    };
                    old_counts[at(&old)] += 1;
                    new_counts[at(&new)] += 1;
                }
            }
        }
        assert_eq!(old_counts, new_counts);
        assert!(old_counts.iter().all(|count| *count > 0), "{old_counts:?}");
    }
}
