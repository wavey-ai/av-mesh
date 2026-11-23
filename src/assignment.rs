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
}

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
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AssignmentRequest<'a> {
    pub region: &'a str,
    /// Stable identifier of the playback session, for example the CMCD `sid`.
    pub session: Option<&'a str>,
    /// Cache that an operator or a test names for this reader.
    pub pin: Option<&'a str>,
    /// Cache that the reader uses now, for example `_HLS_pathway`.
    pub current: Option<&'a str>,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AssignedCache {
    pub node_id: String,
    pub playback_base_url: String,
    pub reason: AssignmentReason,
    pub load_permille: u32,
    pub holds_playlist: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Assignment {
    /// The assigned cache first, then the failover order.
    pub caches: Vec<AssignedCache>,
    pub pinned: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignmentError {
    /// The pinned cache is not a reader cache of the region.
    UnknownPinnedCache,
    /// No cache of the region can take the reader now.
    NoCacheAvailable,
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

    let session = request.session.unwrap_or("");
    let moving = request.moving && policy.move_headroom;
    let mut ranked = regional
        .iter()
        .copied()
        .filter(|cache| admits(cache))
        .map(|cache| {
            let load = cache.effective_load_permille();
            let tier = if moving {
                // Wave 9 C4: by headroom alone, under the rebalance target
                // first (a mover never goes to an edge that would shed it).
                u8::from(cache.load_permille >= policy.rebalance_target_permille)
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
    ranked.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| right.1.total_cmp(&left.1))
            .then_with(|| left.2.node_id.cmp(&right.2.node_id))
    });

    let mut caches = Vec::new();
    let sticky = request.current.and_then(|current| {
        regional
            .iter()
            .copied()
            .find(|cache| cache.node_id == current && healthy(cache))
    });
    if let Some(cache) = sticky {
        caches.push(assigned(cache, AssignmentReason::Sticky));
    }
    let mut ranked = ranked;
    if moving && caches.is_empty() && !ranked.is_empty() {
        // Wave 9 C4: the movers that this entry point placed lately are
        // apportioned over the first tier by headroom (largest remainder):
        // the one whose share is furthest behind takes this one, so that a
        // drained edge's few dozen movers follow the headroom-weighted
        // shares closely rather than by chance (a weighted rendezvous of
        // 36 movers put 5 where 6.9 were due, 1/1.37 of the share).
        let first_tier = ranked[0].0;
        let pool = ranked
            .iter()
            .enumerate()
            .filter(|(_, (tier, _, _))| *tier == first_tier)
            .map(|(index, (_, _, cache))| (index, *cache))
            .collect::<Vec<_>>();
        if let Some(index) = apportion(&pool, request.recent_moves) {
            let chosen = ranked.remove(index);
            ranked.insert(0, chosen);
        }
    }
    if caches.is_empty() && !ranked.is_empty() {
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
    // The caches after the first are where the reader goes when its cache
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
    for (_tier, _, cache) in ranked {
        if caches.len() > policy.max_alternates {
            break;
        }
        if caches.iter().any(|chosen| chosen.node_id == cache.node_id) {
            continue;
        }
        caches.push(assigned(cache, AssignmentReason::Alternate));
    }
    if caches.is_empty() {
        return Err(AssignmentError::NoCacheAvailable);
    }
    Ok(Assignment {
        caches,
        pinned: false,
    })
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
    let healthy = |cache: &CacheCandidate| {
        cache.region == request.region
            && cache.serves_readers
            && cache.playback_base_url.is_some()
            && cache.fresh
            && !cache.draining
            && !cache.overloaded
            && cache.load_permille < FULL_LOAD_PERMILLE
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
    if caches.is_empty() {
        return Err(AssignmentError::NoCacheAvailable);
    }
    Ok(Assignment {
        caches,
        pinned: false,
    })
}

fn assigned(cache: &CacheCandidate, reason: AssignmentReason) -> AssignedCache {
    AssignedCache {
        node_id: cache.node_id.clone(),
        playback_base_url: cache.playback_base_url.clone().unwrap_or_default(),
        reason,
        load_permille: cache.load_permille,
        holds_playlist: cache.holds_playlist(),
    }
}

/// Wave 9 C4: which of `pool` (index into the ranking, cache; in ranking
/// order) a mover goes to, so that the movers placed lately
/// (`recent_moves`: cache, count) and this one follow each cache's share of
/// the pool's headroom: the cache whose due share less what it took is the
/// largest. Ties go to the earlier in the ranking (the session's
/// rendezvous). `None` for an empty pool.
fn apportion(pool: &[(usize, &CacheCandidate)], recent_moves: &[(String, u32)]) -> Option<usize> {
    let taken = |cache: &CacheCandidate| {
        recent_moves
            .iter()
            .filter(|(node_id, _)| *node_id == cache.node_id)
            .map(|(_, count)| f64::from(*count))
            .sum::<f64>()
    };
    let headroom = |cache: &CacheCandidate| {
        f64::from(
            FULL_LOAD_PERMILLE
                .saturating_sub(cache.load_permille)
                .max(1),
        )
    };
    let whole: f64 = pool.iter().map(|(_, cache)| headroom(cache)).sum();
    let total: f64 = pool.iter().map(|(_, cache)| taken(cache)).sum::<f64>() + 1.0;
    let mut best: Option<(f64, usize)> = None;
    for (index, cache) in pool {
        let deficit = headroom(cache) / whole * total - taken(cache);
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
        }
    }

    /// Wave 9 C4: the movers of a drained edge spread over the survivors
    /// in proportion to their headroom (the spread tiers sent them all to
    /// the survivors under 60 %), apportioned against the movers placed
    /// before them, and a survivor over the rebalance target takes none
    /// while one under it has room.
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
        // The wave 9 local F1: headroom 710 / 280 / 480 of 1,470 and 36
        // movers: 17.4 / 6.9 / 11.8 due; within one of each.
        let moved = place(&caches, AssignmentPolicy::default(), 36);
        for (name, due) in [("edge-2", 17.39), ("edge-3", 6.86), ("edge-4", 11.76)] {
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
        // Over the rebalance target: none while a cache under it has room.
        let mut over = caches.clone();
        over[1].load_permille = 850;
        let moved = place(&over, AssignmentPolicy::default(), 36);
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
            playlist_bps: None,
            moving: false,
            recent_moves: &[],
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
}
