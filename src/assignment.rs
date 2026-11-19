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
}

impl CacheCandidate {
    fn holds_playlist(&self) -> bool {
        self.head_part.is_some()
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
}

impl Default for AssignmentPolicy {
    fn default() -> Self {
        Self {
            spread_permille: 600,
            max_alternates: 2,
            max_head_lag_parts: 0,
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
    let admits = |cache: &CacheCandidate| {
        healthy(cache) && !cache.overloaded && cache.load_permille < FULL_LOAD_PERMILLE
    };

    let session = request.session.unwrap_or("");
    let mut ranked = regional
        .iter()
        .copied()
        .filter(|cache| admits(cache))
        .map(|cache| {
            let tier = if cache.holds_playlist() && cache.load_permille < policy.spread_permille {
                0_u8
            } else if !cache.holds_playlist() && cache.load_permille < policy.spread_permille {
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
    for (tier, _, cache) in ranked {
        if caches.len() > policy.max_alternates {
            break;
        }
        if caches.iter().any(|chosen| chosen.node_id == cache.node_id) {
            continue;
        }
        let reason = if !caches.is_empty() {
            AssignmentReason::Alternate
        } else {
            match tier {
                0 => AssignmentReason::HoldsPlaylist,
                1 => AssignmentReason::NewCopy,
                _ => AssignmentReason::Spill,
            }
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

/// Weighted rendezvous score of a cache for a session. The cache with the
/// highest score wins. The weight is the free capacity of the cache, so a
/// cache with twice the room takes about twice the new sessions.
///
/// A request without a session identifier has no hash to spread it, so the
/// score is the free capacity itself and the least loaded cache wins.
fn rendezvous_score(session: &str, cache: &CacheCandidate) -> f64 {
    let free = f64::from(
        FULL_LOAD_PERMILLE
            .saturating_sub(cache.load_permille)
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
        }
    }

    fn request(session: &str) -> AssignmentRequest<'_> {
        AssignmentRequest {
            region: "london",
            session: Some(session),
            pin: None,
            current: None,
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
