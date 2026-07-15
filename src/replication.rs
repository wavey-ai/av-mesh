use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MeshNode {
    pub node_id: String,
    pub region: String,
    pub continent: String,
    pub latitude: f64,
    pub longitude: f64,
    pub total_storage_bytes: u64,
    pub used_storage_bytes: u64,
    pub egress_capacity_bps: u64,
    pub contributor_streams: u64,
    pub active_streams: u64,
    pub draining: bool,
}

impl MeshNode {
    pub fn available_storage_bytes(&self) -> u64 {
        self.total_storage_bytes
            .saturating_sub(self.used_storage_bytes)
    }

    pub fn storage_utilization(&self) -> f64 {
        if self.total_storage_bytes == 0 {
            return 1.0;
        }
        self.used_storage_bytes as f64 / self.total_storage_bytes as f64
    }

    fn can_store(&self, stream: &StreamInfo) -> bool {
        !self.draining && self.available_storage_bytes() >= stream.bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamInfo {
    pub stream_id: u64,
    pub bytes: u64,
    pub contributor_node_id: Option<String>,
    pub active: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DemandSignal {
    pub stream_id: u64,
    pub requester_node_id: String,
    pub region: String,
    pub continent: String,
    pub active_readers: u64,
    pub reads_per_sec: f64,
    pub observed_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplicationPolicy {
    pub baseline_per_region: usize,
    pub baseline_per_continent: usize,
    pub demand_reads_per_sec: f64,
    pub demand_active_readers: u64,
    pub min_mirror_distance_km: f64,
    pub max_new_replicas_per_plan: usize,
}

impl Default for ReplicationPolicy {
    fn default() -> Self {
        Self {
            baseline_per_region: 0,
            baseline_per_continent: 1,
            demand_reads_per_sec: 10.0,
            demand_active_readers: 50,
            min_mirror_distance_km: 300.0,
            max_new_replicas_per_plan: 32,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplicaPlacement {
    pub stream_id: u64,
    pub target_node_id: String,
    pub reason: ReplicaReason,
    pub score: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaReason {
    BaselineRegion { region: String },
    BaselineContinent { continent: String },
    DemandRegion { region: String },
    DemandContinent { continent: String },
}

#[derive(Debug, Clone, Copy)]
enum Scope<'a> {
    Region(&'a str),
    Continent(&'a str),
}

impl ReplicationPolicy {
    pub fn plan_replicas(
        &self,
        stream: &StreamInfo,
        nodes: &[MeshNode],
        existing_replicas: &HashSet<String>,
        demand: &[DemandSignal],
    ) -> Vec<ReplicaPlacement> {
        let existing = existing_replicas
            .iter()
            .filter_map(|node_id| nodes.iter().find(|node| node.node_id == *node_id))
            .collect::<Vec<_>>();
        let mut selected = Vec::new();
        let mut selected_ids = HashSet::new();

        if self.baseline_per_continent > 0 {
            let continents = ordered_counts(nodes.iter().map(|node| node.continent.as_str()));
            for continent in continents.keys() {
                self.fill_scope(
                    stream,
                    nodes,
                    existing_replicas,
                    &mut selected_ids,
                    &mut selected,
                    Scope::Continent(continent),
                    self.baseline_per_continent,
                    ReplicaReason::BaselineContinent {
                        continent: continent.to_string(),
                    },
                    &existing,
                );
            }
        }

        if self.baseline_per_region > 0 {
            let regions = ordered_counts(nodes.iter().map(|node| node.region.as_str()));
            for region in regions.keys() {
                self.fill_scope(
                    stream,
                    nodes,
                    existing_replicas,
                    &mut selected_ids,
                    &mut selected,
                    Scope::Region(region),
                    self.baseline_per_region,
                    ReplicaReason::BaselineRegion {
                        region: region.to_string(),
                    },
                    &existing,
                );
            }
        }

        let mut demand_by_region: HashMap<(&str, &str), DemandTotals> = HashMap::new();
        for signal in demand
            .iter()
            .filter(|signal| signal.stream_id == stream.stream_id)
        {
            let totals = demand_by_region
                .entry((signal.region.as_str(), signal.continent.as_str()))
                .or_default();
            totals.reads_per_sec += signal.reads_per_sec;
            totals.active_readers = totals.active_readers.saturating_add(signal.active_readers);
        }

        for ((region, continent), totals) in demand_by_region {
            if !self.demand_is_hot(totals) {
                continue;
            }

            if !has_replica_in_scope(
                nodes,
                existing_replicas,
                &selected_ids,
                Scope::Region(region),
            ) && self.fill_scope(
                stream,
                nodes,
                existing_replicas,
                &mut selected_ids,
                &mut selected,
                Scope::Region(region),
                1,
                ReplicaReason::DemandRegion {
                    region: region.to_string(),
                },
                &existing,
            ) {
                continue;
            }

            if !has_replica_in_scope(
                nodes,
                existing_replicas,
                &selected_ids,
                Scope::Continent(continent),
            ) {
                self.fill_scope(
                    stream,
                    nodes,
                    existing_replicas,
                    &mut selected_ids,
                    &mut selected,
                    Scope::Continent(continent),
                    1,
                    ReplicaReason::DemandContinent {
                        continent: continent.to_string(),
                    },
                    &existing,
                );
            }
        }

        selected
    }

    #[allow(clippy::too_many_arguments)]
    fn fill_scope(
        &self,
        stream: &StreamInfo,
        nodes: &[MeshNode],
        existing_replicas: &HashSet<String>,
        selected_ids: &mut HashSet<String>,
        selected: &mut Vec<ReplicaPlacement>,
        scope: Scope<'_>,
        target_count: usize,
        reason: ReplicaReason,
        existing: &[&MeshNode],
    ) -> bool {
        let mut added = false;
        while count_replicas_in_scope(nodes, existing_replicas, selected_ids, scope) < target_count
            && selected.len() < self.max_new_replicas_per_plan
        {
            let Some(candidate) = self.choose_candidate(
                stream,
                nodes,
                existing_replicas,
                selected_ids,
                scope,
                existing,
            ) else {
                break;
            };
            selected_ids.insert(candidate.node_id.clone());
            selected.push(ReplicaPlacement {
                stream_id: stream.stream_id,
                target_node_id: candidate.node_id.clone(),
                reason: reason.clone(),
                score: score_candidate(candidate, existing, selected, nodes),
            });
            added = true;
        }
        added
    }

    fn choose_candidate<'a>(
        &self,
        stream: &StreamInfo,
        nodes: &'a [MeshNode],
        existing_replicas: &HashSet<String>,
        selected_ids: &HashSet<String>,
        scope: Scope<'_>,
        existing: &[&MeshNode],
    ) -> Option<&'a MeshNode> {
        let candidates = nodes
            .iter()
            .filter(|node| scope_matches(node, scope))
            .filter(|node| !existing_replicas.contains(&node.node_id))
            .filter(|node| !selected_ids.contains(&node.node_id))
            .filter(|node| node.can_store(stream))
            .collect::<Vec<_>>();

        let selected = selected_ids
            .iter()
            .filter_map(|node_id| nodes.iter().find(|node| node.node_id == *node_id))
            .collect::<Vec<_>>();
        let replicas = existing
            .iter()
            .copied()
            .chain(selected.iter().copied())
            .collect::<Vec<_>>();
        let far_enough = candidates
            .iter()
            .copied()
            .filter(|node| {
                min_distance_to_node_refs(node, &replicas) >= self.min_mirror_distance_km
            })
            .collect::<Vec<_>>();
        let pool = if far_enough.is_empty() {
            candidates
        } else {
            far_enough
        };

        pool.into_iter().max_by(|left, right| {
            score_candidate_against_refs(left, &replicas)
                .partial_cmp(&score_candidate_against_refs(right, &replicas))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    }

    fn demand_is_hot(&self, totals: DemandTotals) -> bool {
        totals.reads_per_sec >= self.demand_reads_per_sec
            || totals.active_readers >= self.demand_active_readers
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct DemandTotals {
    active_readers: u64,
    reads_per_sec: f64,
}

fn ordered_counts<'a>(values: impl Iterator<Item = &'a str>) -> BTreeMap<&'a str, usize> {
    let mut counts = BTreeMap::new();
    for value in values {
        *counts.entry(value).or_insert(0) += 1;
    }
    counts
}

fn count_replicas_in_scope(
    nodes: &[MeshNode],
    existing_replicas: &HashSet<String>,
    selected_ids: &HashSet<String>,
    scope: Scope<'_>,
) -> usize {
    nodes
        .iter()
        .filter(|node| scope_matches(node, scope))
        .filter(|node| {
            existing_replicas.contains(&node.node_id) || selected_ids.contains(&node.node_id)
        })
        .count()
}

fn has_replica_in_scope(
    nodes: &[MeshNode],
    existing_replicas: &HashSet<String>,
    selected_ids: &HashSet<String>,
    scope: Scope<'_>,
) -> bool {
    count_replicas_in_scope(nodes, existing_replicas, selected_ids, scope) > 0
}

fn scope_matches(node: &MeshNode, scope: Scope<'_>) -> bool {
    match scope {
        Scope::Region(region) => node.region == region,
        Scope::Continent(continent) => node.continent == continent,
    }
}

fn score_candidate(
    node: &MeshNode,
    existing: &[&MeshNode],
    selected: &[ReplicaPlacement],
    nodes: &[MeshNode],
) -> f64 {
    let selected = selected
        .iter()
        .filter_map(|placement| {
            nodes
                .iter()
                .find(|candidate| candidate.node_id == placement.target_node_id)
        })
        .collect::<Vec<_>>();
    let replicas = existing
        .iter()
        .copied()
        .chain(selected.iter().copied())
        .collect::<Vec<_>>();

    score_candidate_against_refs(node, &replicas)
}

fn score_candidate_against_refs(node: &MeshNode, replicas: &[&MeshNode]) -> f64 {
    let free_ratio = if node.total_storage_bytes == 0 {
        0.0
    } else {
        node.available_storage_bytes() as f64 / node.total_storage_bytes as f64
    };
    let throughput_score = (node.egress_capacity_bps as f64 / 1_000_000_000.0).min(10.0) / 10.0;
    let load_penalty = (node.active_streams + node.contributor_streams) as f64 * 0.001;
    let distance_score = (min_distance_to_node_refs(node, replicas) / 10_000.0).min(1.0);

    free_ratio * 0.50 + throughput_score * 0.20 + distance_score * 0.30 - load_penalty
}

fn min_distance_to_node_refs(node: &MeshNode, replicas: &[&MeshNode]) -> f64 {
    replicas
        .iter()
        .copied()
        .map(|replica| distance_km(node, replica))
        .fold(f64::INFINITY, f64::min)
}

pub fn distance_km(left: &MeshNode, right: &MeshNode) -> f64 {
    let earth_radius_km = 6_371.0;
    let d_lat = (right.latitude - left.latitude).to_radians();
    let d_lon = (right.longitude - left.longitude).to_radians();
    let lat1 = left.latitude.to_radians();
    let lat2 = right.latitude.to_radians();
    let a = (d_lat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (d_lon / 2.0).sin().powi(2);
    2.0 * earth_radius_km * a.sqrt().asin()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TB: u64 = 1_000_000_000_000;

    #[test]
    fn baseline_replication_stages_one_copy_per_continent() {
        let nodes = global_nodes();
        let stream = stream(1, 50 * 1_000_000_000);
        let policy = ReplicationPolicy {
            baseline_per_continent: 1,
            baseline_per_region: 0,
            ..ReplicationPolicy::default()
        };

        let plan = policy.plan_replicas(&stream, &nodes, &HashSet::new(), &[]);

        assert_eq!(plan.len(), 4);
        assert!(plan.iter().any(|p| p.target_node_id.starts_with("eu-")));
        assert!(plan.iter().any(|p| p.target_node_id.starts_with("na-")));
        assert!(plan.iter().any(|p| p.target_node_id.starts_with("sa-")));
        assert!(plan.iter().any(|p| p.target_node_id.starts_with("apac-")));
    }

    #[test]
    fn demand_signal_places_local_replica_when_region_has_no_copy() {
        let nodes = global_nodes();
        let stream = stream(42, 10 * 1_000_000_000);
        let existing = HashSet::from(["eu-london-1".to_string()]);
        let policy = ReplicationPolicy {
            baseline_per_continent: 0,
            demand_reads_per_sec: 5.0,
            demand_active_readers: 10,
            ..ReplicationPolicy::default()
        };
        let demand = [DemandSignal {
            stream_id: 42,
            requester_node_id: "apac-tokyo-1".into(),
            region: "jp-east".into(),
            continent: "apac".into(),
            active_readers: 200,
            reads_per_sec: 25.0,
            observed_unix_ms: 1,
        }];

        let plan = policy.plan_replicas(&stream, &nodes, &existing, &demand);

        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].target_node_id, "apac-tokyo-1");
        assert_eq!(
            plan[0].reason,
            ReplicaReason::DemandRegion {
                region: "jp-east".into()
            }
        );
    }

    #[test]
    fn storage_capacity_excludes_full_nodes() {
        let nodes = vec![
            node("eu-full", "uk-south", "eu", 51.5, -0.1, TB, TB - 10),
            node("eu-roomy", "uk-south", "eu", 55.9, -3.2, TB, 10),
        ];
        let stream = stream(7, 100);
        let policy = ReplicationPolicy {
            baseline_per_region: 1,
            baseline_per_continent: 0,
            ..ReplicationPolicy::default()
        };

        let plan = policy.plan_replicas(&stream, &nodes, &HashSet::new(), &[]);

        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].target_node_id, "eu-roomy");
    }

    #[test]
    fn close_nodes_are_not_preferred_as_mirrors_when_farther_nodes_exist() {
        let nodes = vec![
            node("uk-london-1", "uk-south", "eu", 51.5074, -0.1278, TB, 100),
            node("uk-london-2", "uk-south", "eu", 51.51, -0.12, TB, 100),
            node(
                "uk-edinburgh-1",
                "uk-south",
                "eu",
                55.9533,
                -3.1883,
                TB,
                100,
            ),
        ];
        let stream = stream(9, 100);
        let existing = HashSet::from(["uk-london-1".to_string()]);
        let policy = ReplicationPolicy {
            baseline_per_region: 2,
            baseline_per_continent: 0,
            min_mirror_distance_km: 300.0,
            ..ReplicationPolicy::default()
        };

        let plan = policy.plan_replicas(&stream, &nodes, &existing, &[]);

        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].target_node_id, "uk-edinburgh-1");
    }

    #[test]
    fn capped_global_region_staging_is_deterministic() {
        let nodes = global_nodes();
        let stream = stream(77, 100);
        let policy = ReplicationPolicy {
            baseline_per_region: 1,
            baseline_per_continent: 0,
            max_new_replicas_per_plan: 3,
            ..ReplicationPolicy::default()
        };

        let first_plan = policy.plan_replicas(&stream, &nodes, &HashSet::new(), &[]);
        let second_plan = policy.plan_replicas(&stream, &nodes, &HashSet::new(), &[]);

        assert_eq!(first_plan, second_plan);
        assert_eq!(
            first_plan
                .iter()
                .map(|placement| placement.target_node_id.as_str())
                .collect::<Vec<_>>(),
            vec!["apac-sydney-1", "sa-sao-paulo-1", "eu-frankfurt-1"]
        );
    }

    #[test]
    fn planner_scales_without_fixed_node_limits() {
        let mut nodes = Vec::with_capacity(10_000);
        for i in 0..10_000 {
            nodes.push(node(
                &format!("node-{i}"),
                &format!("region-{}", i % 128),
                if i % 2 == 0 { "eu" } else { "na" },
                40.0 + (i % 90) as f64 * 0.1,
                -120.0 + (i % 180) as f64 * 0.1,
                TB,
                (i % 100) as u64,
            ));
        }
        let stream = stream(88, 1_000_000);
        let policy = ReplicationPolicy {
            baseline_per_continent: 1,
            baseline_per_region: 1,
            max_new_replicas_per_plan: 512,
            ..ReplicationPolicy::default()
        };

        let plan = policy.plan_replicas(&stream, &nodes, &HashSet::new(), &[]);

        assert!(plan.len() >= 128);
        assert!(plan.len() <= 512);
    }

    fn global_nodes() -> Vec<MeshNode> {
        vec![
            node("eu-london-1", "uk-south", "eu", 51.5074, -0.1278, TB, 100),
            node(
                "eu-frankfurt-1",
                "de-central",
                "eu",
                50.1109,
                8.6821,
                TB,
                100,
            ),
            node("na-virginia-1", "us-east", "na", 37.4316, -78.6569, TB, 100),
            node("na-oregon-1", "us-west", "na", 45.5152, -122.6784, TB, 100),
            node(
                "sa-sao-paulo-1",
                "br-south",
                "sa",
                -23.5558,
                -46.6396,
                TB,
                100,
            ),
            node(
                "apac-tokyo-1",
                "jp-east",
                "apac",
                35.6762,
                139.6503,
                TB,
                100,
            ),
            node(
                "apac-sydney-1",
                "au-east",
                "apac",
                -33.8688,
                151.2093,
                TB,
                100,
            ),
        ]
    }

    fn stream(stream_id: u64, bytes: u64) -> StreamInfo {
        StreamInfo {
            stream_id,
            bytes,
            contributor_node_id: None,
            active: true,
        }
    }

    fn node(
        node_id: &str,
        region: &str,
        continent: &str,
        latitude: f64,
        longitude: f64,
        total_storage_bytes: u64,
        used_storage_bytes: u64,
    ) -> MeshNode {
        MeshNode {
            node_id: node_id.into(),
            region: region.into(),
            continent: continent.into(),
            latitude,
            longitude,
            total_storage_bytes,
            used_storage_bytes,
            egress_capacity_bps: 10_000_000_000,
            contributor_streams: 0,
            active_streams: 0,
            draining: false,
        }
    }
}
