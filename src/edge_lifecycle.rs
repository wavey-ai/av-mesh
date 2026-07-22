use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EdgeLifecycleConfig {
    pub enabled: bool,
    pub min_edges_per_region: usize,
    pub max_edges_per_region: usize,
    pub scale_up_active_readers: u64,
    pub idle_seconds: u64,
    pub provision_cooldown_seconds: u64,
}

impl Default for EdgeLifecycleConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_edges_per_region: 1,
            max_edges_per_region: 4,
            scale_up_active_readers: 80,
            idle_seconds: 900,
            provision_cooldown_seconds: 30,
        }
    }
}

impl EdgeLifecycleConfig {
    pub fn normalized(mut self) -> Self {
        self.min_edges_per_region = self.min_edges_per_region.max(1);
        self.max_edges_per_region = self.max_edges_per_region.max(self.min_edges_per_region);
        self.scale_up_active_readers = self.scale_up_active_readers.max(1);
        self.idle_seconds = self.idle_seconds.max(1);
        self.provision_cooldown_seconds = self.provision_cooldown_seconds.max(1);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeLifecycleNode {
    pub node_id: String,
    pub region: String,
    pub updated_unix_ms: u64,
    pub last_response_unix_ms: Option<u64>,
    pub active_readers: u64,
    pub egress_overloaded: bool,
    pub draining: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeLifecycleAction {
    Provision { region: String },
    Close { node_id: String, region: String },
}

#[derive(Debug, Default)]
pub struct EdgeLifecyclePlanner {
    last_provision_unix_ms: HashMap<String, u64>,
    pressure_samples: HashMap<String, u32>,
}

impl EdgeLifecyclePlanner {
    pub fn plan(
        &mut self,
        config: &EdgeLifecycleConfig,
        nodes: &[EdgeLifecycleNode],
        controller_node_id: &str,
        now_unix_ms: u64,
    ) -> Vec<EdgeLifecycleAction> {
        let config = config.clone().normalized();
        if !config.enabled {
            return Vec::new();
        }

        let mut regions: HashMap<&str, Vec<&EdgeLifecycleNode>> = HashMap::new();
        for node in nodes {
            regions.entry(node.region.as_str()).or_default().push(node);
        }

        let mut actions = Vec::new();
        let cooldown_ms = config.provision_cooldown_seconds.saturating_mul(1_000);
        let idle_ms = config.idle_seconds.saturating_mul(1_000);
        for (region, region_nodes) in regions {
            let live_nodes = region_nodes.iter().filter(|node| !node.draining).count();
            let readers = region_nodes
                .iter()
                .filter(|node| !node.draining)
                .map(|node| node.active_readers)
                .sum::<u64>();
            let overloaded = region_nodes
                .iter()
                .any(|node| !node.draining && node.egress_overloaded);
            let leader = region_nodes
                .iter()
                .filter(|node| !node.draining)
                .min_by(|left, right| left.node_id.cmp(&right.node_id));

            let pressure = overloaded || readers >= config.scale_up_active_readers;
            let samples = if pressure {
                let samples = self.pressure_samples.entry(region.to_owned()).or_default();
                *samples = samples.saturating_add(1);
                *samples
            } else {
                self.pressure_samples.remove(region);
                0
            };
            if live_nodes < config.max_edges_per_region
                && samples >= 2
                && leader.is_some_and(|node| node.node_id == controller_node_id)
            {
                let last = self
                    .last_provision_unix_ms
                    .get(region)
                    .copied()
                    .unwrap_or(0);
                if now_unix_ms.saturating_sub(last) >= cooldown_ms {
                    self.last_provision_unix_ms
                        .insert(region.to_owned(), now_unix_ms);
                    actions.push(EdgeLifecycleAction::Provision {
                        region: region.to_owned(),
                    });
                    continue;
                }
            }

            if live_nodes <= config.min_edges_per_region
                || !leader.is_some_and(|node| node.node_id == controller_node_id)
            {
                continue;
            }
            let candidate = region_nodes
                .iter()
                .filter(|node| !node.draining && node.active_readers == 0)
                .filter(|node| {
                    let activity = node.last_response_unix_ms.unwrap_or(node.updated_unix_ms);
                    now_unix_ms.saturating_sub(activity) >= idle_ms
                })
                .min_by(|left, right| {
                    let left_activity = left.last_response_unix_ms.unwrap_or(left.updated_unix_ms);
                    let right_activity =
                        right.last_response_unix_ms.unwrap_or(right.updated_unix_ms);
                    left_activity
                        .cmp(&right_activity)
                        .then_with(|| left.node_id.cmp(&right.node_id))
                });
            if let Some(node) = candidate {
                actions.push(EdgeLifecycleAction::Close {
                    node_id: node.node_id.clone(),
                    region: region.to_owned(),
                });
            }
        }

        actions.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));
        actions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, readers: u64, overloaded: bool, activity: u64) -> EdgeLifecycleNode {
        EdgeLifecycleNode {
            node_id: id.into(),
            region: "uk".into(),
            updated_unix_ms: activity,
            last_response_unix_ms: Some(activity),
            active_readers: readers,
            egress_overloaded: overloaded,
            draining: false,
        }
    }

    #[test]
    fn overload_provisions_once_per_cooldown() {
        let config = EdgeLifecycleConfig {
            enabled: true,
            max_edges_per_region: 3,
            provision_cooldown_seconds: 30,
            ..EdgeLifecycleConfig::default()
        };
        let mut planner = EdgeLifecyclePlanner::default();
        assert!(planner
            .plan(
                &config,
                &[node("edge-a", 1, true, 100_000)],
                "edge-a",
                100_000
            )
            .is_empty());
        assert_eq!(
            planner.plan(
                &config,
                &[node("edge-a", 1, true, 100_000)],
                "edge-a",
                105_000
            ),
            vec![EdgeLifecycleAction::Provision {
                region: "uk".into()
            }]
        );
        assert!(planner
            .plan(
                &config,
                &[node("edge-a", 1, true, 100_000)],
                "edge-a",
                110_000
            )
            .is_empty());
    }

    #[test]
    fn idle_edges_close_but_keep_one() {
        let config = EdgeLifecycleConfig {
            enabled: true,
            idle_seconds: 60,
            ..EdgeLifecycleConfig::default()
        };
        let mut planner = EdgeLifecyclePlanner::default();
        let nodes = [node("edge-a", 0, false, 0), node("edge-b", 0, false, 0)];
        assert_eq!(
            planner.plan(&config, &nodes, "edge-a", 61_000),
            vec![EdgeLifecycleAction::Close {
                node_id: "edge-a".into(),
                region: "uk".into()
            }]
        );
    }
}
