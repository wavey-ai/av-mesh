# TODO

## Mission Control Current Status

Recent committed dashboard/API work:

- `/api/mesh` exposes `PrivateDiscoveryStatus` under
  `orchestration.private_discovery`.
- Mission Control shows private discovery state and warns when Linode
  provisioning is configured without active private-subnet discovery.
- Topology confidence is exposed and rendered, including resolved/unresolved
  peer counts and private/public target scope.
- Stale telemetry nodes remain visible in the topology graph as stale warning
  nodes instead of disappearing silently.

Verification already run for the recent mission-control work:

```sh
cargo fmt
git diff --check
env -u NO_COLOR trunk build --release --dist /tmp/av-mesh-dashboard-private-discovery-check
cargo test --locked mesh_api_reports_orchestration_status
cargo test --locked mesh_alerts_when_linode_provisioning_lacks_private_discovery
cargo test --locked --features private-subnet-discovery private_discovery_status_reports_enabled_ports
cargo check --locked --target wasm32-unknown-unknown
```

Additional dashboard verification was run for the topology-confidence and stale
topology updates:

```sh
cargo fmt
git diff --check
cargo test --locked telemetry_aggregator_resolves_peer_addresses_to_node_ids
cargo test --locked mesh_target_scope_classifies_private_addresses
cargo test --locked mesh_api_reports_operational_alerts
cargo check --locked --target wasm32-unknown-unknown
env -u NO_COLOR trunk build --release --dist /tmp/av-mesh-dashboard-topology-confidence-check
env -u NO_COLOR trunk build --release --dist /tmp/av-mesh-dashboard-stale-topology-check
```

## Mission Control Remaining Gaps

- Add a richer provisioning detail view:
  - backend readiness per provider
  - last provision result
  - Linode private IP/VLAN/DNS details when available
  - bootstrap command status if `--provision-command` is chained after Linode
- Add contributor-to-mesh path drill-down:
  - map `av-contrib` output stream IDs to mesh stream replicas
  - show latest contrib fMP4 part vs latest mesh part per node
  - surface end-to-end lag from ingest to edge playback
- Improve incident handling:
  - stable incident IDs
  - acknowledgement/suppression model
  - severity rollups by node, stream, and protocol
- Improve data-hose diagnostics:
  - expose reconnect/backoff state from mesh and contrib SSE clients
  - show age of last JSON poll separately from age of last SSE event
  - add WebSocket/WebTransport status if those become dashboard transports
- Add operator runbook links or short next-action text for high-priority alerts.

## Useful Local Commands

Run the local OBS stack from `../av-contrib`:

```sh
RUST_LOG=info cargo run --manifest-path ../av-contrib/Cargo.toml --bin local-obs-stack --release
```

Build just the dashboard:

```sh
cd dashboard
trunk build --release
```

Serve an existing dashboard build through `av-mesh`:

```sh
AV_MESH_DASHBOARD_DIST="$(pwd)/dashboard/dist" cargo run --release --bin av-mesh
```
