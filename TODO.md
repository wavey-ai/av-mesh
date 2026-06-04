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

Current dashboard data-hose diagnostics work in this tree:

- Mission Control separates JSON poll attempts/success/errors from SSE message,
  reconnect, and error state for mesh and contrib feeds.
- The dashboard data-hose cards show last JSON poll attempt/ok/error ages
  separately from last SSE event/reconnect ages.
- SSE diagnostics expose the browser `EventSource.readyState`, reconnect count,
  consecutive reconnect count, and browser-managed reconnect/backoff state.

Verification run for the data-hose diagnostics update:

```sh
cargo fmt --manifest-path dashboard/Cargo.toml
git diff --check
cargo check --manifest-path dashboard/Cargo.toml --locked --target wasm32-unknown-unknown
```

Current contributor-to-mesh path drill-down work in this tree:

- Mission Control adds a `Contributor To Mesh Paths` table that derives path
  rows from `av-contrib` runtime stream IDs, the advertised HLS stream ID,
  `av-mesh` observed stream telemetry, and planned replicas.
- Each path row shows contributor ingest age, fMP4 output age/sequence, mesh
  forwarding age, observed/planned mesh replicas, per-node local/mesh part
  heads, latest aggregate fMP4/mesh/local part heads, worst mesh lag, edge
  playlist probe readiness, estimated source-to-edge playback freshness,
  aggregate contributor-to-mesh and mesh receive rates, and
  degraded/lagging/stale/missing status.
- Playback probe failures for a stream now escalate the corresponding
  contributor-to-mesh path row to `playback down`, so an otherwise linked path
  does not appear healthy when edge playlist reads are failing.
- Mesh-only and contributor-only stream IDs stay visible so mismatches between
  contributor output and mesh replication are operationally obvious.

Verification run for the path drill-down update:

```sh
cargo fmt --manifest-path dashboard/Cargo.toml
git diff --check
cargo check --manifest-path dashboard/Cargo.toml --locked --target wasm32-unknown-unknown
env -u NO_COLOR TRUNK_COLOR=never trunk build --release --dist /tmp/av-mesh-dashboard-path-drilldown-check
env -u NO_COLOR TRUNK_COLOR=never trunk build --release --dist /tmp/av-mesh-dashboard-e2e-lag-check
```

Current incident handling work in this tree:

- Mission Control incidents now carry stable IDs derived from source, code, and
  stable target data such as node/stream details or playback probe URL.
- The incident list uses the stable ID as the Leptos key and includes it in the
  metadata line so future acknowledgement/suppression state has a durable key.
- Mission Control has a browser-local acknowledgement/suppression model keyed
  by stable incident ID. Acknowledged incidents stay visible but de-emphasized,
  suppressed incidents leave the rollup until the operator resets local
  dispositions, and incident summaries include acknowledged/suppressed counts.
- Mission Control incident summaries now roll active incidents up by node,
  stream, and protocol, with separate error/warn/info, acknowledged, and
  suppressed counts for each affected target.

Verification run for the stable incident ID update:

```sh
cargo fmt --manifest-path dashboard/Cargo.toml
git diff --check
cargo check --manifest-path dashboard/Cargo.toml --locked --target wasm32-unknown-unknown
env -u NO_COLOR TRUNK_COLOR=never trunk build --release --dist /tmp/av-mesh-dashboard-incident-ids-check
env -u NO_COLOR TRUNK_COLOR=never trunk build --release --dist /tmp/av-mesh-dashboard-incident-dispositions-check
env -u NO_COLOR TRUNK_COLOR=never trunk build --release --dist /tmp/av-mesh-dashboard-incident-rollups-check
```

Current provisioning detail work in this tree:

- Mission Control now expands the provisioning summary into provider readiness,
  latest provision result, Linode private-network result, and chained bootstrap
  command cards.
- Linode detail cards parse the latest provision result for private IPAM, VLAN,
  DNS, public IPv4, instance id, and provider region when those fields are
  present in the command status.
- The local Linode bootstrap script accepts `AV_MESH_BOOTSTRAP_SSH_KEY` so smoke
  tests can force the SSH identity matching the public key passed to the Linode
  provisioner.

Verification run for the provisioning detail update:

```sh
cargo fmt --manifest-path dashboard/Cargo.toml
cargo check --manifest-path dashboard/Cargo.toml --locked --target wasm32-unknown-unknown
env -u NO_COLOR TRUNK_COLOR=never trunk build --release --dist /tmp/av-mesh-dashboard-provision-detail-check
```

Current mesh repair/backfill work in this tree:

- Mesh replica requests now start from the earliest missing retained slot for a
  stream instead of always starting after the latest present slot. This lets
  playlist demand, LL-HLS tail demand, media demand, warm-stream controls, and
  baseline replica placement backfill holes inside the retained live window with
  the existing mesh replica-request path.
- The larger transport boundary still stands: if a missing slot has fallen out
  of the retained live window, or if no peer can serve it inside the latency
  budget, the mesh still needs a stronger TCP/QUIC/RIST-like repair path.

Verification run for the retained-window backfill update:

```sh
cargo fmt
cargo test --locked replica_request_backfills_missing_retained_stream_slot
cargo test --locked demand
cargo test --locked warm_stream
```

## Mission Control Remaining Gaps

- Improve data-hose diagnostics:
  - add WebSocket/WebTransport status if those become dashboard transports
- Add operator runbook links or short next-action text for high-priority alerts.

## Deployment Test Direction

- Use k3d for local multi-node deployment tests so the smoke environment uses
  the same k3s Kubernetes surface intended for edge nodes.
- Keep direct process tests for packet-level latency/FEC behavior, but use k3d
  manifests to verify service discovery, readiness, dashboard routing, and
  rolling deployment behavior.
- The initial k3d deployment path now includes a local image Dockerfile,
  two-node UK/US Kubernetes manifests, Makefile targets, and
  `scripts/k3d-smoke.sh` for `up`, `check`, `forwards`, and `down`.
- `--peer` and `--telemetry-peer` now resolve DNS names at startup so k3d/k3s
  deployments can use Kubernetes service names instead of literal IPs.
- Local tooling has been installed for the next smoke run: Docker CLI, Colima,
  k3d, and kubectl. The smoke run was paused before creating a cluster; Colima
  is stopped and no k3d cluster or port-forward should be left running.
- Next local deployment step: start Colima, run `./scripts/k3d-smoke.sh up`,
  verify both mesh dashboard URLs and `/api/mesh`, then either leave the local
  cluster up for inspection or run `make k3d-down`.

Verification run for the k3d deployment setup:

```sh
bash -n scripts/k3d-smoke.sh
cargo fmt
cargo test --locked parses_dns_peer_targets_for_orchestrated_deployments
cargo check --locked
cargo check --manifest-path dashboard/Cargo.toml --locked --target wasm32-unknown-unknown
git diff --check
```

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
