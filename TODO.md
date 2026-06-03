# TODO

## Mission Control Pause Point

Current uncommitted work in this repo:

- `src/main.rs`
  - Adds `PrivateDiscoveryStatus` to `OrchestrationStatus` in `/api/mesh`.
  - Reports whether private-subnet discovery is compiled, enabled, listening,
    and which broadcast/mesh ports it uses.
  - Raises `linode_private_discovery_inactive` when Linode provisioning is
    configured but private-subnet discovery is not active.
- `dashboard/src/main.rs`
  - Deserializes `private_discovery` from the mesh API.
  - Shows private discovery in the Leptos orchestration grid.
  - Marks the provision preview as warning when Linode provisioning is ready
    but private discovery is inactive.

Verification already run for this pause point:

```sh
cargo fmt
git diff --check
env -u NO_COLOR trunk build --release --dist /tmp/av-mesh-dashboard-private-discovery-check
cargo test --locked mesh_api_reports_orchestration_status
cargo test --locked mesh_alerts_when_linode_provisioning_lacks_private_discovery
cargo test --locked --features private-subnet-discovery private_discovery_status_reports_enabled_ports
cargo check --locked --target wasm32-unknown-unknown
```

All passed before this TODO was written.

## Mission Control Remaining Gaps

- Commit and push the private-discovery status work once reviewed.
- Add a richer provisioning detail view:
  - backend readiness per provider
  - last provision result
  - Linode private IP/VLAN/DNS details when available
  - bootstrap command status if `--provision-command` is chained after Linode
- Add explicit topology confidence:
  - peer address resolved/unresolved counts
  - private vs public peer address classification
  - stale telemetry node list in the graph, not only summary cards
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
cargo run --manifest-path ../av-contrib/Cargo.toml --bin local-obs-stack --release
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
