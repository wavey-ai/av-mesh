# av-mesh-dashboard

Leptos CSR mission-control UI for `av-mesh` and `av-contrib`.

It consumes:

- `av-mesh` `/api/mesh`
- `av-mesh` `/api/mesh/events`
- `av-contrib` `/api/status`
- `av-contrib` `/api/status/events`
- `av-mesh` `/api/control/*`

Run locally with Trunk:

```sh
cd /Users/jamie/wavey.ai/av-mesh/dashboard
cargo install trunk --locked
trunk serve --address 127.0.0.1 --port 5188
```

The default endpoint URLs match the local OBS stack:

- `https://local.bitneedle.com:19444/api/mesh`
- `https://local.bitneedle.com:19443/api/status`

The dashboard prefers the SSE event streams and falls back to JSON polling when
either service is unavailable or reconnecting.

Build for `av-mesh` to serve at `/mesh`:

```sh
cd /Users/jamie/wavey.ai/av-mesh/dashboard
trunk build --release
```

`av-mesh` serves `dashboard/dist/index.html` at `/mesh` when the build output is
present, and serves the hashed JS/CSS/WASM files from the same directory. Set
`AV_MESH_DASHBOARD_DIST=/path/to/dist` to point a running node at a different
dashboard build. If no built dashboard is available, `av-mesh` falls back to the
embedded legacy `/mesh` page.

The `../av-contrib` `local-obs-stack` binary builds this dist automatically
unless started with `--no-dashboard-build`, and injects the dist path into both
local mesh nodes with `AV_MESH_DASHBOARD_DIST`.
