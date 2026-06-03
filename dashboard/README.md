# av-mesh-dashboard

Leptos CSR mission-control UI for `av-mesh` and `av-contrib`.

It consumes:

- `av-mesh` `/api/mesh`
- `av-contrib` `/api/status`
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

The app is intentionally separate from the `av-mesh` binary for now. Once the
UI shape settles, the built `dist/` assets can be served from `av-mesh` as the
replacement for the embedded `/mesh` HTML.
