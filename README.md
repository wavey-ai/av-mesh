# av-mesh

[![CI](https://github.com/wavey-ai/av-mesh/actions/workflows/ci.yml/badge.svg)](https://github.com/wavey-ai/av-mesh/actions/workflows/ci.yml)

`av-mesh` is the local prototype for a demand-driven audio/video mesh. A
contributor can publish media into one region, all media is stored in
playlist/cache streams, cache slots replicate to other regions over the shared
Wavey RaptorQ-FEC datagram protocol, and users can read replicated streams as
LL-HLS artifact bytes or other stream-addressed slots from any region.

RaptorQ-FEC is the mesh transport. Cache synchronization inside the mesh moves
opaque stream bytes over the Wavey RaptorQ datagram protocol. Contributor-facing
RIST/SRT/RTMP input belongs in `../av-contrib`, which terminates those
protocols, packages OBS-style media as fMP4/CMAF LL-HLS artifacts, and publishes
only stream-addressed artifact bytes into the mesh FEC socket.

Transport reliability stance: RaptorQ-FEC is the right live mesh hot path for
bounded packet loss because it can repair without waiting for RTT. It is not a
general replacement for RIST/SRT ARQ on arbitrary bad WAN paths. If loss exceeds
the configured repair budget, FEC fails closed; RIST/SRT can still recover later
by retransmitting from history if the latency budget allows. The mesh should
therefore stay slot/artifact-native with RaptorQ for live propagation, but needs
an explicit missing-slot repair/backfill path over TCP, QUIC, or a RIST-like ARQ
channel before we claim RIST-class eventual reliability.

The first implementation keeps the mesh transport intentionally small:

- `playlists::mesh` provides UDP-FEC cache discovery and slot replication using
  the reusable `raptorq-datagram-fec` crate. Seed peers and private-subnet
  broadcast discovery only bootstrap the peer set; normal mesh `HELLO` frames
  gossip known peer addresses after that.
- Optional `private-subnet-discovery` support can add mesh peers discovered on a
  10.x private subnet by the existing `discovery` crate's VLAN broadcast path.
  `linode-private-discovery` is an alias for the same generic feature, intended
  for Linode VLAN deployments provisioned by the sibling `linode` project.
- Playlist reads and warm-stream controls send mesh replica requests, so a node
  with local demand can ask peers for a stream immediately.
- Replica requests begin at the earliest missing retained slot for the requested
  stream, which gives live playlist/tail/media demand a retained-window
  backfill path without changing the FEC datagram wire format.
- LL-HLS playlist rendering is cached by stream and `ChunkCache` content
  version. Slot writes and stream reuse invalidate the derived manifest, while
  unchanged requests avoid repeatedly taking every retained-slot read lock.
- `web-service` serves the operator/UI edge over HTTPS/TCP: HLS playlists,
  parts, segments, health checks, JSON mesh snapshots, server-sent mesh events,
  and HTTP `POST /api/control/*` commands. This is not the mesh transport.
- Mesh control commands are sent node-to-node over TCP changes as `AVMC` frames.
  A local HTTP/API command records intent, optionally applies it locally, and
  publishes an `AVMC` envelope to telemetry peers for regional/node targeting.
- Optional raw TCP accepts the same snapshot/control and serialized media
  access-unit bytes in length-prefixed frames. Completed access units are cached
  as stream slots.
- WebSocket and WebTransport handlers remain as explicit edge paths only. They
  are disabled by default; use `--edge-websocket` or
  `--edge-webtransport` when a client specifically needs them.
- `message-packetizer` remains available for bounded UDP-style announcements;
  operator commands use `AVMC` over TCP changes.
- Contributor ingest through `../av-contrib` supports arbitrary non-OBS byte
  streams via `POST`/`PUT /ingest?stream_id=...`, pure-Rust RIST MPEG-TS, SRT
  MPEG-TS, RTMP/FLV, and HTTP `POST`/`PUT /media/access-unit` non-TS media
  access units. OBS-style inputs are boxed into fMP4/CMAF before entering mesh;
  raw RIST, SRT, RTMP, and MPEG-TS payloads stay outside the mesh boundary.
- The sibling `../av-contrib` project is the contributor-facing repo boundary.
  It currently owns
  non-TS media access-unit query parsing and uses `raptorq-datagram-fec` for
  serialized access-unit decoding, so those edge formats are no longer defined
  inside the mesh binary.
- `/mesh` serves Needletail Mission Control assets supplied by the product
  supervisor. Mission Control presents service/feed health, compiled delivery
  programs, independent primary-source and warm-secondary-repair lanes,
  RaptorQ recovery, deadline health, publication continuity, and realtime
  latency.
- Optional `--telemetry-bind` publishes JSON mesh snapshots to a TCP changes feed
  with the `AVMT` tag for central aggregation without scraping stdout. Snapshots
  include the node's mesh socket address so `/api/mesh` can resolve peer
  addresses back to node ids for the topology graph when both sides report.
  Aggregated remote snapshots older than `--telemetry-stale-ms` are pruned from
  capacity totals, topology, replica planning, and regional control targeting.
- Optional `--telemetry-peer` connects a central node to other nodes' TCP changes
  feeds and merges their `AVMT` snapshots into `/api/mesh` and `/mesh`. The same
  feed also carries `AVMC` control commands. Region-scoped commands include
  telemetry-selected target node ids when known, so subscribers execute matching
  warm/close requests on concrete nodes instead of relying only on broad region
  filtering.
- Optional `--provision-command` lets the UI/API `provision_node` control hand
  off to an operator-provided shell command. The command receives
  `AV_MESH_PROVISION_NODE_ID`, `AV_MESH_PROVISION_REGION`,
  `AV_MESH_LOCAL_NODE_ID`, `AV_MESH_LOCAL_REGION`, and `AV_MESH_CONTROL_ID`.
- With `--features linode-provisioner`, `--linode-provision` uses the sibling
  `../linode` crate to create a node, attach it to the region VLAN with a
  `10.0.0.x/24` private address, reboot it, and update DNS. Use
  `--linode-region-map mesh-region=linode-region` for mesh names such as
  `uk=gb-lon`; `linode-private-discovery` enables this provisioner together
  with private-subnet discovery.
- `av_mesh::replication` contains the tested replica planner for baseline
  continent/region staging, demand-triggered local replicas, node storage
  capacity, and anti-affinity for nearby nodes. Telemetry includes all active
  cache stream ids, so baseline staging can pull non-default media/access-unit
  streams as well as the default playlist stream, and `/api/mesh` exposes
  planned replicas across those stream ids for the control API.
- `/api/mesh` exposes stream ids as browser-safe decimal strings in
  `stream_id_text` fields wherever a numeric `stream_id` appears, including the
  local stream stats, active stream telemetry, planned replicas, and recent
  control commands. HTTP control requests accept `stream_id` as either a JSON
  number or a decimal string; browser clients should send strings for Snowflake
  ids.
- `/api/mesh` also exposes `edge_services` with each node's advertised
  `playback_base_url`, active reader count, served request/byte counters, and
  LLHLS tail poll count. It also carries cumulative LL-HLS handler latency
  histograms and p95 latency so operators can separate origin freshness from
  edge response-generation time. Player-facing services such as `av-llhls`
  should use one or more seed nodes only to discover candidates, then score
  those candidates locally instead of routing all playback through a central
  service.
- `/api/mesh` exposes the active cache-mesh transport policy: replication scan
  interval, FEC symbol size, minimum repair symbols, proportional repair ratio,
  and maximum repair symbols. The default one-symbol floor plus 3% proportional
  repair avoids paying a fixed high redundancy cost for small parts while
  protecting larger high-throughput parts from multi-packet loss.
- `/api/mesh` also exposes `mesh_fec` runtime outcomes: source and repair
  datagrams, protected and wire bytes, decoded objects, objects and source
  symbols actually recovered through repair data, late source arrivals,
  repaired sources still absent after a bounded observation window,
  incomplete/expired objects, and encode/decode errors. RelaySession ingress
  exposes the product recovery counters consumed by Mission Control.
- `/metrics` exposes Prometheus text metrics for topology, telemetry freshness,
  node capacity, edge traffic/errors, per-stream ingest/replica lag, current
  alerts, active mesh transport configuration, cache-mesh FEC traffic and
  recovery outcomes, and per-node LL-HLS response latency histograms. Scrapes
  render from the same bounded telemetry snapshot used by `/api/mesh`.

## Local two-region prototype

Run the UK node:

```bash
cargo run -- \
  --region uk \
  --node-id uk-1 \
  --mesh-bind 127.0.0.1:9101 \
  --peer 127.0.0.1:9201 \
  --http-port 9444 \
  --playback-base-url https://127.0.0.1:9444/live \
  --fec-bind 127.0.0.1:12001 \
  --media-fec-bind 127.0.0.1:12101 \
  --telemetry-bind 127.0.0.1:7300 \
  --provision-command 'echo provision node=$AV_MESH_PROVISION_NODE_ID region=$AV_MESH_PROVISION_REGION'
```

For private-subnet discovery on Linode VLANs or any 10.x subnet, build with the
feature and bind mesh UDP on an address reachable from that subnet:

```bash
cargo run --features private-subnet-discovery -- \
  --mesh-bind 0.0.0.0:9101 \
  --private-subnet-discovery \
  --private-discovery-broadcast-port 12345 \
  --private-discovery-mesh-port 9101
```

`--peer` remains useful as a seed list. Private discovery adds peers at runtime
from UDP broadcast announcements, then normal mesh `HELLO` frames gossip peer
addresses. Telemetry fills in the observable node topology for the operator UI.

For Linode-backed provisioning and private-subnet discovery together:

```bash
cargo run --features linode-private-discovery -- \
  --region uk \
  --mesh-bind 0.0.0.0:9101 \
  --private-subnet-discovery \
  --linode-provision \
  --linode-image-id linode/arch \
  --linode-instance-type g6-dedicated-2 \
  --linode-domain-id "$LINODE_DOMAIN_ID" \
  --linode-vlan-tag av-mesh \
  --linode-region-map uk=gb-lon \
  --linode-region-map us=us-east \
  --provision-command "$(pwd)/scripts/bootstrap-linode-node.sh"
```

The Linode provisioner reads API credentials from `LINODE_API_TOKEN` and
`LINODE_PUB_KEY` by default. If a `--provision-command` is also configured, it
runs after a successful Linode API provision and receives
`AV_MESH_LINODE_INSTANCE_ID`, `AV_MESH_LINODE_PUBLIC_IPV4`,
`AV_MESH_LINODE_PRIVATE_IPAM`, `AV_MESH_LINODE_DNS_NAME`, and
`AV_MESH_LINODE_VLAN_LABEL`.

`scripts/bootstrap-linode-node.sh` waits for SSH, syncs the local Wavey workspace
needed by the current path dependencies, builds `av-mesh` on the node with
private-subnet discovery, installs a systemd service, and reuses or generates
shared local TLS material for TCP changes telemetry. Run it with
`AV_MESH_BOOTSTRAP_DRY_RUN=1` to inspect the remote actions without connecting.

Run the US node:

```bash
cargo run -- \
  --region us \
  --node-id us-1 \
  --mesh-bind 127.0.0.1:9201 \
  --peer 127.0.0.1:9101 \
  --http-port 9445 \
  --fec-bind 127.0.0.1:12002 \
  --media-fec-bind 127.0.0.1:12102 \
  --telemetry-bind 127.0.0.1:7301 \
  --telemetry-peer 127.0.0.1:7300
```

Publish MPEG-TS bytes over UDP-FEC into the UK mesh byte socket:

```bash
ffmpeg -re -f lavfi -i testsrc=size=1280x720:rate=30 \
  -f lavfi -i sine=frequency=1000:sample_rate=48000 \
  -c:v libx264 -preset veryfast -tune zerolatency \
  -c:a aac -f mpegts - | \
  cargo run --manifest-path ../av-contrib/Cargo.toml --bin udp-fec-send -- 127.0.0.1:12001
```

Or run a contributor frontend for HTTP/RIST uploads and point it at the UK mesh
FEC sockets:

```bash
cargo run --manifest-path ../av-contrib/Cargo.toml --bin av-contrib -- \
  --http-port 9443 \
  --mesh-fec-target 127.0.0.1:12001 \
  --mesh-media-fec-target 127.0.0.1:12101 \
  --rist-bind 127.0.0.1:7000
```

Then publish over RIST with a RIST-capable sender such as OBS:

- URL: `rist://127.0.0.1:7000`
- Profile: `main`
- Flow ID: `0x72737401`

Or with the included stdin RIST sender:

```bash
ffmpeg -re -f lavfi -i testsrc=size=1280x720:rate=30 \
  -f lavfi -i sine=frequency=1000:sample_rate=48000 \
  -c:v libx264 -preset veryfast -tune zerolatency \
  -c:a aac -f mpegts - | \
  cargo run --manifest-path ../av-contrib/Cargo.toml --bin rist-send -- 127.0.0.1:7000
```

Or publish MPEG-TS bytes over HTTP to the contributor frontend:

```bash
ffmpeg -re -f lavfi -i testsrc=size=1280x720:rate=30 \
  -f lavfi -i sine=frequency=1000:sample_rate=48000 \
  -c:v libx264 -preset veryfast -tune zerolatency \
  -c:a aac -f mpegts - | \
  curl -k -X POST --data-binary @- https://127.0.0.1:9443/ingest
```

Or publish a non-TS media access unit through the contributor frontend. The
contributor wraps the access unit for the media UDP-FEC socket; the mesh stores
the `raptorq-datagram-fec` media header followed by the access-unit payload, so
the same stream can replicate over cache mesh on demand:

```bash
printf 'h264-access-unit-bytes' | \
  curl -k -X POST --data-binary @- \
  'https://127.0.0.1:9443/media/access-unit?stream_id=55&sequence=0&codec=h264&pts_ms=0&duration_ms=33&keyframe=true'

curl -k https://127.0.0.1:9444/media/55/unit/0 --output unit.avmau
```

Or publish the same kind of non-TS access unit over the RaptorQ media/FEC
datagram path:

```bash
printf 'h264-access-unit-bytes' | \
  cargo run --manifest-path ../av-contrib/Cargo.toml --bin media-fec-send -- \
    --stream-id 55 \
    --sequence 1 \
    --codec h264 \
    --pts-ms 33 \
    --duration-ms 33 \
    --keyframe \
    127.0.0.1:12101
```

Then read either region:

- UK default playlist: `https://127.0.0.1:9444/live/stream.m3u8`
- UK stream-specific playlist for playlist/stream id 1:
  `https://127.0.0.1:9444/live/1/stream.m3u8`
- UK LLHLS tail for playlist/stream id 1:
  `https://127.0.0.1:9444/live/1/tail?mode=part`
- UK mesh UI: `https://127.0.0.1:9444/mesh`
- US default playlist: `https://127.0.0.1:9445/live/stream.m3u8`
- US stream-specific playlist for playlist/stream id 1:
  `https://127.0.0.1:9445/live/1/stream.m3u8`
- Health: `https://127.0.0.1:9444/up`
- Stats: `https://127.0.0.1:9444/api/stats`
- Mesh snapshot: `https://127.0.0.1:9444/api/mesh`
- Mesh event stream: `https://127.0.0.1:9444/api/mesh/events`
- Edge WebSocket: opt in with `--edge-websocket`, then use
  `wss://127.0.0.1:9444/ws/mesh`.
- Edge WebTransport: opt in with `--edge-webtransport`; media datagrams can be
  raw RQD2 or `raptorq-fec-transport` stream-prefixed RQD2.
- Mesh/control raw TCP: opt in with `--raw-tcp-port <port>`. Each request and
  response is framed as `[u32_be length][payload]`; payloads can be JSON mesh
  protocol requests or serialized media access units. Add `--raw-tcp-tls` to
  wrap the raw TCP listener with the same TLS material.
- Control messages: `POST /api/control/provision-node`,
  `POST /api/control/close-node`, and `POST /api/control/warm-stream` originate
  commands locally. Nodes with a telemetry publisher forward those commands as
  `AVMC` frames over TCP changes; telemetry peers ingest matching `AVMC` frames
  and execute them by region or node id.

For `av-llhls`, pass the selected node's `playback_base_url` as `baseUrl` and
the decimal-string playlist/stream id as `streamId`. The client will poll
`/live/<stream_id>/tail?mode=part&after=<sequence>`; if the selected node does
not have the stream yet, the first tail polls create mesh demand and return
`204` until replicated bytes arrive. It remains `av-llhls`'s responsibility to
choose a playlist id whose bytestream is compatible with its decoder.

The server uses the local TLS material from `av-service`; clients will need to
trust that cert or use an insecure local test client.

## Realtime performance gate

With a local or deployed contributor-plus-mesh stack already running, collect
repeatable client and service latency measurements with:

```sh
make realtime-benchmark
```

The benchmark sends raw contributor payloads and reads each configured mesh
playlist at configurable concurrency. It prefers persistent HTTP/2 sessions
through `h2load`, falls back to parallel curl processes, and reports
client-observed p50/p95/p99, mean, max, and effective request rate. Duration
mode (`DURATION_SECONDS`) sustains load instead of stopping at a request count.
`PROPAGATION_PROBES` posts unique canaries and concurrently measures when the
exact bytes become fetchable from each edge. Before/after Prometheus snapshots
produce service histogram count and p95 deltas rather than merely checking
that metrics exist. Contributor results also attribute interval p95 to bounded
`encode_wait`, `encode`, `send`, and `telemetry` stages, both in the terminal
and in `RESULT_JSON`, so a forwarding regression identifies its hot path.
`RESULT_JSON` persists machine-readable evidence. Override
topology and load through `CONTRIB_URL`, comma-separated `MESH_URLS`,
`CONCURRENCY`, `H2_STREAMS_PER_CLIENT`, `PAYLOAD_BYTES`, and `LOAD_CLIENT`.
Set `PARALLEL_ENDPOINTS=1` to load the contributor and every edge at the same
time. `CONTRIB_METRICS_URL` and `MESH_METRICS_URLS` can point service histogram
scrapes at private monitoring endpoints while client load uses public origins.

Budgets are explicit and opt-in so a laptop result is not presented as a global
SLO. For example:

```sh
INGEST_P95_BUDGET_MS=15 PLAYLIST_P95_BUDGET_MS=10 \
  SAMPLES=1000 CONCURRENCY=32 make realtime-benchmark
```

Any unexpected HTTP status, missing metrics surface, or p95 budget violation
fails the command.

For the repeatable two-region qualification, including real packet impairment:

```sh
make realtime-qualification
```

This runs baseline and impaired phases against the same release stack. The
default impaired profile applies 35±5 ms one-way mesh delay with 1% loss and
10±2 ms contributor-FEC delay with 1% loss through the unprivileged
`udp-netem` binary. It verifies that both links carried and dropped packets,
rejects emulator overflow/send errors, checks client and service p95 budgets,
requires the impaired mesh phase to demonstrate actual source-symbol recovery
without FEC decode errors, and writes baseline, impaired, link-stat, recovery,
and combined qualification artifacts under `target/realtime-qualification/`.
Default gates are 15 ms contributor ingest/forwarding p95, 5 ms playlist p95,
1 ms edge-handler p95, 200 ms ingest-to-edge propagation p95, and a 3x
impaired/baseline p95 ratio. All profile and latency-budget values are
environment-overridable.

For an authorized deployed canary, run repeated simultaneous windows and
preserve round-level plus whole-soak evidence with:

```sh
CONTRIB_URL=https://contrib-canary.example \
MESH_URLS=https://uk-canary.example,https://us-canary.example \
SOAK_SECONDS=3600 ROUND_SECONDS=60 \
  make realtime-soak
```

The soak defaults to verified TLS, 8 HTTP/2 connections × 4 streams per
endpoint, exact-byte propagation probes, and the provisional local latency
gates. It fails on any bad round, counter reset, new contributor/mesh pipeline
error, MPEG-TS continuity error, or expired FEC object unless the corresponding
limit is explicitly overridden. Each run writes raw metric snapshots, per-round
logs/JSON, counter deltas, cross-round percentiles, and `soak.json` under
`target/realtime-soak/`. Use a dedicated canary stream and explicitly scoped
hosts; the script never deploys or changes servers.

## Persistent observability

`observability/` contains a runnable Prometheus, Alertmanager, and Grafana
bundle. It persists the metrics exposed by `av-contrib` and `av-mesh`, records
forwarding-stage and mesh-handler p95s, charts FEC recovery and wire overhead,
and evaluates bounded-label alerts with diagnosis guidance.

```sh
make observability-check
docker compose -f observability/compose.yml up -d
```

Grafana is served at `http://127.0.0.1:3000/d/wavey-realtime`, Prometheus at
`http://127.0.0.1:9090`, and Alertmanager at `http://127.0.0.1:9093`. The local
scrape configuration trusts the development certificates insecurely; deployed
scrapers must use the deployment CA. Alert thresholds are labeled
`slo: provisional` until a hardware-, geography-, bitrate-, and load-qualified
regional soak establishes production SLOs. See `observability/README.md` for
deployment and notification-routing notes.

## Needletail Mission Control

The product UI lives in `../needletail/mission-control`. Its Leptos/WASM app
consumes `av-mesh` `/api/mesh` and `av-contrib` `/api/status` using bounded,
Serde-default snapshot models.

```bash
make mission-control-check
make mission-control-build
make mission-control-serve
```

By default it points at the local OBS stack endpoints:
`https://local.bitneedle.com:19444/api/mesh` and
`https://local.bitneedle.com:19443/api/status`.

For local OBS testing with both playback edges and the contributor ingress under
one Rust supervisor, use Needletail:

```bash
make local-stack
```

The supervisor builds release `av-mesh`, release `../av-contrib`, and
Needletail Mission Control, then passes the product assets to each playback edge
with `NEEDLETAIL_MISSION_CONTROL_DIST`. It uses the local bitneedle TLS material from
`../tls/local.bitneedle.com`, starts UK and US mesh nodes plus one `av-contrib`
ingress, and prefixes every child process stdout/stderr line into the supervisor
stdout. By default it uses stream id `1`, UK egress
`https://local.bitneedle.com:19444/live/1/stream.m3u8`, US egress
`https://local.bitneedle.com:19445/live/1/stream.m3u8`, and Mission Control at
`/mesh` on both ports. OBS can publish RTMP to server
`rtmp://local.bitneedle.com:19350/live` with stream key `obs-local`, or SRT to
`srt://local.bitneedle.com:27001?mode=caller`. RIST is also bound on
`local.bitneedle.com:27000` with main profile and flow id `0x11223344`. The
supervisor defaults the LL-HLS part target to 50 ms, accepts
`AV_LL_HLS_PART_MS` or `--part-ms` overrides, and shells out to `curl` for local
health checks.

Useful overrides:

```bash
PART_MS=67 \
RUST_LOG=av_mesh=trace,av_contrib=trace,rtmp_ingress=debug \
  STACK_ARGS="--rtmp-bind 127.0.0.1:19351 --srt-bind 127.0.0.1:27011" \
  make local-stack STREAM_ID=4294967351 HOST=local.bitneedle.com
```

Use `--cert` and `--key` to point at alternate PEM files. The default hostname
must resolve to loopback; on this machine `local.bitneedle.com` resolves to
`127.0.0.1` and `::1`.

Use `--mission-control-dist /path/to/dist` to reuse specific product assets.
Use `--no-mission-control-build` with an existing build; `/mesh` returns concise
setup guidance while the asset path is being prepared. `--no-build` reuses the
component release binaries. Run `make help` for direct playback-edge and Mission
Control tasks.

## Local k3d deployment

Use k3d for the local orchestration smoke path. It runs k3s nodes in Docker,
which keeps the test close to edge Kubernetes while still being disposable on a
developer machine.

Prerequisites:

```bash
brew install k3d kubectl
# Docker Desktop, Colima, or another Docker-compatible runtime must be running.
```

Build the local image, create a two-node k3d cluster, deploy UK/US mesh nodes,
and start port-forwards:

```bash
make k3d-up
```

The smoke script builds `deploy/k3d/Dockerfile`, imports `av-mesh:local` into
the cluster, generates a short-lived TLS secret, applies
`deploy/k3d/av-mesh.yaml`, waits for both deployments, and checks `/up` and
`/api/mesh` through local port-forwards.

Useful follow-up commands:

```bash
make k3d-check
kubectl -n av-mesh get pods,svc
make k3d-down
```

The services already use `tracing_subscriber` with `RUST_LOG` env filters. The
current detailed logs are mostly `info!` and `debug!`; setting `trace` is
accepted, but only code paths with `trace!` calls will emit extra trace-level
events. A true same-process Tokio harness should be the next refactor: expose
library `run(config, shutdown)` entry points from `av-mesh` and `av-contrib`,
then have this supervisor call those tasks directly instead of supervising child
binaries.

For a quick automated check, run:

```bash
scripts/two-region-smoke.sh
```

The smoke script builds the binaries, generates short-lived local TLS material
for `tcp-changes`, starts UK and US nodes plus a UK `av-contrib` frontend on
local high ports with the RaptorQ cache mesh configured, sends distinct HTTP,
UDP-FEC, and RIST contributor payloads into UK, verifies those exact HLS parts
can be read from both UK and US by several concurrent clients, and proves the
`AVMT`/`AVMC` TCP changes path by sending a UK-originated warm-stream command
to the US node.

## Current scope

This is not the final multi-protocol edge deployment yet. The current milestone
proves the shared-cache behavior needed by the requested mesh:

1. A node can discover configured peers with UDP-FEC `HELLO` frames, gossip
   those peer addresses through the mesh, and with the optional private-subnet
   discovery feature it can add 10.x subnet peers discovered by UDP broadcast.
2. A node can ingest opaque stream bytes from UDP-FEC or a contributor frontend
   such as `av-contrib`, then write media parts to a `playlists::ChunkCache`.
3. Peers replicate those slots over the shared Wavey RaptorQ-FEC datagram
   protocol, then serve them as HLS parts.
4. Region and continent identity are explicit, starting with `uk` and `us`.
5. The first replication planner tests local global distributions, demand
   signals, storage capacity, and anti-affinity so close nodes are not selected
   as redundant mirrors when better placements exist.
6. Playlist demand and warm-stream controls broadcast replica requests over the
   mesh transport, and peers with the requested stream send cached slots back to
   the requester.
7. The first telemetry/UI path exposes local snapshots through HTTPS and can
   publish the same snapshots over a TCP changes feed for a central collector.
   A node started with `--telemetry-peer` consumes those feeds and exposes an
   aggregate node/connection/capacity/edge-service view.
8. The runtime planner exposes planned replicas through `/api/mesh` and asks for
   the stream when the local node is selected as a baseline replica.
9. UI/API/raw TCP controls publish `AVMC` command frames over TCP changes when a
   telemetry feed is available. Nodes apply warm-stream commands for their
   region/node id and mark themselves draining on matching close-node commands.

Next steps are to harden the bootstrap image path and retire the remaining
contributor-ingest smoke paths from `av-mesh` once `av-contrib` owns those
fixtures end to end.
