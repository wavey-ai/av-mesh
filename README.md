# av-mesh

[![CI](https://github.com/wavey-ai/av-mesh/actions/workflows/ci.yml/badge.svg)](https://github.com/wavey-ai/av-mesh/actions/workflows/ci.yml)

`av-mesh` is the local prototype for a demand-driven audio/video mesh. A
contributor can publish media into one region. The service stores all media in
playlist and cache streams. Cache slots replicate to other regions through the
shared Wavey RaptorQ-FEC datagram protocol. Users can read replicated LL-HLS
artifacts or other stream-addressed slots from any region.

RaptorQ-FEC is the mesh transport. Cache synchronization inside the mesh moves
opaque stream bytes over the Wavey RaptorQ datagram protocol.
Contributor-facing RIST, SRT, and RTMP input belongs in `../av-contrib`. That
service terminates the protocols and packages OBS-style media as fMP4/CMAF
LL-HLS artifacts. It publishes only stream-addressed artifact bytes into the
mesh FEC socket.

Transport reliability stance: RaptorQ-FEC is the right live mesh hot path for
bounded packet loss because it can repair without waiting for RTT. It is not a
general replacement for RIST/SRT ARQ on arbitrary bad WAN paths. If loss exceeds
the configured repair budget, FEC fails closed. RIST/SRT can still recover later
by retransmitting from history if the latency budget allows. The mesh should
therefore stay slot and artifact native for live RaptorQ propagation. It needs
an explicit missing-slot repair and backfill path before it can provide
RIST-class eventual reliability.

This path can use TCP, QUIC, or a RIST-like ARQ
channel.

The first implementation keeps the mesh transport intentionally small:

- `playlists::mesh` provides UDP-FEC cache discovery and slot replication using
  the reusable `raptorq-datagram-fec` crate. Seed peers and private-subnet
  broadcast discovery only bootstrap the peer set. Normal mesh `HELLO` frames
  gossip known peer addresses after that.
- Optional `private-subnet-discovery` support can add mesh peers discovered on a
  10.x private subnet by the existing `discovery` crate's VLAN broadcast path.
  `linode-private-discovery` is an alias for the same generic feature. Use this
  alias for Linode VLAN deployments that the sibling `linode` project provides.
- Playlist reads and warm-stream controls send mesh replica requests, so a node
  with local demand can ask peers for a stream immediately.
- Replica requests begin at the earliest missing retained slot for the requested
  stream. This behavior gives live playlist, tail, and media requests a
  retained-window backfill path. It does not change the FEC datagram wire format.
- LL-HLS playlist rendering is cached by stream and `ChunkCache` content
  version. Slot writes and stream reuse invalidate the derived manifest.
  Unchanged requests do not repeatedly take each retained-slot read lock.
- `web-service` serves the operator/UI edge over HTTPS/TCP: HLS playlists,
  parts, segments, health checks, JSON mesh snapshots, server-sent mesh events,
  and HTTP `POST /api/control/*` commands. This is not the mesh transport.
- Mesh control commands are sent node-to-node over TCP changes as `AVMC` frames.
  A local HTTP/API command records the intent. It can apply the command locally.
  It publishes an `AVMC` envelope to telemetry peers for region or node targeting.
- Optional raw TCP accepts the same snapshot/control and serialized media
  access-unit bytes in length-prefixed frames. Completed access units are cached
  as stream slots.
- WebSocket and WebTransport handlers remain as explicit edge paths only. They
  are disabled by default. Use `--edge-websocket` or
  `--edge-webtransport` when a client specifically needs them.
- `message-packetizer` remains available for bounded UDP-style announcements
  operator commands use `AVMC` over TCP changes.
- Contributor ingest through `../av-contrib` supports arbitrary non-OBS byte
  streams via `POST`/`PUT /ingest?stream_id=...`, pure-Rust RIST MPEG-TS, SRT
  MPEG-TS, RTMP/FLV, and HTTP `POST`/`PUT /media/access-unit` non-TS media
  access units. The service boxes OBS-style inputs into fMP4/CMAF before they
  enter the mesh. Raw RIST, SRT, RTMP, and MPEG-TS payloads stay outside the mesh
  boundary.
- The sibling `../av-contrib` project is the contributor-facing repo boundary.
  It owns non-TS media access-unit query parsing. It uses
  `raptorq-datagram-fec` to decode serialized access units. These edge formats
  are no longer defined inside the mesh binary.
- `/mesh` serves the Needletail operations dashboard supplied by the product
  supervisor. The dashboard presents service and feed health, compiled delivery
  programs, and independent source and repair lanes. It also presents RaptorQ
  recovery, deadline health, publication continuity, and realtime latency.
- Optional `--telemetry-bind` publishes JSON mesh snapshots to a TCP changes feed
  with the `AVMT` tag for central aggregation without scraping stdout. Snapshots
  include the node's mesh socket address so `/api/mesh` can resolve peer
  addresses back to node ids for the topology graph when both sides report.
  The service removes remote snapshots older than `--telemetry-stale-ms`. These
  snapshots do not affect capacity, topology, replica plans, or regional controls.
- Optional `--telemetry-peer` connects a central node to other nodes' TCP changes
  feeds and merges their `AVMT` snapshots into `/api/mesh` and `/mesh`. The same
  feed also carries `AVMC` control commands. Region-scoped commands include a
  target node ID when telemetry identifies one. Subscribers then run matching
  warm or close requests on specific nodes instead of using only region filters.

- Optional `--telemetry-fec-target` sends the node snapshot to a central
  `--telemetry-fec-bind` UDP collector through the shared
  `raptorq-datagram-fec` codec. The collector inserts decoded snapshots into the
  same bounded aggregator used by `/api/mesh`. Browsers never connect to nodes
  individually. This lane is intended for controlled private networking. Keep
  the TLS/TCP changes path for authenticated control commands.
- FEC telemetry defaults to one snapshot every 5 seconds and a total 32 Kbit/s
  send budget. Payloads use named MessagePack inside a versioned 32 KiB envelope
  with a CRC, boot ID, and monotonic sequence. The sender retains at most two
  snapshots or 64 KiB. Under load, it replaces the oldest snapshot and sends
  source symbols before repair symbols. It skips remaining repair symbols when
  a newer snapshot waits. It does not emit per-datagram logs or write telemetry
  to a database.
- `/api/mesh` and `/metrics` expose bounded FEC queue, send, receive, decode,
  duplicate, and error counters under `orchestration.telemetry_fec` and
  `av_mesh_telemetry_fec_*`.
- Optional `--provision-command` lets the UI/API `provision_node` control hand
  off to an operator-provided shell command. The command receives
  `AV_MESH_PROVISION_NODE_ID`, `AV_MESH_PROVISION_REGION`,
  `AV_MESH_LOCAL_NODE_ID`, `AV_MESH_LOCAL_REGION`, and `AV_MESH_CONTROL_ID`.
- With `--features linode-provisioner`, `--linode-provision` uses the sibling
  `../linode` crate to create a node, attach it to the region VLAN with a
  `10.0.0.x/24` private address, reboot it, and update DNS. Use
  `--linode-region-map mesh-region=linode-region` for mesh names such as
  `uk=gb-lon`. `linode-private-discovery` enables this provisioner together
  with private-subnet discovery.
- `av_mesh::replication` contains the tested replica planner for baseline
  continent/region staging, demand-triggered local replicas, node storage
  capacity, and anti-affinity for nearby nodes. Telemetry includes all active
  cache stream IDs. Baseline staging can pull non-default media and access-unit
  streams and the default playlist stream. `/api/mesh` exposes planned replicas
  for these stream IDs to the control API.
- `/api/mesh` exposes stream ids as browser-safe decimal strings in
  `stream_id_text` fields wherever a numeric `stream_id` appears, including the
  local stream stats, active stream telemetry, planned replicas, and recent
  control commands. HTTP control requests accept `stream_id` as either a JSON
  number or a decimal string. Browser clients should send strings for Snowflake
  ids.
- `/api/mesh` also exposes `edge_services` with each node's advertised
  `playback_base_url`, active readers, served requests and bytes, and LL-HLS tail
  polls. It includes cumulative LL-HLS handler latency histograms and p95
  latency. Operators can compare origin freshness with edge response time.
  Player-facing services such as `av-llhls` should use seed nodes only to
  discover candidates. They should score candidates locally and avoid routing
  all playback through a central service.
- `/api/mesh` exposes the active cache-mesh transport policy: replication scan
  interval, FEC symbol size, minimum repair symbols, proportional repair ratio,
  and maximum repair symbols. The default policy has a one-symbol floor and 3%
  proportional repair. It avoids a fixed high redundancy cost for small parts.
  It also protects larger parts from loss of multiple packets.
- `/api/mesh` also exposes `mesh_fec` runtime outcomes. These outcomes include
  source and repair datagrams, protected and wire bytes, and decoded objects.
  They also include repair recovery, late sources, incomplete or expired
  objects, and codec errors. RelaySession ingress reports repair-assisted
  decodes and exact FEC-recovered objects separately. It also reports the number
  of missing source symbols that RaptorQ reconstructed. Warm relays retain at
  most four objects, 2,048 datagrams, and 4 MiB for each child. Promotion replays
  only source datagrams with valid object deadlines.
- `/metrics` exposes Prometheus metrics for topology, telemetry freshness, node
  capacity, edge traffic, errors, stream lag, alerts, and transport settings. It
  also exposes cache-mesh FEC outcomes and per-node LL-HLS latency histograms.
  Scrapes use the same bounded telemetry snapshot as `/api/mesh`.

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

For private-subnet discovery on Linode VLANs or any 10.x subnet, enable the
feature. Bind mesh UDP to an address that the subnet can reach:

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

`scripts/bootstrap-linode-node.sh` waits for SSH and syncs the local Wavey
workspace required by the path dependencies. It builds `av-mesh` on the node
with private-subnet discovery and installs a systemd service. It reuses or
generates shared local TLS material for TCP changes telemetry. Run it with
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

To qualify the FEC snapshot lane locally, make the UK process the collector by
adding `--telemetry-fec-bind 127.0.0.1:7350`, then add this to the US process:

```bash
--telemetry-fec-target 127.0.0.1:7350
```

The FEC lane and TCP changes lane can run together during rollout. Snapshot
collection and serialization remain on one 5-second producer. A full TCP queue
or UDP socket drops telemetry work instead of applying backpressure. Add
`--telemetry-snapshots-fec-only` on a sending node after comparison. This stops
TCP snapshot publication but leaves the TLS/TCP channel available for `AVMC`
control commands.

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
contributor wraps the access unit for the media UDP-FEC socket. The mesh stores
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
- Edge WebTransport: opt in with `--edge-webtransport`. Media datagrams can be
  raw RQD2 or `raptorq-fec-transport` stream-prefixed RQD2.
- Mesh/control raw TCP: opt in with `--raw-tcp-port <port>`. Each request and
  response is framed as `[u32_be length][payload]`. Payloads can be JSON mesh
  protocol requests or serialized media access units. Add `--raw-tcp-tls` to
  wrap the raw TCP listener with the same TLS material.
- Control messages: `POST /api/control/provision-node`,
  `POST /api/control/close-node`, and `POST /api/control/warm-stream` originate
  commands locally. Nodes with a telemetry publisher forward those commands as
  `AVMC` frames over TCP changes. Telemetry peers ingest matching `AVMC` frames
  and execute them by region or node id.

For `av-llhls`, pass the selected node's `playback_base_url` as `baseUrl` and
the decimal-string playlist/stream id as `streamId`. The blocking tail route is
`/live/<stream_id>/tail?after=<sequence>`. It waits on the exact next cache
commit without a polling sleep. A bounded wait can return `204` when the stream
has not arrived. That request also creates mesh demand.

The cache and replication unit remains `--part-ms` (or
`AV_LL_HLS_PART_MS`). Set `--response-ms` or `AV_LL_HLS_RESPONSE_MS` to make
the tail combine consecutive units by default. For example, 5 ms cache units
and `AV_LL_HLS_RESPONSE_MS=200` return 40 ordered units per blocking response.
Controlled clients can override the service default with `parts=<count>` for
an A/B test. Counts are bounded to 200 and to the configured retained cache
capacity. Startup rejects a service default that cannot fit.

The response includes
`x-sequence-start`, `x-sequence-end`, `x-part-count`,
`x-part-duration-ms`, and `x-response-duration-ms`. `x-sequence` is the final
sequence and is the cursor for the next request. The body is the byte-exact
concatenation of the cached units, so the selected bytestream must be
self-delimiting when more than one unit is returned.

Synchronized clients can instead request
`/live/tail-bundle?streams=1,2,3,4,5,6,7,8&from=<sequence>&parts=1`.
The bounded `NTB1` response carries the exact requested sequence range for
every stream or returns no partial bundle. The implementation resolves one
cache generation for each range and uses exact stream and sequence waiters. It
serves prevalidated `Bytes` slices from an indexed canonical slot. It retains
the complete canonical envelope for conflict and replication checks.

Per-track
waits run sequentially under one shared absolute deadline. Every registration
rechecks the cache, so arbitrary track arrival order cannot create a lost
wakeup or extend the request budget.

RelaySession recovery now carries the exact FEC-reconstructed envelope beside
the already bounded, payload-hash-verified `MediaObject`. The cache can commit
that pair directly instead of encoding it twice and decoding/hashing it again.
This does not bypass canonical parsing, announcement-key rebinding, payload
integrity, replay checks, or immutable cache conflict checks. A playback leaf
with no downstream relay or audio subscriber also discards AEP1 traffic after
the magic check. Forwarding relays and active subscribers retain full
validation.

The matched private-GCP profile and its strict non-pass are recorded in
[`Needletail's 19 July tail-bundle report`](../needletail/docs/real-world-tests/2026-07-19-opus-h3-tail-bundle.md).
The latest retained run delivered 2,304,000/2,304,000 parts and reduced edge
host CPU to 34.765%. However, 9 of 288,000 bundles exceeded 20 ms. Treat this
result as an optimization result, not an endurance or production-sizing claim.

The server uses the local TLS material from `av-service`. Clients will need to
trust that cert or use an insecure local test client.

## Realtime performance gate

With a local or deployed contributor-plus-mesh stack already running, collect
repeatable client and service latency measurements with:

```sh
make realtime-benchmark
```

The benchmark sends raw contributor payloads and reads each configured mesh
playlist at configurable concurrency. It prefers persistent HTTP/2 sessions
through `h2load`. It uses parallel curl processes as a fallback. It reports
client p50, p95, p99, mean, maximum, and effective request rate. Duration mode
(`DURATION_SECONDS`) sustains load instead of stopping at a request count.

`PROPAGATION_PROBES` posts unique canaries. It measures when each edge can fetch
the exact bytes. Before-and-after Prometheus snapshots give service histogram
count and p95 changes. Contributor results attribute interval p95 to bounded
`encode_wait`, `encode`, `send`, and `telemetry` stages. The terminal and
`RESULT_JSON` show these results, which identify the hot path of a forwarding
regression. `RESULT_JSON` stores machine-readable evidence.

Override topology and load through `CONTRIB_URL`, comma-separated `MESH_URLS`,
`CONCURRENCY`, `H2_STREAMS_PER_CLIENT`, `PAYLOAD_BYTES`, and `LOAD_CLIENT`. Set
`PARALLEL_ENDPOINTS=1` to load the contributor and each edge at the same time.
`CONTRIB_METRICS_URL` and `MESH_METRICS_URLS` can select private monitoring
endpoints for service histogram scrapes. Client load can continue to use public
origins.

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

This runs baseline and impaired phases against the same release stack. The mesh
profile applies 35±5 ms one-way delay with 1% loss. The contributor-FEC profile
applies 10±2 ms delay with 1% loss. Both profiles use the unprivileged
`udp-netem` binary.

The test verifies that both links carried and dropped packets. It rejects
emulator overflow and send errors. It checks client and service p95 budgets. The
impaired phase must recover source symbols without FEC decode errors. The test
writes qualification artifacts under `target/realtime-qualification/`.

Default gates are 15 ms contributor ingest p95 and 5 ms playlist p95. They also
include 1 ms edge-handler p95 and 200 ms propagation p95. The impaired-to-baseline
p95 ratio must not exceed 3. You can override all profile and latency budgets
with environment variables.

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
gates. It fails on any bad round or counter reset. It also fails on new pipeline
errors, MPEG-TS continuity errors, or expired FEC objects. An explicit limit can
override the corresponding failure.

Each run writes raw metric snapshots, per-round
logs/JSON, counter deltas, cross-round percentiles, and `soak.json` under
`target/realtime-soak/`. Use a dedicated canary stream and explicitly scoped
hosts. The script never deploys or changes servers.

## Persistent observability

`observability/` contains a runnable Prometheus, Alertmanager, and Grafana
bundle. It stores the metrics that `av-contrib` and `av-mesh` expose. It records
forwarding-stage and mesh-handler p95 values. It charts FEC recovery and wire
overhead. It evaluates bounded-label alerts and gives diagnosis guidance.

```sh
make observability-check
docker compose -f observability/compose.yml up -d
```

Grafana is served at `http://127.0.0.1:3000/d/wavey-realtime`, Prometheus at
`http://127.0.0.1:9090`, and Alertmanager at `http://127.0.0.1:9093`. The local
scrape configuration trusts the development certificates insecurely. Deployed
scrapers must use the deployment CA. Alert thresholds are labeled
`slo: provisional` until a hardware-, geography-, bitrate-, and load-qualified
regional soak establishes production SLOs. See `observability/README.md` for
deployment and notification-routing notes.

## Needletail Operations

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
Needletail Operations. It passes the product assets to each playback edge with
`NEEDLETAIL_MISSION_CONTROL_DIST`. It uses the local bitneedle TLS material from
`../tls/local.bitneedle.com`. It starts UK and US mesh nodes and one `av-contrib`
ingress. It prefixes each child process output line with its source.

By default, it uses stream ID `1`, UK egress
`https://local.bitneedle.com:19444/live/1/stream.m3u8`, US egress
`https://local.bitneedle.com:19445/live/1/stream.m3u8`, and Operations at
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
must resolve to loopback. On this machine `local.bitneedle.com` resolves to
`127.0.0.1` and `::1`.

Use `--mission-control-dist /path/to/dist` to reuse specific product assets.
Use `--no-mission-control-build` with an existing build. `/mesh` returns concise
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

The smoke script builds `deploy/k3d/Dockerfile` and imports `av-mesh:local` into
the cluster. It generates a short-lived TLS secret and applies
`deploy/k3d/av-mesh.yaml`. It waits for both deployments. It then checks `/up`
and `/api/mesh` through local port-forwards.

Useful follow-up commands:

```bash
make k3d-check
kubectl -n av-mesh get pods,svc
make k3d-down
```

The services already use `tracing_subscriber` with `RUST_LOG` env filters. The
current detailed logs are mostly `info!` and `debug!`. Setting `trace` is
accepted, but only code paths with `trace!` calls will emit extra trace-level
events. The next refactor should provide a same-process Tokio harness. Expose
library `run(config, shutdown)` entry points from `av-mesh` and `av-contrib`.
Then, make this supervisor call those tasks directly instead of supervising
child binaries.

For a quick automated check, run:

```bash
scripts/two-region-smoke.sh
```

The smoke script builds the binaries and generates short-lived local TLS
material for `tcp-changes`. It starts UK and US nodes and a UK `av-contrib`
frontend on local high ports. It configures the RaptorQ cache mesh. It sends
distinct HTTP, UDP-FEC, and RIST contributor payloads into the UK node. Several
concurrent clients then read the exact HLS parts from both nodes. Finally, the
script sends a UK warm-stream command to the US node through the `AVMT`/`AVMC`
TCP changes path.

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
7. The telemetry/UI path exposes local snapshots through HTTPS and can publish
   them through either the compatibility TCP changes feed or the bounded
   RaptorQ UDP lane. Both feed one central aggregate
   node/connection/capacity/edge-service view.
8. The runtime planner exposes planned replicas through `/api/mesh` and asks for
   the stream when the local node is selected as a baseline replica.
9. UI/API/raw TCP controls publish `AVMC` command frames over TCP changes when a
   telemetry feed is available. Nodes apply warm-stream commands for their
   region/node id and mark themselves draining on matching close-node commands.

Next, harden the bootstrap image path. Remove the remaining contributor-ingest
smoke paths from `av-mesh` after `av-contrib` owns those fixtures end to end.
