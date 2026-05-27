# av-mesh

[![CI](https://github.com/wavey-ai/av-mesh/actions/workflows/ci.yml/badge.svg)](https://github.com/wavey-ai/av-mesh/actions/workflows/ci.yml)

`av-mesh` is the local prototype for a two-region audio/video mesh. A contributor
can publish media into one region, cache slots replicate to other regions over
UDP with RaptorQ FEC and an optional RIST backhaul, and users can read the
replicated stream as HLS-style MPEG-TS parts from any region.

The first implementation keeps the control surface intentionally small:

- `playlists::mesh` provides UDP-FEC cache discovery and slot replication.
- `web-service` serves HTTPS HLS playlists, parts, segments, and health checks.
- `message-packetizer` packetizes mesh control events and RIST mesh cache-slot
  frames into RIST/SRT-sized datagrams.
- Contributor ingest supports pure-Rust RIST, UDP-FEC, local UDP MPEG-TS
  datagrams, and streamed HTTP `POST`/`PUT /ingest` uploads in this prototype.

## Local two-region prototype

Run the UK node:

```bash
cargo run -- \
  --region uk \
  --node-id uk-1 \
  --mesh-bind 127.0.0.1:9101 \
  --peer 127.0.0.1:9201 \
  --http-port 9444 \
  --ingest-bind 127.0.0.1:10001 \
  --fec-bind 127.0.0.1:12001 \
  --rist-bind 127.0.0.1:7000 \
  --rist-mesh-bind 127.0.0.1:7100 \
  --rist-mesh-peer 127.0.0.1:7101
```

Run the US node:

```bash
cargo run -- \
  --region us \
  --node-id us-1 \
  --mesh-bind 127.0.0.1:9201 \
  --peer 127.0.0.1:9101 \
  --http-port 9445 \
  --ingest-bind 127.0.0.1:10002 \
  --fec-bind 127.0.0.1:12002 \
  --rist-bind 127.0.0.1:7001 \
  --rist-mesh-bind 127.0.0.1:7101 \
  --rist-mesh-peer 127.0.0.1:7100
```

Publish a local MPEG-TS stream into the UK node over plain UDP:

```bash
ffmpeg -re -f lavfi -i testsrc=size=1280x720:rate=30 \
  -f lavfi -i sine=frequency=1000:sample_rate=48000 \
  -c:v libx264 -preset veryfast -tune zerolatency \
  -c:a aac -f mpegts udp://127.0.0.1:10001?pkt_size=1316
```

For stdin-based tooling, the prototype also includes a raw UDP sender:

```bash
ffmpeg -re -f lavfi -i testsrc=size=1280x720:rate=30 \
  -f lavfi -i sine=frequency=1000:sample_rate=48000 \
  -c:v libx264 -preset veryfast -tune zerolatency \
  -c:a aac -f mpegts - | \
  cargo run --bin udp-send -- 127.0.0.1:10001
```

Or publish MPEG-TS bytes over UDP-FEC:

```bash
ffmpeg -re -f lavfi -i testsrc=size=1280x720:rate=30 \
  -f lavfi -i sine=frequency=1000:sample_rate=48000 \
  -c:v libx264 -preset veryfast -tune zerolatency \
  -c:a aac -f mpegts - | \
  cargo run --bin udp-fec-send -- 127.0.0.1:12001
```

Or publish over RIST with a RIST-capable sender such as OBS:

- URL: `rist://127.0.0.1:7000`
- Profile: `main`
- Flow ID: `0x72737401`

Or with the included stdin RIST sender:

```bash
ffmpeg -re -f lavfi -i testsrc=size=1280x720:rate=30 \
  -f lavfi -i sine=frequency=1000:sample_rate=48000 \
  -c:v libx264 -preset veryfast -tune zerolatency \
  -c:a aac -f mpegts - | \
  cargo run --bin rist-send -- 127.0.0.1:7000
```

Or publish MPEG-TS bytes over HTTP:

```bash
ffmpeg -re -f lavfi -i testsrc=size=1280x720:rate=30 \
  -f lavfi -i sine=frequency=1000:sample_rate=48000 \
  -c:v libx264 -preset veryfast -tune zerolatency \
  -c:a aac -f mpegts - | \
  curl -k -X POST --data-binary @- https://127.0.0.1:9444/ingest
```

Then read either region:

- UK playlist: `https://127.0.0.1:9444/live/stream.m3u8`
- US playlist: `https://127.0.0.1:9445/live/stream.m3u8`
- Health: `https://127.0.0.1:9444/up`
- Stats: `https://127.0.0.1:9444/api/stats`

The server uses the local TLS material from `web-services`; clients will need to
trust that cert or use an insecure local test client.

For a quick automated check, run:

```bash
scripts/two-region-smoke.sh
```

The smoke script builds the binaries, starts UK and US nodes on local high
ports with UDP-FEC and RIST mesh backhauls configured, sends distinct HTTP, raw
UDP, UDP-FEC, and RIST ingest payloads into UK, and verifies those exact HLS
parts can be read from both UK and US by several concurrent clients.

## Current scope

This is not the final multi-protocol edge deployment yet. The current milestone
proves the shared-cache behavior needed by the requested mesh:

1. A node can discover configured peers with UDP-FEC `HELLO` frames.
2. A node can ingest MPEG-TS from RIST, UDP-FEC, local UDP, or streamed HTTP and
   write media parts to a `playlists::ChunkCache`.
3. Peers replicate those slots over UDP-FEC and optional RIST mesh backhaul, then
   serve them as HLS parts.
4. Region identity is explicit, starting with `uk` and `us`.

Next steps are to broaden contributor protocols behind the existing
`web-services` proxy/ingest layer and add production deployment topology.
