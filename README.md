# av-mesh

`av-mesh` is the local prototype for a two-region audio/video mesh. A contributor
can publish media into one region, cache slots replicate to other regions over
UDP with RaptorQ FEC, and users can read the replicated stream as HLS-style
MPEG-TS parts from any region.

The first implementation keeps the control surface intentionally small:

- `playlists::mesh` provides UDP-FEC cache discovery and slot replication.
- `web-service` serves HTTPS HLS playlists, parts, segments, and health checks.
- `message-packetizer` packetizes mesh control events so the control plane can
  later move over RIST/SRT-sized datagrams with signing enabled.
- Contributor ingest is a local UDP MPEG-TS datagram endpoint in this prototype.
  RIST and the existing `web-services/upload-response` UDP-FEC ingest path are
  the intended production protocol frontends.

## Local two-region prototype

Run the UK node:

```bash
cargo run -- \
  --region uk \
  --node-id uk-1 \
  --mesh-bind 127.0.0.1:9101 \
  --peer 127.0.0.1:9201 \
  --http-port 9444 \
  --ingest-bind 127.0.0.1:10001
```

Run the US node:

```bash
cargo run -- \
  --region us \
  --node-id us-1 \
  --mesh-bind 127.0.0.1:9201 \
  --peer 127.0.0.1:9101 \
  --http-port 9445 \
  --ingest-bind 127.0.0.1:10002
```

Publish a local MPEG-TS stream into the UK node:

```bash
ffmpeg -re -f lavfi -i testsrc=size=1280x720:rate=30 \
  -f lavfi -i sine=frequency=1000:sample_rate=48000 \
  -c:v libx264 -preset veryfast -tune zerolatency \
  -c:a aac -f mpegts udp://127.0.0.1:10001?pkt_size=1316
```

Then read either region:

- UK playlist: `https://127.0.0.1:9444/live/stream.m3u8`
- US playlist: `https://127.0.0.1:9445/live/stream.m3u8`
- Health: `https://127.0.0.1:9444/up`
- Stats: `https://127.0.0.1:9444/api/stats`

The server uses the local TLS material from `web-services`; clients will need to
trust that cert or use an insecure local test client.

## Current scope

This is not the final multi-protocol edge deployment yet. The current milestone
proves the shared-cache behavior needed by the requested mesh:

1. A node can discover configured peers with UDP-FEC `HELLO` frames.
2. A node can write media parts to a `playlists::ChunkCache`.
3. Peers replicate those slots over UDP-FEC and serve them as HLS parts.
4. Region identity is explicit, starting with `uk` and `us`.

Next steps are to wire RIST ingest from `web-services/examples/obs-rist-llhls`
or `upload-response::PureRistIngest` into the same `LiveTsCache`, then broaden
contributor protocols behind the existing `web-services` proxy/ingest layer.

