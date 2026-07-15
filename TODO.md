# TODO

## Needletail integration

- Keep `/api/mesh` RelaySession ingress snapshots bounded and low-cardinality.
- Add controller-compiled delivery class, generation, parent identity, carrier,
  trust, route state, and path-stretch fields as desired state reaches each
  playback edge.
- Add contiguous object watermark and known-gap counters for Mission Control.
- Export edge p50/p95/p99 latency directly while retaining cumulative histogram
  buckets for independent verification.
- Carry selected streams exclusively through the dual-parent RelaySession fabric
  after the local impairment gate demonstrates source-symbol recovery through an
  independent warm secondary.

Mission Control source, build, and UI tests live in
`../needletail/mission-control`.
