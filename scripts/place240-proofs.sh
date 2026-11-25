#!/usr/bin/env bash
# Place240's in-process proofs at 247 Mbit/s (src/tests/place_heavy.rs),
# runnable as one gate step on a quiet machine.
#
# The test process starts on NODE_CPUS (taskset): the edges, the entry and
# the viewers (the test's tokio runtime, the edges' class threads) run
# there; the source side (the 4K publisher's makers and sender, the audio
# publisher, the proxies) moves itself to SOURCE_CPUS (PH_SOURCE_CPUS). Both sets must be idle: the 247 Mbit/s loopback drops
# datagrams on a busy box and viewers then move for missing parts.
#
# Needs 16 CPUs by default: NODE_CPUS 16-23 (8) and SOURCE_CPUS 24-31 (8).
# Usage: scripts/place240-proofs.sh [cargo-wrapper]
#   NODE_CPUS=16-23 SOURCE_CPUS=24-31 PH_MBPS=247 scripts/place240-proofs.sh
# The cargo wrapper (default `cargo`; the lab uses rmcargo) builds the test
# binary first; the run itself is not wrapped.
set -euo pipefail
cd "$(dirname "$0")/.."
CARGO="${1:-cargo}"
NODE_CPUS="${NODE_CPUS:-16-23}"
SOURCE_CPUS="${SOURCE_CPUS:-24-31}"
export PH_MBPS="${PH_MBPS:-247}"
export PH_SOURCE_CPUS="$SOURCE_CPUS"

binary=$("$CARGO" test --release --bin av-mesh --no-run --message-format=json 2>/dev/null \
  | sed -n 's/.*"executable":"\([^"]*av_mesh-[^"]*\)".*/\1/p' | tail -1)
if [ -z "$binary" ] || [ ! -x "$binary" ]; then
  echo "place240-proofs: no test binary built" >&2
  exit 2
fi
echo "place240-proofs: $binary on CPUs $NODE_CPUS (node) + $SOURCE_CPUS (source), $PH_MBPS Mbit/s" >&2
status=0
for test in \
  tests::place_heavy::heavy_viewers_spread_and_move_off_a_stalled_edge \
  tests::place_heavy::an_upstream_stall_moves_each_viewer_at_most_once \
  tests::place_heavy::at_budget_the_next_heavy_session_is_refused_and_audio_is_untouched; do
  echo "== $test" >&2
  taskset -c "$NODE_CPUS" "$binary" "$test" --exact --ignored --nocapture \
    --test-threads 1 || status=1
done
exit $status
