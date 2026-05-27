#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${ROOT}/target/debug/av-mesh"
TMPDIR="$(mktemp -d "${TMPDIR:-/tmp}/av-mesh-smoke.XXXXXX")"

UK_MESH="${UK_MESH:-127.0.0.1:19101}"
US_MESH="${US_MESH:-127.0.0.1:19201}"
UK_HTTP="${UK_HTTP:-19444}"
US_HTTP="${US_HTTP:-19445}"
UK_UDP="${UK_UDP:-127.0.0.1:11001}"
US_UDP="${US_UDP:-127.0.0.1:11002}"
UK_RIST="${UK_RIST:-127.0.0.1:17000}"
US_RIST="${US_RIST:-127.0.0.1:17001}"

UK_PID=""
US_PID=""

cleanup() {
  if [[ -n "${UK_PID}" ]]; then
    kill "${UK_PID}" 2>/dev/null || true
  fi
  if [[ -n "${US_PID}" ]]; then
    kill "${US_PID}" 2>/dev/null || true
  fi
  wait "${UK_PID}" 2>/dev/null || true
  wait "${US_PID}" 2>/dev/null || true
  rm -rf "${TMPDIR}"
}
trap cleanup EXIT

wait_for_health() {
  local port="$1"
  local name="$2"
  for _ in $(seq 1 80); do
    if curl -skfs "https://127.0.0.1:${port}/up" >/dev/null; then
      return 0
    fi
    sleep 0.1
  done

  echo "${name} did not become healthy" >&2
  echo "--- ${name} log ---" >&2
  sed -n '1,200p' "${TMPDIR}/${name}.log" >&2 || true
  return 1
}

wait_for_playlist() {
  local port="$1"
  local name="$2"
  for _ in $(seq 1 100); do
    if curl -skfs "https://127.0.0.1:${port}/live/stream.m3u8" | tee "${TMPDIR}/${name}.m3u8" | grep -q 'part0.ts'; then
      return 0
    fi
    sleep 0.1
  done

  echo "${name} playlist did not expose part0.ts" >&2
  echo "--- ${name} playlist ---" >&2
  cat "${TMPDIR}/${name}.m3u8" >&2 || true
  echo "--- uk log ---" >&2
  sed -n '1,200p' "${TMPDIR}/uk.log" >&2 || true
  echo "--- us log ---" >&2
  sed -n '1,200p' "${TMPDIR}/us.log" >&2 || true
  return 1
}

part_size() {
  local port="$1"
  curl -skfs "https://127.0.0.1:${port}/live/part0.ts" | wc -c | tr -d '[:space:]'
}

cd "${ROOT}"
cargo build --locked

RUST_LOG="${RUST_LOG:-av_mesh=info,playlists=info,web_service=info}" \
  "${BIN}" \
  --region uk \
  --node-id uk-smoke \
  --mesh-bind "${UK_MESH}" \
  --peer "${US_MESH}" \
  --http-port "${UK_HTTP}" \
  --ingest-bind "${UK_UDP}" \
  --rist-bind "${UK_RIST}" \
  --part-ms 100 \
  --parts-per-segment 2 \
  --window-parts 8 \
  --slot-kb 64 \
  >"${TMPDIR}/uk.log" 2>&1 &
UK_PID="$!"

RUST_LOG="${RUST_LOG:-av_mesh=info,playlists=info,web_service=info}" \
  "${BIN}" \
  --region us \
  --node-id us-smoke \
  --mesh-bind "${US_MESH}" \
  --peer "${UK_MESH}" \
  --http-port "${US_HTTP}" \
  --ingest-bind "${US_UDP}" \
  --rist-bind "${US_RIST}" \
  --part-ms 100 \
  --parts-per-segment 2 \
  --window-parts 8 \
  --slot-kb 64 \
  >"${TMPDIR}/us.log" 2>&1 &
US_PID="$!"

wait_for_health "${UK_HTTP}" uk
wait_for_health "${US_HTTP}" us

printf 'AVMESH-SMOKE-PART-0001' \
  | curl -skfs -X POST --data-binary @- "https://127.0.0.1:${UK_HTTP}/ingest" >/dev/null

wait_for_playlist "${UK_HTTP}" uk
wait_for_playlist "${US_HTTP}" us

UK_PART_SIZE="$(part_size "${UK_HTTP}")"
US_PART_SIZE="$(part_size "${US_HTTP}")"

if [[ "${UK_PART_SIZE}" -le 0 || "${US_PART_SIZE}" -le 0 ]]; then
  echo "expected non-empty HLS parts; uk=${UK_PART_SIZE} us=${US_PART_SIZE}" >&2
  exit 1
fi

echo "two-region smoke passed: uk_part=${UK_PART_SIZE} bytes us_part=${US_PART_SIZE} bytes"
