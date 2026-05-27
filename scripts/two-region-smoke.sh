#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${ROOT}/target/debug/av-mesh"
UDP_BIN="${ROOT}/target/debug/udp-send"
UDP_FEC_BIN="${ROOT}/target/debug/udp-fec-send"
RIST_BIN="${ROOT}/target/debug/rist-send"
TMPDIR="$(mktemp -d "${TMPDIR:-/tmp}/av-mesh-smoke.XXXXXX")"

UK_MESH="${UK_MESH:-127.0.0.1:19101}"
US_MESH="${US_MESH:-127.0.0.1:19201}"
UK_HTTP="${UK_HTTP:-19444}"
US_HTTP="${US_HTTP:-19445}"
UK_UDP="${UK_UDP:-127.0.0.1:11001}"
US_UDP="${US_UDP:-127.0.0.1:11002}"
UK_FEC="${UK_FEC:-127.0.0.1:12001}"
US_FEC="${US_FEC:-127.0.0.1:12002}"
UK_RIST="${UK_RIST:-127.0.0.1:17000}"
US_RIST="${US_RIST:-127.0.0.1:17001}"
UK_RIST_MESH="${UK_RIST_MESH:-127.0.0.1:17100}"
US_RIST_MESH="${US_RIST_MESH:-127.0.0.1:17101}"
SMOKE_USERS="${SMOKE_USERS:-8}"

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

wait_for_part() {
  local port="$1"
  local name="$2"
  local seq="$3"
  local expected="$4"
  local part_file="${TMPDIR}/${name}-part${seq}.ts"
  local expected_file="${TMPDIR}/expected-part${seq}.ts"

  printf '%s' "${expected}" >"${expected_file}"
  for _ in $(seq 1 120); do
    if curl -skfs "https://127.0.0.1:${port}/live/part${seq}.ts" >"${part_file}"; then
      if cmp -s "${expected_file}" "${part_file}"; then
        return 0
      fi
    fi
    sleep 0.1
  done

  echo "${name} part${seq}.ts did not match expected payload" >&2
  echo "--- ${name} part${seq}.ts ---" >&2
  cat "${part_file}" >&2 || true
  echo >&2
  echo "--- ${name} playlist ---" >&2
  curl -skfs "https://127.0.0.1:${port}/live/stream.m3u8" >&2 || true
  echo "--- uk log ---" >&2
  sed -n '1,200p' "${TMPDIR}/uk.log" >&2 || true
  echo "--- us log ---" >&2
  sed -n '1,200p' "${TMPDIR}/us.log" >&2 || true
  return 1
}

publish_part() {
  local protocol="$1"
  local seq="$2"
  local payload="$3"

  case "${protocol}" in
    http)
      printf '%s' "${payload}" \
        | curl -skfs -X POST --data-binary @- "https://127.0.0.1:${UK_HTTP}/ingest" >/dev/null
      ;;
    udp)
      printf '%s' "${payload}" \
        | "${UDP_BIN}" "${UK_UDP}" >/dev/null
      ;;
    udp-fec)
      printf '%s' "${payload}" \
        | "${UDP_FEC_BIN}" "${UK_FEC}" >/dev/null
      ;;
    rist)
      printf '%s' "${payload}" \
        | "${RIST_BIN}" "${UK_RIST}" >/dev/null
      ;;
    *)
      echo "unknown publish protocol: ${protocol}" >&2
      return 1
      ;;
  esac

  wait_for_part "${UK_HTTP}" uk "${seq}" "${payload}"
  wait_for_part "${US_HTTP}" us "${seq}" "${payload}"
}

verify_many_hls_users() {
  local pids=()
  local failed=0

  for region in uk us; do
    local port
    if [[ "${region}" == "uk" ]]; then
      port="${UK_HTTP}"
    else
      port="${US_HTTP}"
    fi

    for user in $(seq 1 "${SMOKE_USERS}"); do
      (
        curl -skfs "https://127.0.0.1:${port}/live/stream.m3u8" >/dev/null
        for part in 0 1 2 3; do
          curl -skfs "https://127.0.0.1:${port}/live/part${part}.ts" >/dev/null
        done
      ) >"${TMPDIR}/${region}-user-${user}.log" 2>&1 &
      pids+=("$!")
    done
  done

  for pid in "${pids[@]}"; do
    if ! wait "${pid}"; then
      failed=1
    fi
  done

  if [[ "${failed}" -ne 0 ]]; then
    echo "one or more concurrent HLS users failed" >&2
    echo "--- uk log ---" >&2
    sed -n '1,200p' "${TMPDIR}/uk.log" >&2 || true
    echo "--- us log ---" >&2
    sed -n '1,200p' "${TMPDIR}/us.log" >&2 || true
    return 1
  fi
}

cd "${ROOT}"
cargo build --locked --bins

RUST_LOG="${RUST_LOG:-av_mesh=info,playlists=info,web_service=info}" \
  "${BIN}" \
  --region uk \
  --node-id uk-smoke \
  --mesh-bind "${UK_MESH}" \
  --peer "${US_MESH}" \
  --http-port "${UK_HTTP}" \
  --ingest-bind "${UK_UDP}" \
  --fec-bind "${UK_FEC}" \
  --rist-bind "${UK_RIST}" \
  --rist-mesh-bind "${UK_RIST_MESH}" \
  --rist-mesh-peer "${US_RIST_MESH}" \
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
  --fec-bind "${US_FEC}" \
  --rist-bind "${US_RIST}" \
  --rist-mesh-bind "${US_RIST_MESH}" \
  --rist-mesh-peer "${UK_RIST_MESH}" \
  --part-ms 100 \
  --parts-per-segment 2 \
  --window-parts 8 \
  --slot-kb 64 \
  >"${TMPDIR}/us.log" 2>&1 &
US_PID="$!"

wait_for_health "${UK_HTTP}" uk
wait_for_health "${US_HTTP}" us

publish_part http 0 'AVMESH-SMOKE-HTTP-0000'
publish_part udp 1 'AVMESH-SMOKE-UDP-0001'
publish_part udp-fec 2 'AVMESH-SMOKE-FEC-0002'
publish_part rist 3 'AVMESH-SMOKE-RIST-0003'
verify_many_hls_users

echo "two-region smoke passed: http udp udp-fec rist ingest reached UK/US HLS for ${SMOKE_USERS} users per region"
