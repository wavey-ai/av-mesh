#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTRIB_ROOT="${CONTRIB_ROOT:-$(cd "${ROOT}/../av-contrib" && pwd)}"
BIN="${ROOT}/target/debug/av-mesh"
CONTRIB_BIN="${CONTRIB_ROOT}/target/debug/av-contrib"
UDP_FEC_BIN="${CONTRIB_ROOT}/target/debug/udp-fec-send"
RIST_BIN="${CONTRIB_ROOT}/target/debug/rist-send"
TMPDIR="$(mktemp -d "${TMPDIR:-/tmp}/av-mesh-smoke.XXXXXX")"

UK_MESH="${UK_MESH:-127.0.0.1:19101}"
US_MESH="${US_MESH:-127.0.0.1:19201}"
UK_HTTP="${UK_HTTP:-19444}"
US_HTTP="${US_HTTP:-19445}"
UK_CONTRIB_HTTP="${UK_CONTRIB_HTTP:-19443}"
UK_FEC="${UK_FEC:-127.0.0.1:12001}"
US_FEC="${US_FEC:-127.0.0.1:12002}"
UK_MEDIA_FEC="${UK_MEDIA_FEC:-127.0.0.1:12101}"
US_MEDIA_FEC="${US_MEDIA_FEC:-127.0.0.1:12102}"
UK_RIST="${UK_RIST:-127.0.0.1:17000}"
UK_TELEMETRY="${UK_TELEMETRY:-127.0.0.1:17300}"
US_TELEMETRY="${US_TELEMETRY:-127.0.0.1:17301}"
SMOKE_USERS="${SMOKE_USERS:-8}"

UK_PID=""
US_PID=""
CONTRIB_PID=""

cleanup() {
  if [[ -n "${CONTRIB_PID}" ]]; then
    kill "${CONTRIB_PID}" 2>/dev/null || true
  fi
  if [[ -n "${UK_PID}" ]]; then
    kill "${UK_PID}" 2>/dev/null || true
  fi
  if [[ -n "${US_PID}" ]]; then
    kill "${US_PID}" 2>/dev/null || true
  fi
  wait "${CONTRIB_PID}" 2>/dev/null || true
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

generate_local_tls() {
  local openssl_conf="${TMPDIR}/openssl-local-wavey.cnf"
  cat >"${openssl_conf}" <<EOF
[req]
distinguished_name=req_distinguished_name
x509_extensions=v3_req
prompt=no

[req_distinguished_name]
CN=local.wavey.ai

[v3_req]
subjectAltName=@alt_names

[alt_names]
DNS.1=local.wavey.ai
EOF

  openssl req -x509 -newkey rsa:2048 -sha256 -days 7 -nodes \
    -keyout "${TMPDIR}/local.wavey.ai.key" \
    -out "${TMPDIR}/local.wavey.ai.crt" \
    -config "${openssl_conf}" >/dev/null 2>&1
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

wait_for_mesh_text() {
  local port="$1"
  local name="$2"
  local pattern="$3"
  local description="$4"
  local snapshot_file="${TMPDIR}/${name}-mesh.json"

  for _ in $(seq 1 120); do
    if curl -skfs "https://127.0.0.1:${port}/api/mesh" >"${snapshot_file}"; then
      if grep -Fq "${pattern}" "${snapshot_file}"; then
        return 0
      fi
    fi
    sleep 0.1
  done

  echo "${name} mesh snapshot did not contain ${description}" >&2
  echo "--- ${name} mesh snapshot ---" >&2
  cat "${snapshot_file}" >&2 || true
  echo >&2
  echo "--- uk log ---" >&2
  sed -n '1,240p' "${TMPDIR}/uk.log" >&2 || true
  echo "--- us log ---" >&2
  sed -n '1,240p' "${TMPDIR}/us.log" >&2 || true
  return 1
}

publish_part() {
  local protocol="$1"
  local seq="$2"
  local payload="$3"

  case "${protocol}" in
    http)
      printf '%s' "${payload}" \
        | curl -skfs -X POST --data-binary @- "https://127.0.0.1:${UK_CONTRIB_HTTP}/ingest" >/dev/null
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

verify_tcp_changes_control() {
  local command_file="${TMPDIR}/uk-warm-command.json"

  wait_for_mesh_text "${UK_HTTP}" uk '"node_id":"us-smoke"' "remote US telemetry"
  wait_for_mesh_text "${US_HTTP}" us '"node_id":"uk-smoke"' "remote UK telemetry"

  curl -skfs -X POST \
    -H 'content-type: application/json' \
    --data-binary '{"stream_id":4,"region":"us"}' \
    "https://127.0.0.1:${UK_HTTP}/api/control/warm-stream" \
    >"${command_file}"

  if ! grep -Fq "published AVMC control" "${command_file}"; then
    echo "UK warm-stream command did not publish AVMC control" >&2
    echo "--- command response ---" >&2
    cat "${command_file}" >&2 || true
    echo >&2
    return 1
  fi

  wait_for_mesh_text "${US_HTTP}" us "received from uk-smoke command" "remote AVMC command receipt"
  wait_for_mesh_text "${US_HTTP}" us '"stream_id":4' "warm-stream command stream id"
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
cargo build --locked --bin av-mesh
cd "${CONTRIB_ROOT}"
cargo build --locked --bins
cd "${ROOT}"
generate_local_tls

RUST_LOG="${RUST_LOG:-av_mesh=info,playlists=info,web_service=info}" \
  "${BIN}" \
  --cert "${TMPDIR}/local.wavey.ai.crt" \
  --key "${TMPDIR}/local.wavey.ai.key" \
  --region uk \
  --node-id uk-smoke \
  --mesh-bind "${UK_MESH}" \
  --peer "${US_MESH}" \
  --http-port "${UK_HTTP}" \
  --fec-bind "${UK_FEC}" \
  --media-fec-bind "${UK_MEDIA_FEC}" \
  --telemetry-bind "${UK_TELEMETRY}" \
  --telemetry-peer "${US_TELEMETRY}" \
  --telemetry-dns-name local.wavey.ai \
  --telemetry-interval-ms 200 \
  --part-ms 100 \
  --parts-per-segment 2 \
  --window-parts 8 \
  --slot-kb 64 \
  >"${TMPDIR}/uk.log" 2>&1 &
UK_PID="$!"

RUST_LOG="${RUST_LOG:-av_mesh=info,playlists=info,web_service=info}" \
  "${BIN}" \
  --cert "${TMPDIR}/local.wavey.ai.crt" \
  --key "${TMPDIR}/local.wavey.ai.key" \
  --region us \
  --node-id us-smoke \
  --mesh-bind "${US_MESH}" \
  --peer "${UK_MESH}" \
  --http-port "${US_HTTP}" \
  --fec-bind "${US_FEC}" \
  --media-fec-bind "${US_MEDIA_FEC}" \
  --telemetry-bind "${US_TELEMETRY}" \
  --telemetry-peer "${UK_TELEMETRY}" \
  --telemetry-dns-name local.wavey.ai \
  --telemetry-interval-ms 200 \
  --part-ms 100 \
  --parts-per-segment 2 \
  --window-parts 8 \
  --slot-kb 64 \
  >"${TMPDIR}/us.log" 2>&1 &
US_PID="$!"

RUST_LOG="${RUST_LOG:-av_contrib=info,web_service=info}" \
  "${CONTRIB_BIN}" \
  --cert "${TMPDIR}/local.wavey.ai.crt" \
  --key "${TMPDIR}/local.wavey.ai.key" \
  --http-port "${UK_CONTRIB_HTTP}" \
  --mesh-fec-target "${UK_FEC}" \
  --mesh-media-fec-target "${UK_MEDIA_FEC}" \
  --rist-bind "${UK_RIST}" \
  >"${TMPDIR}/contrib.log" 2>&1 &
CONTRIB_PID="$!"

wait_for_health "${UK_HTTP}" uk
wait_for_health "${US_HTTP}" us
wait_for_health "${UK_CONTRIB_HTTP}" contrib

publish_part http 0 'AVMESH-SMOKE-HTTP-0000'
publish_part udp-fec 1 'AVMESH-SMOKE-FEC-0001'
publish_part udp-fec 2 'AVMESH-SMOKE-FEC-0002'
publish_part rist 3 'AVMESH-SMOKE-RIST-0003'
verify_tcp_changes_control
verify_many_hls_users

echo "two-region smoke passed: http/rist contributor frontend plus udp-fec mesh bytes reached UK/US HLS, tcp-changes AVMT topology converged, and AVMC warm-stream control reached US for ${SMOKE_USERS} users per region"
