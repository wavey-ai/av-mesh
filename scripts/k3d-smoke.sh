#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CLUSTER="${AV_MESH_K3D_CLUSTER:-av-mesh}"
NAMESPACE="${AV_MESH_K3D_NAMESPACE:-av-mesh}"
IMAGE="${AV_MESH_K3D_IMAGE:-av-mesh:local}"
PIDS_FILE="${AV_MESH_K3D_PIDS_FILE:-/tmp/av-mesh-k3d-port-forwards.pids}"
LOG_DIR="${AV_MESH_K3D_LOG_DIR:-/tmp}"
UK_PORT="${AV_MESH_K3D_UK_PORT:-19444}"
US_PORT="${AV_MESH_K3D_US_PORT:-19445}"

usage() {
  cat <<EOF
Usage: $0 [up|check|forwards|down]

Commands:
  up        Build/import the local image, deploy the two-node mesh, and start port-forwards.
  check     Probe the forwarded UK/US mesh health and API endpoints.
  forwards  Restart only the local port-forwards.
  down      Stop port-forwards and delete the k3d cluster.
EOF
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

require_stack() {
  require_cmd docker
  require_cmd k3d
  require_cmd kubectl
  require_cmd openssl
  docker info >/dev/null 2>&1 || {
    echo "docker is not running or is not reachable" >&2
    exit 1
  }
}

cluster_exists() {
  k3d cluster get "${CLUSTER}" >/dev/null 2>&1
}

ensure_cluster() {
  if cluster_exists; then
    return
  fi
  k3d cluster create "${CLUSTER}" \
    --agents 2 \
    --wait \
    --k3s-arg "--disable=traefik@server:*"
}

use_context() {
  kubectl config use-context "k3d-${CLUSTER}" >/dev/null
}

ensure_tls_secret() {
  local tmp cert key conf
  tmp="$(mktemp -d)"
  cert="${tmp}/tls.crt"
  key="${tmp}/tls.key"
  conf="${tmp}/openssl.cnf"
  cat >"${conf}" <<EOF
[req]
distinguished_name=req_distinguished_name
x509_extensions=v3_req
prompt=no

[req_distinguished_name]
CN=localhost

[v3_req]
subjectAltName=@alt_names

[alt_names]
DNS.1=localhost
IP.1=127.0.0.1
EOF
  openssl req -x509 -newkey rsa:2048 -sha256 -days 7 -nodes \
    -keyout "${key}" \
    -out "${cert}" \
    -config "${conf}" >/dev/null 2>&1
  kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
  kubectl create secret tls av-mesh-tls \
    --namespace "${NAMESPACE}" \
    --cert "${cert}" \
    --key "${key}" \
    --dry-run=client \
    -o yaml | kubectl apply -f -
  rm -rf "${tmp}"
}

build_image() {
  docker build -f "${ROOT}/deploy/k3d/Dockerfile" -t "${IMAGE}" "${ROOT}"
  k3d image import "${IMAGE}" -c "${CLUSTER}"
}

deploy_mesh() {
  kubectl apply -f "${ROOT}/deploy/k3d/av-mesh.yaml"
  kubectl rollout status deployment/av-mesh-uk -n "${NAMESPACE}" --timeout=180s
  kubectl rollout status deployment/av-mesh-us -n "${NAMESPACE}" --timeout=180s
}

stop_forwards() {
  if [[ ! -f "${PIDS_FILE}" ]]; then
    return
  fi
  while IFS= read -r pid; do
    [[ -n "${pid}" ]] || continue
    kill "${pid}" >/dev/null 2>&1 || true
  done <"${PIDS_FILE}"
  rm -f "${PIDS_FILE}"
}

start_forwards() {
  stop_forwards
  nohup kubectl port-forward -n "${NAMESPACE}" service/av-mesh-uk "${UK_PORT}:9444" \
    >"${LOG_DIR}/av-mesh-k3d-uk-port-forward.log" 2>&1 &
  echo "$!" >"${PIDS_FILE}"
  nohup kubectl port-forward -n "${NAMESPACE}" service/av-mesh-us "${US_PORT}:9444" \
    >"${LOG_DIR}/av-mesh-k3d-us-port-forward.log" 2>&1 &
  echo "$!" >>"${PIDS_FILE}"
  sleep 2
}

check_mesh() {
  local base
  for base in "https://127.0.0.1:${UK_PORT}" "https://127.0.0.1:${US_PORT}"; do
    curl -kfsS "${base}/up" >/dev/null
    curl -kfsS "${base}/api/mesh" >/dev/null
    curl -kfsS "${base}/mesh" >/dev/null
    echo "ok ${base}"
  done
}

up() {
  require_stack
  ensure_cluster
  use_context
  ensure_tls_secret
  build_image
  deploy_mesh
  start_forwards
  check_mesh
  cat <<EOF

Mission Control:
  UK: https://127.0.0.1:${UK_PORT}/mesh
  US: https://127.0.0.1:${US_PORT}/mesh

Stop and delete the local cluster with:
  $0 down
EOF
}

down() {
  require_cmd k3d
  stop_forwards
  if cluster_exists; then
    k3d cluster delete "${CLUSTER}"
  fi
}

cmd="${1:-up}"
case "${cmd}" in
  up)
    up
    ;;
  check)
    require_cmd curl
    check_mesh
    ;;
  forwards)
    require_cmd kubectl
    start_forwards
    ;;
  down)
    down
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage >&2
    exit 1
    ;;
esac
