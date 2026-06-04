#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AV_MESH_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE_ROOT="${AV_MESH_BOOTSTRAP_SOURCE_ROOT:-$(cd "${AV_MESH_ROOT}/.." && pwd)}"

REMOTE_HOST="${AV_MESH_LINODE_PUBLIC_IPV4:-${1:-}}"
REMOTE_USER="${AV_MESH_BOOTSTRAP_SSH_USER:-root}"
SSH_CONNECT_TIMEOUT="${AV_MESH_BOOTSTRAP_SSH_CONNECT_TIMEOUT:-10}"
SSH_WAIT_ATTEMPTS="${AV_MESH_BOOTSTRAP_SSH_WAIT_ATTEMPTS:-60}"
SSH_KEY="${AV_MESH_BOOTSTRAP_SSH_KEY:-}"
REMOTE_ROOT="${AV_MESH_BOOTSTRAP_REMOTE_ROOT:-/opt/wavey.ai}"
SERVICE_NAME="${AV_MESH_BOOTSTRAP_SERVICE_NAME:-av-mesh}"
NODE_ID="${AV_MESH_PROVISION_NODE_ID:-${AV_MESH_LINODE_LABEL:-av-mesh-node}}"
REGION="${AV_MESH_PROVISION_REGION:-${AV_MESH_LOCAL_REGION:-uk}}"
CONTINENT="${AV_MESH_BOOTSTRAP_CONTINENT:-}"
LATITUDE="${AV_MESH_BOOTSTRAP_LATITUDE:-}"
LONGITUDE="${AV_MESH_BOOTSTRAP_LONGITUDE:-}"
FEATURES="${AV_MESH_BOOTSTRAP_FEATURES:-private-subnet-discovery}"
PRIVATE_DISCOVERY="${AV_MESH_BOOTSTRAP_PRIVATE_DISCOVERY:-1}"
MESH_PORT="${AV_MESH_BOOTSTRAP_MESH_PORT:-9101}"
HTTP_PORT="${AV_MESH_BOOTSTRAP_HTTP_PORT:-9444}"
PLAYBACK_BASE_URL="${AV_MESH_BOOTSTRAP_PLAYBACK_BASE_URL:-}"
TELEMETRY_PORT="${AV_MESH_BOOTSTRAP_TELEMETRY_PORT:-7300}"
DISCOVERY_PORT="${AV_MESH_BOOTSTRAP_DISCOVERY_PORT:-12345}"
TELEMETRY_DNS_NAME="${AV_MESH_BOOTSTRAP_TELEMETRY_DNS_NAME:-local.wavey.ai}"
RUST_LOG_VALUE="${AV_MESH_BOOTSTRAP_RUST_LOG:-av_mesh=info,playlists=info,web_service=info}"
EXTRA_ARGS="${AV_MESH_BOOTSTRAP_EXTRA_ARGS:-}"
DRY_RUN="${AV_MESH_BOOTSTRAP_DRY_RUN:-0}"
TLS_DIR="${AV_MESH_BOOTSTRAP_TLS_DIR:-${AV_MESH_ROOT}/.bootstrap-tls}"
TLS_CERT_PATH="${AV_MESH_TLS_CERT_PATH:-}"
TLS_KEY_PATH="${AV_MESH_TLS_KEY_PATH:-}"

PRIVATE_IPV4="${AV_MESH_TELEMETRY_PRIVATE_IPV4:-}"
if [[ -z "${PRIVATE_IPV4}" && -n "${AV_MESH_LINODE_PRIVATE_IPAM:-}" ]]; then
  PRIVATE_IPV4="${AV_MESH_LINODE_PRIVATE_IPAM%%/*}"
fi
PRIVATE_IPV4="${PRIVATE_IPV4:-127.0.0.1}"
if [[ -z "${PLAYBACK_BASE_URL}" ]]; then
  if [[ -n "${AV_MESH_LINODE_DNS_NAME:-}" ]]; then
    PLAYBACK_BASE_URL="https://${AV_MESH_LINODE_DNS_NAME}:${HTTP_PORT}/live"
  elif [[ -n "${REMOTE_HOST}" ]]; then
    PLAYBACK_BASE_URL="https://${REMOTE_HOST}:${HTTP_PORT}/live"
  fi
fi

SSH_OPTS=(
  -o StrictHostKeyChecking=no
  -o ConnectTimeout="${SSH_CONNECT_TIMEOUT}"
)
if [[ -n "${SSH_KEY}" ]]; then
  SSH_OPTS+=(-i "${SSH_KEY}")
fi

SYNC_DIRS=(
  av-mesh
  playlists
  raptor-fec
  web-services
  tcp-changes
  discovery
  linode
)

usage() {
  cat <<EOF
Usage: AV_MESH_LINODE_PUBLIC_IPV4=<ip> $0
       $0 <ip>

Environment:
  AV_MESH_PROVISION_NODE_ID       Node id for the new mesh node.
  AV_MESH_PROVISION_REGION        Mesh region for the new node.
  AV_MESH_LINODE_PRIVATE_IPAM     Private VLAN CIDR, used for telemetry private IPv4.
  AV_MESH_BOOTSTRAP_PLAYBACK_BASE_URL  Public av-llhls base URL, default: Linode DNS or public IP.
  AV_MESH_BOOTSTRAP_FEATURES      Cargo features, default: private-subnet-discovery.
  AV_MESH_BOOTSTRAP_EXTRA_ARGS    Additional av-mesh args appended to the service.
  AV_MESH_TLS_CERT_PATH/KEY_PATH  Shared TLS material to install; generated locally if omitted.
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

run() {
  if [[ "${DRY_RUN}" == "1" ]]; then
    printf 'DRY-RUN:'
    printf ' %q' "$@"
    printf '\n'
  else
    "$@"
  fi
}

validate_local_inputs() {
  [[ -n "${REMOTE_HOST}" ]] || {
    usage >&2
    die "remote host missing"
  }
  command -v ssh >/dev/null || die "ssh is required"
  command -v scp >/dev/null || die "scp is required"
  command -v rsync >/dev/null || die "rsync is required"
  for dir in "${SYNC_DIRS[@]}"; do
    [[ -d "${WORKSPACE_ROOT}/${dir}" ]] || die "missing ${WORKSPACE_ROOT}/${dir}"
  done
}

ensure_tls() {
  if [[ -n "${TLS_CERT_PATH}" || -n "${TLS_KEY_PATH}" ]]; then
    [[ -f "${TLS_CERT_PATH}" ]] || die "AV_MESH_TLS_CERT_PATH not found: ${TLS_CERT_PATH}"
    [[ -f "${TLS_KEY_PATH}" ]] || die "AV_MESH_TLS_KEY_PATH not found: ${TLS_KEY_PATH}"
    return
  fi

  TLS_CERT_PATH="${TLS_DIR}/local.wavey.ai.crt"
  TLS_KEY_PATH="${TLS_DIR}/local.wavey.ai.key"
  if [[ -f "${TLS_CERT_PATH}" && -f "${TLS_KEY_PATH}" ]]; then
    return
  fi

  if [[ "${DRY_RUN}" == "1" ]]; then
    echo "DRY-RUN: generate shared TLS material in ${TLS_DIR}"
    return
  fi

  command -v openssl >/dev/null || die "openssl is required to generate local TLS material"
  mkdir -p "${TLS_DIR}"
  local openssl_conf="${TLS_DIR}/openssl-local-wavey.cnf"
  cat >"${openssl_conf}" <<EOF
[req]
distinguished_name=req_distinguished_name
x509_extensions=v3_req
prompt=no

[req_distinguished_name]
CN=${TELEMETRY_DNS_NAME}

[v3_req]
subjectAltName=@alt_names

[alt_names]
DNS.1=${TELEMETRY_DNS_NAME}
EOF

  openssl req -x509 -newkey rsa:2048 -sha256 -days 365 -nodes \
    -keyout "${TLS_KEY_PATH}" \
    -out "${TLS_CERT_PATH}" \
    -config "${openssl_conf}" >/dev/null 2>&1
  chmod 600 "${TLS_KEY_PATH}"
}

wait_for_ssh() {
  local attempt=0
  until ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${REMOTE_HOST}" "echo ok" >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [[ "${attempt}" -ge "${SSH_WAIT_ATTEMPTS}" ]]; then
      die "timed out waiting for SSH on ${REMOTE_HOST}"
    fi
    sleep 5
  done
}

sync_workspace() {
  run ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${REMOTE_HOST}" \
    "install -d -m 755 '${REMOTE_ROOT}' /etc/av-mesh/tls /var/lib/av-mesh"
  for dir in "${SYNC_DIRS[@]}"; do
    run rsync -az --delete \
      --exclude target \
      --exclude .git \
      --exclude .bootstrap-tls \
      --exclude node_modules \
      "${WORKSPACE_ROOT}/${dir}/" \
      "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_ROOT}/${dir}/"
  done
  run scp "${SSH_OPTS[@]}" "${TLS_CERT_PATH}" \
    "${REMOTE_USER}@${REMOTE_HOST}:/etc/av-mesh/tls/fullchain.pem"
  run scp "${SSH_OPTS[@]}" "${TLS_KEY_PATH}" \
    "${REMOTE_USER}@${REMOTE_HOST}:/etc/av-mesh/tls/privkey.pem"
}

install_remote_service() {
  run ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${REMOTE_HOST}" \
    NODE_ID="${NODE_ID}" \
    REGION="${REGION}" \
    CONTINENT="${CONTINENT}" \
    LATITUDE="${LATITUDE}" \
    LONGITUDE="${LONGITUDE}" \
    FEATURES="${FEATURES}" \
    REMOTE_ROOT="${REMOTE_ROOT}" \
    SERVICE_NAME="${SERVICE_NAME}" \
    MESH_PORT="${MESH_PORT}" \
    HTTP_PORT="${HTTP_PORT}" \
    PLAYBACK_BASE_URL="${PLAYBACK_BASE_URL}" \
    TELEMETRY_PORT="${TELEMETRY_PORT}" \
    DISCOVERY_PORT="${DISCOVERY_PORT}" \
    TELEMETRY_DNS_NAME="${TELEMETRY_DNS_NAME}" \
    PRIVATE_IPV4="${PRIVATE_IPV4}" \
    PRIVATE_DISCOVERY="${PRIVATE_DISCOVERY}" \
    RUST_LOG_VALUE="${RUST_LOG_VALUE}" \
    EXTRA_ARGS="${EXTRA_ARGS}" \
    'bash -se' <<'REMOTE'
set -euo pipefail

if command -v pacman >/dev/null 2>&1; then
  pacman -Sy --noconfirm archlinux-keyring ca-certificates-mozilla
  pacman -Syu --noconfirm --needed base-devel ca-certificates curl git openssl pkgconf rsync
elif command -v apt-get >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y build-essential ca-certificates curl git libssl-dev pkg-config rsync
else
  echo "unsupported distro: expected pacman or apt-get" >&2
  exit 1
fi

systemctl enable --now systemd-timesyncd 2>/dev/null || true
systemctl enable --now sshd.service 2>/dev/null || true
systemctl enable --now ssh.service 2>/dev/null || true

if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi
if [ -f /root/.cargo/env ]; then
  . /root/.cargo/env
fi

build_args=(cargo build --release --locked --manifest-path "${REMOTE_ROOT}/av-mesh/Cargo.toml" --bin av-mesh)
if [ -n "${FEATURES}" ]; then
  build_args+=(--features "${FEATURES}")
fi
CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-1}" "${build_args[@]}"

install -m 755 "${REMOTE_ROOT}/av-mesh/target/release/av-mesh" /usr/local/bin/av-mesh
install -d -m 755 /etc/av-mesh /var/lib/av-mesh
chmod 600 /etc/av-mesh/tls/privkey.pem

cat >/etc/av-mesh/env <<EOF
RUST_LOG=${RUST_LOG_VALUE}
EOF
printf 'AV_MESH_EXTRA_ARGS=%q\n' "${EXTRA_ARGS}" >>/etc/av-mesh/env
chmod 600 /etc/av-mesh/env

cat >/usr/local/bin/av-mesh-run <<'RUNNER'
#!/usr/bin/env bash
set -euo pipefail

extra_args=()
if [ -n "${AV_MESH_EXTRA_ARGS:-}" ]; then
  # shellcheck disable=SC2206
  extra_args=(${AV_MESH_EXTRA_ARGS})
fi
if [ -n "${AV_MESH_BOOTSTRAP_CONTINENT:-}" ]; then
  extra_args+=(--continent "${AV_MESH_BOOTSTRAP_CONTINENT}")
fi
if [ -n "${AV_MESH_BOOTSTRAP_LATITUDE:-}" ]; then
  extra_args+=(--latitude "${AV_MESH_BOOTSTRAP_LATITUDE}")
fi
if [ -n "${AV_MESH_BOOTSTRAP_LONGITUDE:-}" ]; then
  extra_args+=(--longitude "${AV_MESH_BOOTSTRAP_LONGITUDE}")
fi

mesh_args=(
  /usr/local/bin/av-mesh
  --region "${AV_MESH_REGION}"
  --node-id "${AV_MESH_NODE_ID}"
  --mesh-bind "0.0.0.0:${AV_MESH_MESH_PORT}"
  --http-port "${AV_MESH_HTTP_PORT}"
  --telemetry-bind "0.0.0.0:${AV_MESH_TELEMETRY_PORT}"
  --telemetry-dns-name "${AV_MESH_TELEMETRY_DNS_NAME}"
  --telemetry-private-ipv4 "${AV_MESH_TELEMETRY_PRIVATE_IPV4}"
  --cert /etc/av-mesh/tls/fullchain.pem
  --key /etc/av-mesh/tls/privkey.pem
)
if [ -n "${AV_MESH_PLAYBACK_BASE_URL:-}" ]; then
  mesh_args+=(--playback-base-url "${AV_MESH_PLAYBACK_BASE_URL}")
fi
if [ "${AV_MESH_PRIVATE_SUBNET_DISCOVERY:-1}" = "1" ]; then
  mesh_args+=(
    --private-subnet-discovery
    --private-discovery-broadcast-port "${AV_MESH_DISCOVERY_PORT}"
    --private-discovery-mesh-port "${AV_MESH_MESH_PORT}"
  )
fi

exec "${mesh_args[@]}" "${extra_args[@]}"
RUNNER
chmod +x /usr/local/bin/av-mesh-run

cat >/etc/systemd/system/"${SERVICE_NAME}".service <<EOF
[Unit]
Description=Wavey AV Mesh Node
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/av-mesh/env
Environment=AV_MESH_NODE_ID=${NODE_ID}
Environment=AV_MESH_REGION=${REGION}
Environment=AV_MESH_MESH_PORT=${MESH_PORT}
Environment=AV_MESH_HTTP_PORT=${HTTP_PORT}
Environment=AV_MESH_PLAYBACK_BASE_URL=${PLAYBACK_BASE_URL}
Environment=AV_MESH_TELEMETRY_PORT=${TELEMETRY_PORT}
Environment=AV_MESH_TELEMETRY_DNS_NAME=${TELEMETRY_DNS_NAME}
Environment=AV_MESH_TELEMETRY_PRIVATE_IPV4=${PRIVATE_IPV4}
Environment=AV_MESH_DISCOVERY_PORT=${DISCOVERY_PORT}
Environment=AV_MESH_PRIVATE_SUBNET_DISCOVERY=${PRIVATE_DISCOVERY}
EOF

if [ -n "${CONTINENT}" ]; then
  echo "Environment=AV_MESH_BOOTSTRAP_CONTINENT=${CONTINENT}" >>/etc/systemd/system/"${SERVICE_NAME}".service
fi
if [ -n "${LATITUDE}" ]; then
  echo "Environment=AV_MESH_BOOTSTRAP_LATITUDE=${LATITUDE}" >>/etc/systemd/system/"${SERVICE_NAME}".service
fi
if [ -n "${LONGITUDE}" ]; then
  echo "Environment=AV_MESH_BOOTSTRAP_LONGITUDE=${LONGITUDE}" >>/etc/systemd/system/"${SERVICE_NAME}".service
fi

cat >>/etc/systemd/system/"${SERVICE_NAME}".service <<'EOF'
ExecStart=/usr/local/bin/av-mesh-run
Restart=always
RestartSec=5
StateDirectory=av-mesh
NoNewPrivileges=yes
PrivateTmp=yes

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}"
systemctl restart "${SERVICE_NAME}"
systemctl --no-pager --full status "${SERVICE_NAME}" || true
REMOTE
}

main() {
  validate_local_inputs
  ensure_tls

  echo "Bootstrapping av-mesh node ${NODE_ID} (${REGION}) on ${REMOTE_USER}@${REMOTE_HOST}"
  echo "private_ipv4=${PRIVATE_IPV4} mesh_port=${MESH_PORT} telemetry_port=${TELEMETRY_PORT}"
  echo "playback_base_url=${PLAYBACK_BASE_URL:-unset}"
  echo "features=${FEATURES}"

  if [[ "${DRY_RUN}" == "1" ]]; then
    echo "DRY-RUN: would wait for SSH on ${REMOTE_HOST}"
  else
    wait_for_ssh
  fi
  sync_workspace
  install_remote_service
}

main "$@"
