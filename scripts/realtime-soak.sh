#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NEEDLETAIL_ROOT="${NEEDLETAIL_ROOT:-$(cd "${SCRIPT_DIR}/../../needletail" && pwd)}"
echo "realtime-soak orchestration moved to Needletail" >&2
exec "${NEEDLETAIL_ROOT}/scripts/realtime-soak.sh" "$@"
