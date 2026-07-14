#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"
assert_kind_context
exec kubectl -n observability port-forward --address=127.0.0.1 service/grafana "${GRAFANA_LOCAL_PORT:-3000}:3000"

