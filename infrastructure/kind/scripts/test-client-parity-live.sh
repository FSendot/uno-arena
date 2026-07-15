#!/usr/bin/env bash
# Live CLI/BFF contract acceptance through the kind Gateway service.
# Owns only its kubectl port-forward child; application fixtures are unique per run.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARITY_RUNNER="${SCRIPT_DIR}/../../../client-checkpoint/tests/run-live-client-parity.sh"

# shellcheck source=port-forward-gateway.sh
source "${SCRIPT_DIR}/port-forward-gateway.sh"
trap cleanup EXIT INT TERM

[[ -f "${PARITY_RUNNER}" ]] || die "missing client parity runner: ${PARITY_RUNNER}"
export UNOARENA_API_URL="${GATEWAY_BASE_URL}"

echo "running live client parity via ${UNOARENA_API_URL}" >&2
bash "${PARITY_RUNNER}"

echo "ok kind-test-client-parity-live"
