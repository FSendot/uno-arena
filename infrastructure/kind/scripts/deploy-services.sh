#!/usr/bin/env bash
# Explicit: deploy all application contexts after the kind foundation is ready.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

assert_kind_context

# Deploy datastore-owning/read-model contexts first. Their readiness probes are
# context-local; cross-context calls occur only when traffic arrives.
deployers=(
  deploy-identity.sh
  deploy-game-integrity.sh
  deploy-ranking.sh
  deploy-tournament-orchestration.sh
  deploy-analytics.sh
  deploy-spectator-view.sh
  deploy-room-gameplay.sh
  deploy-gateway.sh
)

for deployer in "${deployers[@]}"; do
  "${SCRIPT_DIR}/${deployer}"
done

echo "ok kind-deploy-services count=${#deployers[@]}"
