#!/usr/bin/env bash
# Explicit: helm upgrade --install Tournament Orchestration into kind (no static duplicate Deployment).
# Requires bootstrap-postgres-tournament complete and local image loaded.
# Never exposes Tournament publicly (ClusterIP only via chart Service).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
require_cmd helm
assert_kind_context

CHART="${REPO_ROOT}/services/tournament-orchestration/helm/tournament-orchestration"
VALUES_KIND="${CHART}/values.kind.yaml"
RELEASE="${TOURNAMENT_HELM_RELEASE:-tournament-orchestration}"
TIMEOUT="${TOURNAMENT_HELM_TIMEOUT:-180s}"

[[ -f "${VALUES_KIND}" ]] || die "missing ${VALUES_KIND}"

echo "helm upgrade --install ${RELEASE} (kind values) into ${KIND_NAMESPACE}"
helm upgrade --install "${RELEASE}" "${CHART}" \
  --namespace "${KIND_NAMESPACE}" \
  -f "${VALUES_KIND}" \
  --wait --timeout "${TIMEOUT}"

echo "waiting for deployment/${RELEASE}"
kubectl -n "${KIND_NAMESPACE}" rollout status "deployment/${RELEASE}" --timeout="${TIMEOUT}"
echo "ok kind-deploy-tournament-orchestration release=${RELEASE}"
