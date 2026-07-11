#!/usr/bin/env bash
# Explicit: helm upgrade --install Gateway into kind (no static duplicate Deployment).
# Requires local Redis + image loaded. Kafka SessionInvalidated consumer remains PENDING.
# Debezium player-feed sink remains PENDING (prove SSE via manual XADD).
# Never exposes Gateway publicly beyond chart Service (ClusterIP).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
require_cmd helm
assert_kind_context

CHART="${REPO_ROOT}/services/gateway/helm/gateway"
VALUES_KIND="${CHART}/values.kind.yaml"
RELEASE="${GATEWAY_HELM_RELEASE:-gateway}"
TIMEOUT="${GATEWAY_HELM_TIMEOUT:-180s}"

[[ -f "${VALUES_KIND}" ]] || die "missing ${VALUES_KIND}"

echo "helm upgrade --install ${RELEASE} (kind values) into ${KIND_NAMESPACE}"
helm upgrade --install "${RELEASE}" "${CHART}" \
  --namespace "${KIND_NAMESPACE}" \
  -f "${VALUES_KIND}" \
  --wait --timeout "${TIMEOUT}"

echo "waiting for deployment/${RELEASE}"
kubectl -n "${KIND_NAMESPACE}" rollout status "deployment/${RELEASE}" --timeout="${TIMEOUT}"
echo "ok kind-deploy-gateway release=${RELEASE} (Kafka/Debezium PENDING)"
