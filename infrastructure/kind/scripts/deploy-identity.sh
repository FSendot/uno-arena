#!/usr/bin/env bash
# Explicit: helm upgrade --install Identity into kind (no static duplicate Deployment).
# Requires bootstrap-postgres-identity complete and local image loaded.
# Never exposes Identity publicly (ClusterIP only via chart Service).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
require_cmd helm
assert_kind_context

CHART="${REPO_ROOT}/services/identity/helm/identity"
VALUES_KIND="${CHART}/values.kind.yaml"
RELEASE="${IDENTITY_HELM_RELEASE:-identity}"
TIMEOUT="${IDENTITY_HELM_TIMEOUT:-180s}"

[[ -f "${VALUES_KIND}" ]] || die "missing ${VALUES_KIND}"

echo "helm upgrade --install ${RELEASE} (kind values) into ${KIND_NAMESPACE}"
helm upgrade --install "${RELEASE}" "${CHART}" \
  --namespace "${KIND_NAMESPACE}" \
  -f "${VALUES_KIND}" \
  --wait --timeout "${TIMEOUT}"

echo "waiting for deployment/${RELEASE}"
kubectl -n "${KIND_NAMESPACE}" rollout status "deployment/${RELEASE}" --timeout="${TIMEOUT}"
echo "ok kind-deploy-identity release=${RELEASE}"
