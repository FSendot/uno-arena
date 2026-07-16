#!/usr/bin/env bash
# Operator-only Argo core installation. Does not register repositories or apply Applications.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
assert_simulator_cluster_exists
assert_exact_context

ARGOCD_INSTALL_MANIFEST="${LOCAL_PRODUCTION_DIR}/vendor/argocd/install.yaml"
"${LOCAL_PRODUCTION_DIR}/vendor/verify.sh"
[[ -f "${ARGOCD_INSTALL_MANIFEST}" && ! -L "${ARGOCD_INSTALL_MANIFEST}" ]] ||
  die "missing pre-vendored Argo CD manifest"

kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd apply --server-side -f "${ARGOCD_INSTALL_MANIFEST}"
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd wait \
  --for=condition=Available deployments --all --timeout=300s
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd rollout status \
  statefulset/argocd-application-controller --timeout=300s
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd patch configmap argocd-cmd-params-cm \
  --type=merge \
  -p='{"data":{"applicationsetcontroller.enable.progressive.syncs":"true"}}'
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd rollout restart deployment/argocd-applicationset-controller
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd rollout status \
  deployment/argocd-applicationset-controller --timeout=300s
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd patch service argocd-server \
  --type=strategic \
  -p='{"spec":{"type":"NodePort","ports":[{"name":"https","port":443,"targetPort":8080,"nodePort":30445}]}}'

"${SCRIPT_DIR}/configure-argocd-ci-account.sh"
echo "ok local-production-argocd-core context=${LOCAL_PRODUCTION_CONTEXT}"
