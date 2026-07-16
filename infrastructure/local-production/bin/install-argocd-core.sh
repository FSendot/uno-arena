#!/usr/bin/env bash
# Operator-only Argo core installation. Does not register repositories or apply Applications.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
require_cmd ruby
assert_simulator_cluster_exists
assert_exact_context

ARGOCD_INSTALL_MANIFEST="${LOCAL_PRODUCTION_DIR}/vendor/argocd/install.yaml"
"${LOCAL_PRODUCTION_DIR}/vendor/verify.sh"
[[ -f "${ARGOCD_INSTALL_MANIFEST}" && ! -L "${ARGOCD_INSTALL_MANIFEST}" ]] ||
  die "missing pre-vendored Argo CD manifest"

ARGOCD_NETWORK_POLICIES=(
  argocd-application-controller-network-policy
  argocd-applicationset-controller-network-policy
  argocd-dex-server-network-policy
  argocd-notifications-controller-network-policy
  argocd-redis-network-policy
  argocd-repo-server-network-policy
  argocd-server-network-policy
)
# This local kind simulator's userspace NFQUEUE policy engine blocks selected
# pods on the OrbStack kernel. Omit only Argo's bundled policies here;
# application namespaces retain Ambient controls and production retains CNI policies.
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd delete networkpolicy \
  "${ARGOCD_NETWORK_POLICIES[@]}" --ignore-not-found
"${SCRIPT_DIR}/render-argocd-core-manifest" "${ARGOCD_INSTALL_MANIFEST}" |
  kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd apply --server-side -f -
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd wait \
  --for=condition=Available deployments --all --timeout=300s
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd rollout status \
  statefulset/argocd-application-controller --timeout=300s
"${SCRIPT_DIR}/check-argocd-controller-network.sh"
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
