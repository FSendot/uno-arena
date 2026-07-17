#!/usr/bin/env bash
# Install the existing checksum-verified Istio 1.30.2 ambient chart set.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd helm
require_cmd kubectl
assert_simulator_cluster_exists
assert_exact_context

ISTIO_DIR="${REPO_ROOT}/infrastructure/istio"
"${ISTIO_DIR}/verify.sh"

helm upgrade --install istio-base "${ISTIO_DIR}/charts/base" \
  --namespace istio-system --create-namespace \
  --kube-context "${LOCAL_PRODUCTION_CONTEXT}" --wait --timeout 5m
helm upgrade --install istiod "${ISTIO_DIR}/charts/istiod" \
  --namespace istio-system --kube-context "${LOCAL_PRODUCTION_CONTEXT}" \
  --values "${ISTIO_DIR}/values/istiod.kind.yaml" \
  --set gatewayClasses.istio.service.spec.type=NodePort \
  --wait --timeout 5m
helm upgrade --install istio-cni "${ISTIO_DIR}/charts/cni" \
  --namespace istio-system --kube-context "${LOCAL_PRODUCTION_CONTEXT}" \
  --values "${ISTIO_DIR}/values/cni.kind.yaml" \
  --wait --timeout 5m
helm upgrade --install ztunnel "${ISTIO_DIR}/charts/ztunnel" \
  --namespace istio-system --kube-context "${LOCAL_PRODUCTION_CONTEXT}" \
  --values "${ISTIO_DIR}/values/ztunnel.kind.yaml" \
  --wait --timeout 5m

# The upstream CNI chart mounts its ConfigMap through envFrom without a pod
# template checksum, so an in-place value change otherwise leaves old agents
# running with stale settings (for example NATIVE_NFTABLES).
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n istio-system \
  rollout restart daemonset/istio-cni-node
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n istio-system rollout status deployment/istiod --timeout=300s
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n istio-system rollout status daemonset/istio-cni-node --timeout=300s
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n istio-system rollout status daemonset/ztunnel --timeout=300s
echo "ok local-production-istio version=1.30.2 context=${LOCAL_PRODUCTION_CONTEXT}"
