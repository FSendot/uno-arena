#!/usr/bin/env bash
# Offline-only, exact-context installation of the local-production foundations.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd helm
require_cmd kubectl
assert_simulator_cluster_exists
assert_exact_context

defer_private_sources=false
case "${1:-}" in
  "") ;;
  --defer-private-sources) defer_private_sources=true ;;
  *) die "usage: $0 [--defer-private-sources]" ;;
esac
[[ "$#" -le 1 ]] || die "usage: $0 [--defer-private-sources]"
if [[ "${defer_private_sources}" == false ]]; then
  : "${ARGOCD_GIT_REPO_URL:?set ARGOCD_GIT_REPO_URL to the GitLab repository HTTPS URL}"
  : "${ARGOCD_HELM_REPO_URL:?set ARGOCD_HELM_REPO_URL to the GitLab Helm registry HTTPS URL}"
fi

VENDOR="${LOCAL_PRODUCTION_DIR}/vendor"
MANIFESTS="${LOCAL_PRODUCTION_DIR}/manifests"
"${VENDOR}/verify.sh"

kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" apply -f "${MANIFESTS}/00-namespaces.yaml"

kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" apply --server-side \
  -f "${VENDOR}/gateway-api/standard-install.yaml"
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" wait --for=condition=Established \
  crd/gateways.gateway.networking.k8s.io crd/httproutes.gateway.networking.k8s.io --timeout=180s

kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" apply --server-side \
  -f "${VENDOR}/cert-manager/cert-manager.yaml"
# The full production-like simulator saturates a small Mac during cold
# reconciliation. Keep cert-manager out of BestEffort and prevent kubelet from
# repeatedly killing the admission webhook while its API watches catch up.
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n cert-manager patch deployment cert-manager \
  --type=strategic \
  -p='{"spec":{"template":{"spec":{"containers":[{"name":"cert-manager-controller","resources":{"requests":{"cpu":"100m","memory":"128Mi"},"limits":{"cpu":"500m","memory":"512Mi"}}}]}}}}'
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n cert-manager patch deployment cert-manager-cainjector \
  --type=strategic \
  -p='{"spec":{"template":{"spec":{"containers":[{"name":"cert-manager-cainjector","resources":{"requests":{"cpu":"100m","memory":"128Mi"},"limits":{"cpu":"500m","memory":"512Mi"}}}]}}}}'
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n cert-manager patch deployment cert-manager-webhook \
  --type=strategic \
  -p='{"spec":{"template":{"spec":{"containers":[{"name":"cert-manager-webhook","startupProbe":{"httpGet":{"path":"/livez","port":"healthcheck"},"periodSeconds":10,"timeoutSeconds":10,"failureThreshold":60},"livenessProbe":{"httpGet":{"path":"/livez","port":"healthcheck"},"initialDelaySeconds":60,"periodSeconds":10,"timeoutSeconds":10,"failureThreshold":10},"readinessProbe":{"httpGet":{"path":"/healthz","port":"healthcheck"},"initialDelaySeconds":5,"periodSeconds":5,"timeoutSeconds":10,"failureThreshold":10},"resources":{"requests":{"cpu":"100m","memory":"128Mi"},"limits":{"cpu":"500m","memory":"256Mi"}}}]}}}}'
for deployment in cert-manager cert-manager-cainjector cert-manager-webhook; do
  kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n cert-manager rollout status \
    "deployment/${deployment}" --timeout=600s
done

helm upgrade --install external-secrets "${VENDOR}/external-secrets/external-secrets-2.7.0.tgz" \
  --namespace external-secrets --kube-context "${LOCAL_PRODUCTION_CONTEXT}" \
  --set installCRDs=true --wait --timeout 5m

kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" apply -f "${VENDOR}/metrics-server/components.yaml"
metrics_args="$(kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n kube-system \
  get deployment metrics-server -o jsonpath='{.spec.template.spec.containers[0].args}')"
if [[ "${metrics_args}" != *--kubelet-insecure-tls* ]]; then
  # kind node certificates do not carry routable node-IP SANs. This exception
  # is local-only and must never be copied into a production-cluster overlay.
  kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n kube-system patch deployment metrics-server \
    --type=json \
    -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
fi
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n kube-system rollout status \
  deployment/metrics-server --timeout=300s

"${SCRIPT_DIR}/install-istio.sh"

kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" apply -f "${MANIFESTS}/10-external-secrets-local.yaml"
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" wait --for=condition=Ready \
  clustersecretstore/local-kubernetes --timeout=180s

kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" apply -f "${MANIFESTS}/20-ca-bootstrap.yaml"
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" wait --for=condition=Ready \
  clusterissuer/local-selfsigned-bootstrap --timeout=180s
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n cert-manager wait --for=condition=Ready \
  certificate/uno-arena-local-ca --timeout=180s
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" wait --for=condition=Ready \
  clusterissuer/uno-arena-local-ca --timeout=180s
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" apply -f "${MANIFESTS}/21-local-certificates.yaml"
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n uno-arena wait --for=condition=Ready \
  certificate/uno-arena-local-tls --timeout=180s
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd wait --for=condition=Ready \
  certificate/argocd-server-local-tls --timeout=180s

kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" apply -f "${MANIFESTS}/30-gateway.yaml"
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n uno-arena wait --for=condition=Programmed \
  gateway/uno-arena-gateway --timeout=300s
for route in uno-arena-http-redirect uno-arena-bff; do
  kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n uno-arena wait \
    --for=jsonpath='{.status.parents[0].conditions[?(@.type=="Accepted")].status}'=True \
    "httproute/${route}" --timeout=180s
done
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n uno-arena patch service uno-arena-gateway-istio \
  --type=strategic \
  -p='{"spec":{"type":"NodePort","ports":[{"name":"http","port":8080,"nodePort":30080},{"name":"https","port":8443,"nodePort":30443}]}}'

kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" apply -f "${MANIFESTS}/40-ambient-security.yaml"
if [[ "${defer_private_sources}" == true ]]; then
  "${SCRIPT_DIR}/install-argocd-core.sh"
  "${SCRIPT_DIR}/acceptance.sh" --pre-source
else
  "${SCRIPT_DIR}/bootstrap-argocd.sh"
  "${SCRIPT_DIR}/acceptance.sh" --foundation-only
fi
echo "ok local-production-foundations context=${LOCAL_PRODUCTION_CONTEXT}"
