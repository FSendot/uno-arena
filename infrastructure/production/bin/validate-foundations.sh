#!/usr/bin/env bash
# Read-only, fail-closed production foundation prerequisite validation.
set -euo pipefail

: "${PRODUCTION_KUBE_CONTEXT:?set the exact non-kind production kubectl context}"
command -v kubectl >/dev/null 2>&1 || { echo "kubectl is required" >&2; exit 1; }
[[ "$(kubectl config current-context)" == "${PRODUCTION_KUBE_CONTEXT}" ]] || {
  echo "current context does not equal PRODUCTION_KUBE_CONTEXT" >&2
  exit 1
}
case "${PRODUCTION_KUBE_CONTEXT}" in
  kind-*|docker-desktop|orbstack) echo "production validation refuses a local simulator context" >&2; exit 1 ;;
esac

k=(kubectl --context "${PRODUCTION_KUBE_CONTEXT}")
for crd in \
  applications.argoproj.io applicationsets.argoproj.io \
  certificates.cert-manager.io externalsecrets.external-secrets.io \
  gateways.gateway.networking.k8s.io authorizationpolicies.security.istio.io; do
  "${k[@]}" get "crd/${crd}" >/dev/null
done
for ref in \
  argocd/deployment/argocd-applicationset-controller \
  cert-manager/deployment/cert-manager \
  external-secrets/deployment/external-secrets \
  istio-system/deployment/istiod; do
  namespace="${ref%%/*}"
  resource="${ref#*/}"
  "${k[@]}" -n "${namespace}" rollout status "${resource}" --timeout=30s
done
"${k[@]}" -n argocd rollout status statefulset/argocd-application-controller --timeout=30s
"${k[@]}" get clustersecretstore/production-secrets >/dev/null
"${k[@]}" get gatewayclass/istio >/dev/null
"${k[@]}" get clusterissuer/uno-arena-production >/dev/null
echo "ok production-foundation-contract context=${PRODUCTION_KUBE_CONTEXT}"
