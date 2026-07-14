#!/usr/bin/env bash
# Explicit cluster mutation: install vendored Istio Ambient 1.30.2 in dependency order.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd helm
assert_kind_context
ISTIO_DIR="${REPO_ROOT}/infrastructure/istio"
# A genuinely empty node must pull the exact Istio images before Helm can
# observe readiness. Keep this local budget large enough for a cold registry
# path; callers can still override it explicitly.
TIMEOUT="${KIND_ISTIO_TIMEOUT:-600s}"

"${ISTIO_DIR}/verify.sh"
helm upgrade --install istio-base "${ISTIO_DIR}/charts/base" -n istio-system --create-namespace --wait --timeout "${TIMEOUT}"
helm upgrade --install istiod "${ISTIO_DIR}/charts/istiod" -n istio-system -f "${ISTIO_DIR}/values/istiod.kind.yaml" --wait --timeout "${TIMEOUT}"
helm upgrade --install istio-cni "${ISTIO_DIR}/charts/cni" -n istio-system -f "${ISTIO_DIR}/values/cni.kind.yaml" --wait --timeout "${TIMEOUT}"
helm upgrade --install ztunnel "${ISTIO_DIR}/charts/ztunnel" -n istio-system -f "${ISTIO_DIR}/values/ztunnel.kind.yaml" --wait --timeout "${TIMEOUT}"

kubectl -n istio-system rollout status deployment/istiod --timeout="${TIMEOUT}"
kubectl -n istio-system rollout status daemonset/istio-cni-node --timeout="${TIMEOUT}"
kubectl -n istio-system rollout status daemonset/ztunnel --timeout="${TIMEOUT}"
echo "ok kind-istio version=1.30.2"
