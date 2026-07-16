#!/usr/bin/env bash
set -euo pipefail

LOCAL_PRODUCTION_CLUSTER="uno-arena-production"
LOCAL_PRODUCTION_CONTEXT="kind-${LOCAL_PRODUCTION_CLUSTER}"
LOCAL_PRODUCTION_NAMESPACE="uno-arena"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
LOCAL_PRODUCTION_DIR="${REPO_ROOT}/infrastructure/local-production"
LOCAL_PRODUCTION_CLUSTER_CONFIG="${LOCAL_PRODUCTION_DIR}/kind/cluster.yaml"

die() {
  echo "error: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

assert_exact_context() {
  require_cmd kubectl
  require_cmd kind
  local current actual_cluster actual_server actual_ca expected_server expected_ca
  current="$(kubectl config current-context 2>/dev/null || true)"
  [[ "${current}" == "${LOCAL_PRODUCTION_CONTEXT}" ]] ||
    die "refusing context '${current:-none}'; expected ${LOCAL_PRODUCTION_CONTEXT}"

  actual_cluster="$(kubectl config view --raw \
    -o jsonpath="{.contexts[?(@.name==\"${LOCAL_PRODUCTION_CONTEXT}\")].context.cluster}")"
  actual_server="$(kubectl config view --raw \
    -o jsonpath="{.clusters[?(@.name==\"${actual_cluster}\")].cluster.server}")"
  actual_ca="$(kubectl config view --raw \
    -o jsonpath="{.clusters[?(@.name==\"${actual_cluster}\")].cluster.certificate-authority-data}")"
  expected_server="$(kind get kubeconfig --name "${LOCAL_PRODUCTION_CLUSTER}" |
    kubectl config view --raw --kubeconfig=/dev/stdin -o jsonpath='{.clusters[0].cluster.server}')"
  expected_ca="$(kind get kubeconfig --name "${LOCAL_PRODUCTION_CLUSTER}" |
    kubectl config view --raw --kubeconfig=/dev/stdin -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')"
  [[ -n "${actual_server}" && -n "${actual_ca}" &&
    "${actual_server}" == "${expected_server}" && "${actual_ca}" == "${expected_ca}" ]] ||
    die "context ${LOCAL_PRODUCTION_CONTEXT} is not bound to kind cluster ${LOCAL_PRODUCTION_CLUSTER}"
}

assert_simulator_cluster_exists() {
  require_cmd kind
  kind get clusters 2>/dev/null | grep -qx "${LOCAL_PRODUCTION_CLUSTER}" ||
    die "kind cluster '${LOCAL_PRODUCTION_CLUSTER}' does not exist"
}

sha256_file() {
  shasum -a 256 "$1" | awk '{print $1}'
}
