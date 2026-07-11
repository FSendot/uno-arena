#!/usr/bin/env bash
# Shared helpers for kind foundation scripts. No network.
set -euo pipefail

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-uno-arena}"
KIND_CONTEXT_NAME="${KIND_CONTEXT_NAME:-kind-${KIND_CLUSTER_NAME}}"
KIND_NAMESPACE="${KIND_NAMESPACE:-uno-arena}"

# Non-destructive scripts may override cluster/context names for validation experiments.
# Destructive reset (reset.sh) hard-pins literal uno-arena / kind-uno-arena and rejects overrides.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
KIND_DIR="${REPO_ROOT}/infrastructure/kind"
MANIFESTS_DIR="${KIND_DIR}/manifests"
GENERATED_DIR="${KIND_DIR}/generated"
CLUSTER_CONFIG="${KIND_DIR}/cluster.yaml"
BOOTSTRAP_IMAGE="${BOOTSTRAP_IMAGE:-uno-arena/bootstrap:local}"

die() {
  echo "error: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

# Refuse to operate on any kubectl context other than the disposable kind cluster.
assert_kind_context() {
  require_cmd kubectl
  local current
  current="$(kubectl config current-context 2>/dev/null || true)"
  [[ -n "$current" ]] || die "no kubectl current-context; expected ${KIND_CONTEXT_NAME}"
  [[ "$current" == "$KIND_CONTEXT_NAME" ]] || die "refusing to operate on context '${current}' (expected ${KIND_CONTEXT_NAME})"
}

assert_kind_cluster_name() {
  require_cmd kind
  if ! kind get clusters 2>/dev/null | grep -qx "${KIND_CLUSTER_NAME}"; then
    die "kind cluster '${KIND_CLUSTER_NAME}' not found"
  fi
}
