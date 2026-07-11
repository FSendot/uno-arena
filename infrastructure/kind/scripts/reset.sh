#!/usr/bin/env bash
# Explicit destructive reset: delete ONLY the disposable kind cluster named uno-arena
# while kubectl context is kind-uno-arena.
# KIND_CLUSTER_NAME / KIND_CONTEXT_NAME overrides are REJECTED for reset.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Pin before sourcing lib.sh defaults — reset must not honor overrides.
if [[ -n "${KIND_CLUSTER_NAME:-}" && "${KIND_CLUSTER_NAME}" != "uno-arena" ]]; then
  echo "error: kind-reset rejects KIND_CLUSTER_NAME='${KIND_CLUSTER_NAME}' (literal uno-arena only)" >&2
  exit 1
fi
if [[ -n "${KIND_CONTEXT_NAME:-}" && "${KIND_CONTEXT_NAME}" != "kind-uno-arena" ]]; then
  echo "error: kind-reset rejects KIND_CONTEXT_NAME='${KIND_CONTEXT_NAME}' (literal kind-uno-arena only)" >&2
  exit 1
fi
unset KIND_CLUSTER_NAME KIND_CONTEXT_NAME

# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

# Force literals regardless of lib.sh defaults.
KIND_CLUSTER_NAME="uno-arena"
KIND_CONTEXT_NAME="kind-uno-arena"

require_cmd kind
require_cmd kubectl

if ! kind get clusters 2>/dev/null | grep -qx "uno-arena"; then
  echo "kind cluster 'uno-arena' not present; nothing to reset"
  exit 0
fi

if kubectl config get-contexts "kind-uno-arena" >/dev/null 2>&1; then
  current="$(kubectl config current-context 2>/dev/null || true)"
  if [[ -n "$current" && "$current" != "kind-uno-arena" ]]; then
    die "refusing reset while current-context is '${current}' (expected kind-uno-arena or empty)"
  fi
fi

kind delete cluster --name "uno-arena"
echo "ok kind-reset deleted cluster=uno-arena"
