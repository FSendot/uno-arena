#!/usr/bin/env bash
# Explicit target: create the disposable kind cluster. Does not apply manifests.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kind
require_cmd kubectl

if kind get clusters 2>/dev/null | grep -qx "${KIND_CLUSTER_NAME}"; then
  echo "kind cluster '${KIND_CLUSTER_NAME}' already exists"
else
  kind create cluster --name "${KIND_CLUSTER_NAME}" --config "${CLUSTER_CONFIG}"
fi

kubectl config use-context "${KIND_CONTEXT_NAME}" >/dev/null
assert_kind_context
echo "ok kind-create-cluster context=${KIND_CONTEXT_NAME}"
