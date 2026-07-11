#!/usr/bin/env bash
# Explicit target: build bootstrap image and load into kind. Requires docker + kind.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd docker
require_cmd kind
assert_kind_cluster_name
assert_kind_context

docker build -f "${REPO_ROOT}/infrastructure/bootstrap/Dockerfile" \
  -t "${BOOTSTRAP_IMAGE}" \
  "${REPO_ROOT}"

kind load docker-image "${BOOTSTRAP_IMAGE}" --name "${KIND_CLUSTER_NAME}"
echo "ok kind-build-load-bootstrap image=${BOOTSTRAP_IMAGE}"
