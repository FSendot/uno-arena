#!/usr/bin/env bash
# Explicit: build every local service image and load it into the disposable kind cluster.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd docker
require_cmd kind
assert_kind_cluster_name
assert_kind_context

services=(
  analytics
  game-integrity
  gateway
  identity
  ranking
  room-gameplay
  spectator-view
  tournament-orchestration
)

node="${KIND_CLUSTER_NAME}-control-plane"
node_arch="$(docker exec "${node}" uname -m)"
case "${node_arch}" in
  x86_64|amd64) platform="linux/amd64" ;;
  aarch64|arm64) platform="linux/arm64" ;;
  *) die "kind node architecture '${node_arch}' has no reviewed service/KurrentDB image set" ;;
esac

for service in "${services[@]}"; do
  dockerfile="${REPO_ROOT}/services/${service}/Dockerfile"
  image="uno-arena/${service}:local"
  [[ -f "${dockerfile}" ]] || die "missing service Dockerfile: ${dockerfile}"

  echo "building ${image} for ${platform}"
  docker build --platform "${platform}" -f "${dockerfile}" -t "${image}" "${REPO_ROOT}"
  kind load docker-image "${image}" --name "${KIND_CLUSTER_NAME}"
done

echo "ok kind-build-load-services count=${#services[@]} platform=${platform}"
