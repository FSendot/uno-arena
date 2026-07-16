#!/usr/bin/env bash
# Explicit mutation: create only the long-lived local production simulator.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kind
require_cmd kubectl

# kind v0.32.0 release-supported Kubernetes node image. The digest is part of
# the simulator contract: never let an operator/runner resolve a mutable tag.
KIND_NODE_IMAGE="kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5"

if kind get clusters 2>/dev/null | grep -qx "${LOCAL_PRODUCTION_CLUSTER}"; then
  echo "kind cluster '${LOCAL_PRODUCTION_CLUSTER}' already exists"
else
  require_cmd docker
  docker image inspect "${KIND_NODE_IMAGE}" >/dev/null 2>&1 ||
    die "pinned kind node image is not present locally: ${KIND_NODE_IMAGE}; preload it in an explicitly networked step"
  kind create cluster --name "${LOCAL_PRODUCTION_CLUSTER}" --config "${LOCAL_PRODUCTION_CLUSTER_CONFIG}" \
    --image "${KIND_NODE_IMAGE}"
fi

assert_simulator_cluster_exists
assert_exact_context
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" wait --for=condition=Ready nodes --all --timeout=180s

# Kubernetes 1.36 rejects reserved node-role labels when they are passed via
# kubelet --node-labels. Assign the conventional worker role through the API
# after kind has joined the nodes instead.
worker_nodes="$(kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" get nodes \
  -l '!node-role.kubernetes.io/control-plane' -o name)"
[[ "$(wc -w <<<"${worker_nodes}" | tr -d ' ')" == "2" ]] ||
  die "expected two non-control-plane nodes before assigning worker roles"
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" label ${worker_nodes} \
  node-role.kubernetes.io/worker= --overwrite >/dev/null

node_count="$(kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" get nodes -o name | wc -l | tr -d ' ')"
[[ "${node_count}" == "3" ]] || die "expected 3 simulator nodes, found ${node_count}"
worker_count="$(kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" get nodes \
  -l node-role.kubernetes.io/worker -o name | wc -l | tr -d ' ')"
[[ "${worker_count}" == "2" ]] || die "expected 2 explicitly labeled worker nodes, found ${worker_count}"
echo "ok local-production-create context=${LOCAL_PRODUCTION_CONTEXT} nodes=${node_count} workers=${worker_count}"
