#!/usr/bin/env bash
# Explicit mutation: create only the long-lived local production simulator.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kind
require_cmd kubectl
require_cmd docker
require_cmd ruby

# kind v0.32.0 release-supported Kubernetes node image. The digest is part of
# the simulator contract: never let an operator/runner resolve a mutable tag.
KIND_NODE_IMAGE="kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5"

if kind get clusters 2>/dev/null | grep -qx "${LOCAL_PRODUCTION_CLUSTER}"; then
  echo "kind cluster '${LOCAL_PRODUCTION_CLUSTER}' already exists"
else
  docker image inspect "${KIND_NODE_IMAGE}" >/dev/null 2>&1 ||
    die "pinned kind node image is not present locally: ${KIND_NODE_IMAGE}; preload it in an explicitly networked step"
  kind create cluster --name "${LOCAL_PRODUCTION_CLUSTER}" --config "${LOCAL_PRODUCTION_CLUSTER_CONFIG}" \
    --image "${KIND_NODE_IMAGE}"
fi

# Budget the simulator for the default 8-vCPU local Docker VM. The hard 4+2+2
# split prevents aggregate oversubscription; the 4:1:1 shares below provide an
# additional control-plane preference on runtimes that enforce node weights.
docker update --cpus 4 "${LOCAL_PRODUCTION_CLUSTER}-control-plane" >/dev/null
docker update --cpus 2 "${LOCAL_PRODUCTION_CLUSTER}-worker" >/dev/null
docker update --cpus 2 "${LOCAL_PRODUCTION_CLUSTER}-worker2" >/dev/null
# Keep etcd and the API responsive during local cold-start storms. CPU shares are
# relative only under contention; idle control-plane capacity remains available.
docker update --cpu-shares 4096 "${LOCAL_PRODUCTION_CLUSTER}-control-plane" >/dev/null
docker update --cpu-shares 1024 "${LOCAL_PRODUCTION_CLUSTER}-worker" >/dev/null
docker update --cpu-shares 1024 "${LOCAL_PRODUCTION_CLUSTER}-worker2" >/dev/null

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

# External resolvers may return IPv6 first, but this kind lane and its Ambient
# dataplane are intentionally IPv4-only. Suppress AAAA responses so Git/Helm
# clients do not select an unroutable address during reconciliation.
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n kube-system get configmap coredns -o json |
  "${SCRIPT_DIR}/render-coredns-ipv4-patch" |
  kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n kube-system patch configmap coredns \
    --type=merge --patch-file=/dev/stdin >/dev/null

# kind initially places both DNS replicas on the control-plane. Spread them so
# API/etcd load or a single node restart cannot stall every in-cluster lookup.
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n kube-system patch deployment coredns \
  --type=strategic \
  -p='{"spec":{"template":{"spec":{"topologySpreadConstraints":[{"maxSkew":1,"topologyKey":"kubernetes.io/hostname","whenUnsatisfiable":"DoNotSchedule","labelSelector":{"matchLabels":{"k8s-app":"kube-dns"}}}],"containers":[{"name":"coredns","startupProbe":{"httpGet":{"path":"/ready","port":"readiness-probe"},"periodSeconds":10,"timeoutSeconds":5,"failureThreshold":60},"readinessProbe":{"timeoutSeconds":5,"failureThreshold":10},"resources":{"limits":{"cpu":"1"}}}]}}}}'
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n kube-system rollout restart deployment/coredns
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n kube-system rollout status \
  deployment/coredns --timeout=300s

node_count="$(kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" get nodes -o name | wc -l | tr -d ' ')"
[[ "${node_count}" == "3" ]] || die "expected 3 simulator nodes, found ${node_count}"
worker_count="$(kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" get nodes \
  -l node-role.kubernetes.io/worker -o name | wc -l | tr -d ' ')"
[[ "${worker_count}" == "2" ]] || die "expected 2 explicitly labeled worker nodes, found ${worker_count}"
echo "ok local-production-create context=${LOCAL_PRODUCTION_CONTEXT} nodes=${node_count} workers=${worker_count}"
