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

# Check host capacity before creating anything so an undersized Docker VM does
# not leave a partial cluster behind.
docker_cpu_count="$(docker info --format '{{.NCPU}}')"
[[ "${docker_cpu_count}" =~ ^[0-9]+$ && "${docker_cpu_count}" -ge 8 ]] ||
  die "local-production requires at least 8 CPUs assigned to Docker; found ${docker_cpu_count}"
docker_memory_bytes="$(docker info --format '{{.MemTotal}}')"
minimum_memory_bytes="$((10 * 1024 * 1024 * 1024))"
[[ "${docker_memory_bytes}" =~ ^[0-9]+$ && "${docker_memory_bytes}" -ge "${minimum_memory_bytes}" ]] ||
  die "local-production requires at least 10 GiB assigned to Docker; found ${docker_memory_bytes} bytes"

if kind get clusters 2>/dev/null | grep -qx "${LOCAL_PRODUCTION_CLUSTER}"; then
  echo "kind cluster '${LOCAL_PRODUCTION_CLUSTER}' already exists"
else
  docker image inspect "${KIND_NODE_IMAGE}" >/dev/null 2>&1 ||
    die "pinned kind node image is not present locally: ${KIND_NODE_IMAGE}; preload it in an explicitly networked step"
  kind create cluster --name "${LOCAL_PRODUCTION_CLUSTER}" --config "${LOCAL_PRODUCTION_CLUSTER_CONFIG}" \
    --image "${KIND_NODE_IMAGE}"
fi

# kind's kubelets advertise the Docker VM's full CPU count for every node. Keep
# the container quotas aligned with that advertised capacity: a 4+2+2 split
# makes the scheduler admit work that the worker cgroups cannot run and can
# starve CoreDNS, etcd, and the API during cold-start reconciliation.
docker update --cpus "${docker_cpu_count}" "${LOCAL_PRODUCTION_CLUSTER}-control-plane" >/dev/null
docker update --cpus "${docker_cpu_count}" "${LOCAL_PRODUCTION_CLUSTER}-worker" >/dev/null
docker update --cpus "${docker_cpu_count}" "${LOCAL_PRODUCTION_CLUSTER}-worker2" >/dev/null
# Keep etcd and the API responsive during local cold-start storms. CPU shares are
# relative only under contention; idle control-plane capacity remains available.
docker update --cpu-shares 4096 "${LOCAL_PRODUCTION_CLUSTER}-control-plane" >/dev/null
docker update --cpu-shares 1024 "${LOCAL_PRODUCTION_CLUSTER}-worker" >/dev/null
docker update --cpu-shares 1024 "${LOCAL_PRODUCTION_CLUSTER}-worker2" >/dev/null

# kubeadm's default liveness budget restarts the API server after 80 seconds of
# probe timeouts. A cold three-node replay can legitimately exceed that while
# exact pinned images initialize, even though etcd remains alive and recovers.
# Keep readiness strict, but allow five minutes before liveness restarts the API
# and temporarily loses its warmed RBAC/discovery caches.
apiserver_manifest="$(mktemp)"
apiserver_manifest_patched="${apiserver_manifest}.patched"
trap 'rm -f "${apiserver_manifest}" "${apiserver_manifest_patched}"' EXIT
docker cp "${LOCAL_PRODUCTION_CLUSTER}-control-plane:/etc/kubernetes/manifests/kube-apiserver.yaml" \
  "${apiserver_manifest}" >/dev/null
"${SCRIPT_DIR}/render-kube-apiserver-probe-manifest" \
  <"${apiserver_manifest}" >"${apiserver_manifest_patched}"
docker cp "${apiserver_manifest_patched}" \
  "${LOCAL_PRODUCTION_CLUSTER}-control-plane:/etc/kubernetes/manifests/kube-apiserver.yaml" >/dev/null
rm -f "${apiserver_manifest}" "${apiserver_manifest_patched}"
trap - EXIT

assert_simulator_cluster_exists
assert_exact_context
for attempt in {1..60}; do
  if kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" --request-timeout=10s \
    get --raw=/readyz >/dev/null 2>&1; then
    break
  fi
  [[ "${attempt}" != "60" ]] || die "kube-apiserver did not recover after its liveness manifest update"
  sleep 5
done
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

# Run one DNS replica per node and spread them so API/etcd load or a single node
# restart cannot stall every in-cluster lookup.
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n kube-system patch deployment coredns \
  --type=strategic \
  -p='{"spec":{"replicas":3,"template":{"spec":{"affinity":{"podAntiAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":[{"labelSelector":{"matchLabels":{"k8s-app":"kube-dns"}},"topologyKey":"kubernetes.io/hostname"}]}},"topologySpreadConstraints":[{"maxSkew":1,"topologyKey":"kubernetes.io/hostname","whenUnsatisfiable":"DoNotSchedule","labelSelector":{"matchLabels":{"k8s-app":"kube-dns"}}}],"containers":[{"name":"coredns","startupProbe":{"httpGet":{"path":"/ready","port":"readiness-probe"},"periodSeconds":10,"timeoutSeconds":5,"failureThreshold":60},"readinessProbe":{"timeoutSeconds":5,"failureThreshold":10},"resources":{"limits":{"cpu":"1"}}}]}}}}'
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n kube-system rollout restart deployment/coredns
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n kube-system rollout status \
  deployment/coredns --timeout=300s

node_count="$(kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" get nodes -o name | wc -l | tr -d ' ')"
[[ "${node_count}" == "3" ]] || die "expected 3 simulator nodes, found ${node_count}"
worker_count="$(kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" get nodes \
  -l node-role.kubernetes.io/worker -o name | wc -l | tr -d ' ')"
[[ "${worker_count}" == "2" ]] || die "expected 2 explicitly labeled worker nodes, found ${worker_count}"
echo "ok local-production-create context=${LOCAL_PRODUCTION_CONTEXT} nodes=${node_count} workers=${worker_count}"
