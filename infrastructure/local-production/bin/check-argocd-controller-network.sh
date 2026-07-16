#!/usr/bin/env bash
# Prove the Ready Argo application controller can reach its required cluster services.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
assert_simulator_cluster_exists
assert_exact_context

for _attempt in $(seq 1 15); do
  if kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd exec \
    argocd-application-controller-0 -- timeout 5 bash -ec '
      getent hosts kubernetes.default.svc argocd-redis
      : >/dev/tcp/kubernetes.default.svc/443
      : >/dev/tcp/argocd-redis/6379
    '; then
    echo "ok local-production-argocd-controller-network context=${LOCAL_PRODUCTION_CONTEXT}"
    exit 0
  fi
  sleep 2
done

die "Argo application controller cannot reach DNS, Kubernetes API, and Redis"
