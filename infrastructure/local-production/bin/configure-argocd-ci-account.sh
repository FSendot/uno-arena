#!/usr/bin/env bash
# Operator-only configuration of the cross-project, read-only GitLab CI API account.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
assert_simulator_cluster_exists
assert_exact_context

kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd patch configmap argocd-cm \
  --type=merge \
  -p='{"data":{"accounts.ci-readonly":"apiKey","accounts.ci-readonly.enabled":"true"}}' >/dev/null
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd patch configmap argocd-rbac-cm \
  --type=merge \
  -p='{"data":{"policy.csv":"p, ci-readonly, applications, get, uno-arena-bootstrap/*, allow\np, ci-readonly, applications, get, uno-arena-foundations/*, allow\np, ci-readonly, applications, get, uno-arena-workloads/*, allow\np, ci-readonly, applications, get, uno-arena-stateful-platform/*, allow\n"}}' >/dev/null

kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd rollout restart deployment/argocd-server >/dev/null
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd rollout status \
  deployment/argocd-server --timeout=300s
echo "ok local-production-argocd-ci-account context=${LOCAL_PRODUCTION_CONTEXT}"
