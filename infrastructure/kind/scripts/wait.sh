#!/usr/bin/env bash
# Wait for foundation Deployments and bootstrap Jobs. Requires live kind cluster.
# Prefer make kind-apply which already waits; this target re-checks the same conditions.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
assert_kind_context

TIMEOUT="${KIND_WAIT_TIMEOUT:-300s}"

deployments=(
  postgres-identity
  postgres-room-gameplay
  postgres-tournament
  postgres-ranking
  redis
  kafka
  kurrentdb
  clickhouse
  keycloak
)

for d in "${deployments[@]}"; do
  echo "waiting for deployment/${d}"
  kubectl -n "${KIND_NAMESPACE}" rollout status "deployment/${d}" --timeout="${TIMEOUT}"
done

jobs=(
  bootstrap-postgres-identity
  bootstrap-postgres-room-gameplay
  bootstrap-postgres-tournament
  bootstrap-postgres-ranking
  bootstrap-clickhouse-analytics
  bootstrap-kafka-topics
)

for j in "${jobs[@]}"; do
  echo "waiting for job/${j}"
  kubectl -n "${KIND_NAMESPACE}" wait --for=condition=complete "job/${j}" --timeout="${TIMEOUT}"
done

# Debezium Kafka Connect + connector registration Job (status only — no delivery claim).
echo "waiting for deployment/debezium-connect"
kubectl -n "${KIND_NAMESPACE}" rollout status "deployment/debezium-connect" --timeout="${TIMEOUT}"
echo "waiting for job/register-debezium-connectors"
kubectl -n "${KIND_NAMESPACE}" wait --for=condition=complete "job/register-debezium-connectors" --timeout="${TIMEOUT}"

# Debezium Server after bootstrap Jobs (Room realtime Redis sink). Status only — no delivery claim.
echo "waiting for deployment/debezium-server-room-realtime"
kubectl -n "${KIND_NAMESPACE}" rollout status "deployment/debezium-server-room-realtime" --timeout="${TIMEOUT}"

echo "ok kind-wait"
