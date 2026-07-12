#!/usr/bin/env bash
# Explicit target: apply foundation manifests + generated Kafka topic Job.
# Order: namespace/secrets/datastores → wait datastore readiness → bootstrap/topic Jobs → wait Jobs.
# Job backoff is not consumed by datastore startup.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
require_cmd ruby
assert_kind_context

ruby "${SCRIPT_DIR}/render-kafka-topics.rb" >/dev/null

DATASTORE_TIMEOUT="${KIND_DATASTORE_TIMEOUT:-300s}"
JOB_TIMEOUT="${KIND_JOB_TIMEOUT:-300s}"

kubectl apply -f "${MANIFESTS_DIR}/00-namespace.yaml"
kubectl apply -f "${MANIFESTS_DIR}/01-local-secrets.yaml"
kubectl apply -f "${MANIFESTS_DIR}/10-postgres"
kubectl apply -f "${MANIFESTS_DIR}/20-redis"
kubectl apply -f "${MANIFESTS_DIR}/30-kafka"
kubectl apply -f "${MANIFESTS_DIR}/40-kurrentdb"
kubectl apply -f "${MANIFESTS_DIR}/50-clickhouse"
# Realm JSON is embedded in the ConfigMap; do not apply the directory (kubectl would treat realm-unoarena.json as a K8s object).
kubectl apply -f "${MANIFESTS_DIR}/60-keycloak/keycloak.yaml"

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
  echo "waiting for datastore deployment/${d} (timeout=${DATASTORE_TIMEOUT})"
  kubectl -n "${KIND_NAMESPACE}" rollout status "deployment/${d}" --timeout="${DATASTORE_TIMEOUT}"
done

kubectl apply -f "${MANIFESTS_DIR}/70-bootstrap"
kubectl apply -f "${GENERATED_DIR}/job-kafka-topics.yaml"

jobs=(
  bootstrap-postgres-identity
  bootstrap-postgres-room-gameplay
  bootstrap-postgres-tournament
  bootstrap-postgres-ranking
  bootstrap-clickhouse-analytics
  bootstrap-kafka-topics
)

for j in "${jobs[@]}"; do
  echo "waiting for bootstrap job/${j} (timeout=${JOB_TIMEOUT})"
  kubectl -n "${KIND_NAMESPACE}" wait --for=condition=complete "job/${j}" --timeout="${JOB_TIMEOUT}"
done

# Debezium Kafka Connect after Kafka + Postgres bootstrap (connect-* topics + CDC pubs).
# Requires image loaded via make kind-load-debezium-connect. Does not claim live CDC delivery.
# Delete completed registration Job so config changes re-run idempotently (Jobs are immutable).
kubectl -n "${KIND_NAMESPACE}" delete job/register-debezium-connectors --ignore-not-found=true
kubectl apply -f "${MANIFESTS_DIR}/80-debezium"
echo "waiting for deployment/debezium-connect (timeout=${DATASTORE_TIMEOUT})"
kubectl -n "${KIND_NAMESPACE}" rollout status "deployment/debezium-connect" --timeout="${DATASTORE_TIMEOUT}"
echo "waiting for job/register-debezium-connectors (timeout=${JOB_TIMEOUT})"
kubectl -n "${KIND_NAMESPACE}" wait --for=condition=complete "job/register-debezium-connectors" --timeout="${JOB_TIMEOUT}"

# Debezium Server (Room realtime → Redis) after Redis + Room Postgres bootstrap.
# Requires image loaded via make kind-load-debezium-server. Does not claim live CDC delivery.
# ConfigMap apply does not bump the Deployment pod template; restart so Server reloads
# mounted startup config (namespace-scoped; does not restart unrelated Deployments).
kubectl apply -f "${MANIFESTS_DIR}/80-debezium-server"
echo "restarting deployment/debezium-server-room-realtime (reload mounted ConfigMap)"
kubectl -n "${KIND_NAMESPACE}" rollout restart "deployment/debezium-server-room-realtime"
echo "waiting for deployment/debezium-server-room-realtime (timeout=${DATASTORE_TIMEOUT})"
kubectl -n "${KIND_NAMESPACE}" rollout status "deployment/debezium-server-room-realtime" --timeout="${DATASTORE_TIMEOUT}"

echo "ok kind-apply namespace=${KIND_NAMESPACE}"
