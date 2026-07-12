#!/usr/bin/env bash
# Offline structure checks for Gateway kind deploy + adapter scripts.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

fail=0
check() {
  local file="$1"
  local needle="$2"
  if ! grep -qF "${needle}" "${file}"; then
    echo "FAIL: ${file} missing ${needle}" >&2
    fail=1
  fi
}

DEPLOY="${SCRIPT_DIR}/deploy-gateway.sh"
LIVE="${SCRIPT_DIR}/test-gateway-adapter.sh"
PF="${SCRIPT_DIR}/port-forward-gateway.sh"
STRUCT="${SCRIPT_DIR}/test-gateway-adapter-structure.sh"
INTEG="${SCRIPT_DIR}/test-gateway-integration.sh"
INTEG_STRUCT="${SCRIPT_DIR}/test-gateway-integration-structure.sh"

for f in "${DEPLOY}" "${LIVE}" "${PF}" "${STRUCT}" "${INTEG}" "${INTEG_STRUCT}"; do
  [[ -f "${f}" ]] || { echo "FAIL: missing ${f}" >&2; fail=1; }
done

check "${DEPLOY}" "helm upgrade --install"
check "${DEPLOY}" "values.kind.yaml"
check "${DEPLOY}" "assert_kind_context"
check "${DEPLOY}" "SessionInvalidated"
! grep -qiE 'debezium.*PENDING|PENDING.*[Dd]ebezium|Kafka/Debezium remain PENDING' "${DEPLOY}" \
  || { echo "FAIL: deploy must not claim Debezium/Kafka PENDING" >&2; fail=1; }
# Room realtime Debezium Server → Redis player-feed pipeline is implemented.
! grep -qE 'type:\s*(NodePort|LoadBalancer)' "${DEPLOY}" || { echo "FAIL: deploy must not set public Service type" >&2; fail=1; }

check "${PF}" 'LOCAL_PORT_SPEC="0:'
check "${PF}" "jobs -pr"
check "${PF}" "trap cleanup EXIT"
check "${PF}" "port-forward --address=127.0.0.1"
check "${PF}" "GATEWAY_BASE_URL"

check "${LIVE}" "GATEWAY_BASE_URL"
check "${LIVE}" "port-forward-gateway"
! grep -qiE 'debezium.*PENDING|PENDING.*[Dd]ebezium|Kafka/Debezium remain PENDING' "${LIVE}" \
  || { echo "FAIL: adapter live must not claim Debezium/Kafka PENDING" >&2; fail=1; }

SI_LIVE="${SCRIPT_DIR}/test-gateway-si-redis-admission-live.sh"
SI_ALIAS="${SCRIPT_DIR}/test-gateway-session-invalidation-live.sh"
[[ -f "${SI_LIVE}" ]] || { echo "FAIL: missing ${SI_LIVE}" >&2; fail=1; }
[[ -f "${SI_ALIAS}" ]] || { echo "FAIL: missing ${SI_ALIAS}" >&2; fail=1; }
check "${SI_LIVE}" "cross-Hub admission only"
check "${SI_LIVE}" "not Kafka"
check "${SI_LIVE}" "Redis DB6 durable SI"

CHART="${REPO_ROOT}/services/gateway/helm/gateway"
[[ -f "${CHART}/values.kind.yaml" ]] || { echo "FAIL: missing values.kind.yaml" >&2; fail=1; }
[[ -f "${CHART}/helm-test.sh" ]] || { echo "FAIL: missing helm-test.sh" >&2; fail=1; }
[[ -f "${CHART}/templates/_helpers.tpl" ]] || { echo "FAIL: missing _helpers.tpl" >&2; fail=1; }

check "${CHART}/values.kind.yaml" "REDIS_URL"
check "${CHART}/values.kind.yaml" "6379/6"
check "${CHART}/values.kind.yaml" "GATEWAY_PLAYER_FEED_REDIS_URL"
check "${CHART}/values.kind.yaml" "6379/2"
check "${CHART}/values.kind.yaml" "GATEWAY_SPECTATOR_REDIS_URL"
check "${CHART}/values.kind.yaml" "6379/5"
check "${CHART}/values.kind.yaml" "uno-arena-local-credentials"
check "${CHART}/values.kind.yaml" "GATEWAY_IDENTITY_SERVICE_CREDENTIAL"
check "${CHART}/values.kind.yaml" "GATEWAY_ROOM_SERVICE_CREDENTIAL"
check "${CHART}/values.kind.yaml" "GATEWAY_TOURNAMENT_SERVICE_CREDENTIAL"
check "${CHART}/values.kind.yaml" "GATEWAY_SPECTATOR_SERVICE_CREDENTIAL"
check "${CHART}/values.kind.yaml" "identity-internal-credential"
check "${CHART}/values.kind.yaml" "room-service-credential"
check "${CHART}/values.kind.yaml" "tournament-internal-credential"
check "${CHART}/values.kind.yaml" "spectator-view-internal-credential"
! grep -qE '^[[:space:]]*GATEWAY_CAPABILITY_MODE:' "${CHART}/values.kind.yaml" || { echo "FAIL: kind must not set capability mode" >&2; fail=1; }
! grep -qE '^[[:space:]]*GATEWAY_ALLOW_FAKES:' "${CHART}/values.kind.yaml" || { echo "FAIL: kind must not set allow fakes" >&2; fail=1; }
check "${CHART}/values.kind.yaml" "KAFKA_BROKERS"
check "${CHART}/values.kind.yaml" "kafka.uno-arena.svc.cluster.local:9092"
check "${CHART}/values.kind.yaml" "KAFKA_CONSUMER_GROUP"
check "${CHART}/values.kind.yaml" "gateway"
check "${CHART}/values.kind.yaml" "KAFKA_SESSION_INVALIDATED_TOPIC"
check "${CHART}/values.kind.yaml" "identity.session.invalidated"
check "${CHART}/values.kind.yaml" "KAFKA_SESSION_INVALIDATED_DLQ_TOPIC"
check "${CHART}/values.kind.yaml" "identity.session.invalidated.gateway.dlq"
check "${CHART}/values.kind.yaml" "GATEWAY_SESSION_INVALIDATION_TTL"
check "${CHART}/values.kind.yaml" 'GATEWAY_SESSION_INVALIDATION_TTL: "7h"'
check "${CHART}/values.kind.yaml" "ADR-0029"
check "${CHART}/values.kind.yaml" "6h"
! grep -qiE 'debezium.*PENDING|PENDING.*[Dd]ebezium' "${CHART}/values.kind.yaml" \
  || { echo "FAIL: values.kind must not claim Debezium PENDING" >&2; fail=1; }
! grep -qiE 'debezium.*PENDING|PENDING.*[Dd]ebezium' "${CHART}/values.yaml" \
  || { echo "FAIL: values.yaml must not claim Debezium PENDING" >&2; fail=1; }

SECRETS="${MANIFESTS_DIR}/01-local-secrets.yaml"
check "${SECRETS}" "identity-internal-credential"
check "${SECRETS}" "room-service-credential"
check "${SECRETS}" "room-to-gateway-credential"
check "${SECRETS}" "tournament-internal-credential"
check "${SECRETS}" "spectator-view-internal-credential"

[[ "${fail}" -eq 0 ]] || exit 1
echo "ok gateway-adapter-structure"
