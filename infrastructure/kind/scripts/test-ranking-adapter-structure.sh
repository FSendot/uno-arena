#!/usr/bin/env bash
# Offline structure checks for Ranking kind deploy + adapter scripts.
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

DEPLOY="${SCRIPT_DIR}/deploy-ranking.sh"
LIVE="${SCRIPT_DIR}/test-ranking-adapter.sh"
PF="${SCRIPT_DIR}/port-forward-ranking.sh"
STRUCT="${SCRIPT_DIR}/test-ranking-adapter-structure.sh"
INTEG="${SCRIPT_DIR}/test-ranking-integration.sh"
INTEG_STRUCT="${SCRIPT_DIR}/test-ranking-integration-structure.sh"

for f in "${DEPLOY}" "${LIVE}" "${PF}" "${STRUCT}" "${INTEG}" "${INTEG_STRUCT}"; do
  [[ -f "${f}" ]] || { echo "FAIL: missing ${f}" >&2; fail=1; }
done

check "${DEPLOY}" "helm upgrade --install"
check "${DEPLOY}" "values.kind.yaml"
check "${DEPLOY}" "assert_kind_context"
! grep -qE 'type:\s*(NodePort|LoadBalancer)' "${DEPLOY}" || { echo "FAIL: deploy must not set public Service type" >&2; fail=1; }

check "${PF}" 'LOCAL_PORT_SPEC="0:'
check "${PF}" "jobs -pr"
check "${PF}" "trap cleanup EXIT"
check "${PF}" "port-forward --address=127.0.0.1"
check "${PF}" "RANKING_BASE_URL"

check "${LIVE}" "RANKING_BASE_URL"
check "${LIVE}" "outbox_events"
check "${LIVE}" "port-forward-ranking"

CHART="${REPO_ROOT}/services/ranking/helm/ranking"
[[ -f "${CHART}/values.kind.yaml" ]] || { echo "FAIL: missing values.kind.yaml" >&2; fail=1; }
[[ -f "${CHART}/helm-test.sh" ]] || { echo "FAIL: missing helm-test.sh" >&2; fail=1; }
[[ -f "${CHART}/templates/_helpers.tpl" ]] || { echo "FAIL: missing _helpers.tpl" >&2; fail=1; }

KIND_VALUES="${CHART}/values.kind.yaml"
check "${KIND_VALUES}" "KAFKA_BROKERS"
check "${KIND_VALUES}" "kafka.uno-arena.svc.cluster.local:9092"
check "${KIND_VALUES}" "KAFKA_CONSUMER_GROUP"
check "${KIND_VALUES}" "KAFKA_GAME_COMPLETED_TOPIC"
check "${KIND_VALUES}" "KAFKA_GAME_COMPLETED_DLQ_TOPIC"
check "${KIND_VALUES}" "KAFKA_PLAYERS_ADVANCED_TOPIC"
check "${KIND_VALUES}" "KAFKA_PLAYERS_ADVANCED_DLQ_TOPIC"
check "${KIND_VALUES}" "KAFKA_TOURNAMENT_COMPLETED_TOPIC"
check "${KIND_VALUES}" "KAFKA_TOURNAMENT_COMPLETED_DLQ_TOPIC"
! grep -qiE 'debezium.*PENDING|PENDING.*[Dd]ebezium' "${KIND_VALUES}" \
  || { echo "FAIL: values.kind must not claim Debezium PENDING" >&2; fail=1; }

SECRETS="${MANIFESTS_DIR}/01-local-secrets.yaml"
check "${SECRETS}" "ranking-database-url"

[[ "${fail}" -eq 0 ]] || exit 1
echo "ok ranking-adapter-structure"
