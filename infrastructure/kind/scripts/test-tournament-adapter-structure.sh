#!/usr/bin/env bash
# Offline structure checks for Tournament Orchestration kind deploy + adapter scripts.
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

DEPLOY="${SCRIPT_DIR}/deploy-tournament-orchestration.sh"
LIVE="${SCRIPT_DIR}/test-tournament-adapter.sh"
PF="${SCRIPT_DIR}/port-forward-tournament-orchestration.sh"
STRUCT="${SCRIPT_DIR}/test-tournament-adapter-structure.sh"
INTEG="${SCRIPT_DIR}/test-tournament-integration.sh"
INTEG_STRUCT="${SCRIPT_DIR}/test-tournament-integration-structure.sh"

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
check "${PF}" "TOURNAMENT_BASE_URL"

check "${LIVE}" "TOURNAMENT_BASE_URL"
check "${LIVE}" "outbox_events"
check "${LIVE}" "port-forward-tournament"

CHART="${REPO_ROOT}/services/tournament-orchestration/helm/tournament-orchestration"
[[ -f "${CHART}/values.kind.yaml" ]] || { echo "FAIL: missing values.kind.yaml" >&2; fail=1; }
[[ -f "${CHART}/helm-test.sh" ]] || { echo "FAIL: missing helm-test.sh" >&2; fail=1; }
[[ -f "${CHART}/templates/_helpers.tpl" ]] || { echo "FAIL: missing _helpers.tpl" >&2; fail=1; }

KIND_VALUES="${CHART}/values.kind.yaml"
check "${KIND_VALUES}" "KAFKA_BROKERS"
check "${KIND_VALUES}" "kafka.uno-arena.svc.cluster.local:9092"
check "${KIND_VALUES}" "KAFKA_CONSUMER_GROUP"
check "${KIND_VALUES}" "KAFKA_MATCH_COMPLETED_TOPIC"
check "${KIND_VALUES}" "KAFKA_MATCH_COMPLETED_DLQ_TOPIC"
check "${KIND_VALUES}" "room.match.completed.tournament-orchestration.dlq"
check "${KIND_VALUES}" "KAFKA_ROOM_RUNTIME_READY_TOPIC"
check "${KIND_VALUES}" "KAFKA_ROOM_RUNTIME_READY_DLQ_TOPIC"
check "${KIND_VALUES}" "room.runtime.ready.tournament-orchestration.dlq"

SECRETS="${MANIFESTS_DIR}/01-local-secrets.yaml"
check "${SECRETS}" "tournament-database-url"

[[ "${fail}" -eq 0 ]] || exit 1
echo "ok tournament-adapter-structure"
