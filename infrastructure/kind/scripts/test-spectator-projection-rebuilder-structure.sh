#!/usr/bin/env bash
# Offline structure checks for Spectator ADR-0039 projection-rebuilder wiring.
# Deterministic proof lane (not a live Kafka/Redis recovery claim).
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

SRC="${REPO_ROOT}/services/spectator-view/src"
CHART="${REPO_ROOT}/services/spectator-view/helm/spectator-view"
RENDER="${SCRIPT_DIR}/render-kafka-topics.rb"
PLAN="${REPO_ROOT}/infrastructure/kind/generated/kafka-topic-plan.yaml"
CREATE="${REPO_ROOT}/infrastructure/kind/generated/kafka-create-topics.sh"
LIVE="${SCRIPT_DIR}/test-spectator-projection-rebuilder-live.sh"

check "${SRC}/main.go" "workerRoleSpectatorProjectionRebuilder"
check "${SRC}/main.go" "wireProjectionRebuildWorker"
check "${SRC}/projection_rebuild_worker.go" "WORKER_ROLE"
check "${SRC}/projection_rebuild_worker.go" "ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL"
check "${SRC}/projection_rebuild_worker.go" "StoreBackedProjectionRebuildExecutor"
check "${SRC}/projection_rebuild_worker.go" "KafkaHeldSpectatorDLQSource"
check "${SRC}/projection_rebuild_worker.go" "NewRedisRebuildIdempotency"
check "${SRC}/kafka_held_source.go" "MaxScanRecords"
check "${SRC}/kafka_held_source.go" "MaxPollCycles"
check "${SRC}/kafka_held_source.go" "DefaultHeldDLQMaxScanRecords"
check "${LIVE}" 'for _ in $(seq 1 10)'
check "${LIVE}" '000|502|503|504'
check "${LIVE}" 'X-Command-Id: ${COMMAND_ID}'

check "${CHART}/templates/projection-rebuilder-deployment.yaml" "spectator-projection-rebuilder"
check "${CHART}/templates/projection-rebuilder-deployment.yaml" "KAFKA_PROJECTION_REBUILD_TOPIC"
check "${CHART}/templates/projection-rebuilder-deployment.yaml" "projectionRebuilder.secretEnv"
check "${CHART}/values.yaml" "ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL"
check "${CHART}/values.kind.yaml" "ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL"
! grep -q 'SPECTATOR_VIEW_INTERNAL_CREDENTIAL' "${CHART}/templates/projection-rebuilder-deployment.yaml" \
  || { echo "FAIL: rebuilder template must not inject API credential" >&2; fail=1; }
! grep -q 'readinessProbe' "${CHART}/templates/projection-rebuilder-deployment.yaml" \
  || { echo "FAIL: rebuilder template must not define readinessProbe" >&2; fail=1; }
grep -q 'containerPort: 9090' "${CHART}/templates/projection-rebuilder-deployment.yaml" \
  || { echo "FAIL: rebuilder template must expose private metrics port 9090" >&2; fail=1; }
! grep -q 'containerPort: 8080' "${CHART}/templates/projection-rebuilder-deployment.yaml" \
  || { echo "FAIL: rebuilder template must not expose application port 8080" >&2; fail=1; }

check "${CHART}/values.yaml" "enabled: false"
check "${CHART}/values.staging.yaml" "enabled: false"
check "${CHART}/values.production.yaml" "enabled: false"
# Kind enables the worker after the live request→snapshot→fenced swap→quarantine release proof.
if ! grep -A2 '^projectionRebuilder:' "${CHART}/values.kind.yaml" | grep -q 'enabled: true'; then
  echo "FAIL: values.kind.yaml must enable projectionRebuilder after live recovery proof" >&2
  fail=1
fi
check "${CHART}/values.kind.yaml" "spectator.projection.rebuild_requested"

check "${RENDER}" "KIND_REBUILD_REQUEST_PARTITIONS"
check "${RENDER}" "spectator.projection.rebuild_requested"
check "${RENDER}" "spectator.projection.rebuild_requested"

if [[ -f "${PLAN}" ]]; then
  check "${PLAN}" "spectator.projection.rebuild_requested"
  check "${PLAN}" "spectator.projection.rebuild_requested.spectator-view.dlq"
  ruby -e '
    require "yaml"
    plan = YAML.load_file(ARGV[0])
    domain = plan.dig("spec", "domainTopics") || []
    dlq = plan.dig("spec", "dlqTopics") || []
    t = domain.find { |x| x["name"] == "spectator.projection.rebuild_requested" }
    d = dlq.find { |x| x["name"] == "spectator.projection.rebuild_requested.spectator-view.dlq" }
    abort "FAIL: rebuild topic missing" unless t
    abort "FAIL: rebuild partitions=#{t["partitions"]} want 2" unless t["partitions"].to_i == 2
    abort "FAIL: rebuild RF=#{t["replicationFactor"]} want 1" unless t["replicationFactor"].to_i == 1
    abort "FAIL: rebuild DLQ missing" unless d
    abort "FAIL: rebuild DLQ partitions=#{d["partitions"]} want 2" unless d["partitions"].to_i == 2
  ' "${PLAN}" || fail=1
fi
if [[ -f "${CREATE}" ]]; then
  check "${CREATE}" 'create_or_assert_topic "spectator.projection.rebuild_requested" 2'
  check "${CREATE}" 'create_or_assert_topic "spectator.projection.rebuild_requested.spectator-view.dlq" 2'
fi

[[ "${fail}" -eq 0 ]] || exit 1
echo "ok spectator-projection-rebuilder-structure"
