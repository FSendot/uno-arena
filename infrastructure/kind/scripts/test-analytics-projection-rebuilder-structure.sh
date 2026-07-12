#!/usr/bin/env bash
# Offline structure checks for Analytics ADR-0039 projection-rebuilder wiring.
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

SRC="${REPO_ROOT}/services/analytics/src"
CHART="${REPO_ROOT}/services/analytics/helm/analytics"
MIG="${REPO_ROOT}/services/analytics/migrations/001_init.sql"
RENDER="${SCRIPT_DIR}/render-kafka-topics.rb"
PLAN="${REPO_ROOT}/infrastructure/kind/generated/kafka-topic-plan.yaml"
CREATE="${REPO_ROOT}/infrastructure/kind/generated/kafka-create-topics.sh"

check "${SRC}/main.go" "workerRoleAnalyticsProjectionRebuilder"
check "${SRC}/main.go" "wireProjectionRebuildWorker"
check "${SRC}/projection_rebuild_worker.go" "WORKER_ROLE"
check "${SRC}/projection_rebuild_worker.go" "ANALYTICS_ROOM_CREDENTIAL"
check "${SRC}/projection_rebuild_worker.go" "must not receive ANALYTICS_OPS_CREDENTIAL"
check "${SRC}/kafka_rebuild_parse.go" "AnalyticsProjectionRebuildRequested"
check "${SRC}/kafka_rebuild_consumer.go" "failureToDLQAndCommit"
check "${SRC}/store/recovery.go" "ListWriteGenerations"
check "${SRC}/store/recovery.go" "CloneActiveGenerationInto"
check "${MIG}" "recovery_jobs"
	check "${MIG}" "recovery_leases"
check "${MIG}" "ORDER BY (recovery_job_id, owner_token)"
check "${MIG}" "recovery_page_checkpoints"
check "${MIG}" "recovery_request_idempotency"

check "${CHART}/templates/projection-rebuilder-deployment.yaml" "analytics-projection-rebuilder"
check "${CHART}/templates/projection-rebuilder-deployment.yaml" "KAFKA_PROJECTION_REBUILD_TOPIC"
check "${CHART}/templates/projection-rebuilder-deployment.yaml" "projectionRebuilder.secretEnv"
check "${CHART}/templates/projection-rebuilder-deployment.yaml" "ROOM_GAMEPLAY_URL"
check "${CHART}/templates/projection-rebuilder-deployment.yaml" "TOURNAMENT_ORCHESTRATION_URL"
check "${CHART}/templates/projection-rebuilder-deployment.yaml" "RANKING_URL"
! grep -q 'ANALYTICS_OPS_CREDENTIAL' "${CHART}/templates/projection-rebuilder-deployment.yaml" \
  || { echo "FAIL: rebuilder template must not inject ANALYTICS_OPS_CREDENTIAL" >&2; fail=1; }
! grep -q 'readinessProbe' "${CHART}/templates/projection-rebuilder-deployment.yaml" \
  || { echo "FAIL: rebuilder template must not define readinessProbe" >&2; fail=1; }
! grep -q 'containerPort' "${CHART}/templates/projection-rebuilder-deployment.yaml" \
  || { echo "FAIL: rebuilder template must not expose ports" >&2; fail=1; }

check "${CHART}/values.yaml" "enabled: false"
check "${CHART}/values.staging.yaml" "enabled: false"
check "${CHART}/values.production.yaml" "enabled: false"
if ! grep -A2 '^projectionRebuilder:' "${CHART}/values.kind.yaml" | grep -q 'enabled: false'; then
  echo "FAIL: values.kind.yaml must keep projectionRebuilder.enabled=false" >&2
  fail=1
fi
check "${CHART}/values.kind.yaml" "analytics.projection.rebuild_requested"

check "${RENDER}" "KIND_REBUILD_REQUEST_PARTITIONS"
check "${RENDER}" "analytics.projection.rebuild_requested"
check "${RENDER}" 'analytics.projection.rebuild_requested", "consumer" => "analytics"'

if [[ -f "${PLAN}" ]]; then
  check "${PLAN}" "analytics.projection.rebuild_requested"
  check "${PLAN}" "analytics.projection.rebuild_requested.analytics.dlq"
  ruby -e '
    require "yaml"
    plan = YAML.load_file(ARGV[0])
    domain = plan.dig("spec", "domainTopics") || []
    dlq = plan.dig("spec", "dlqTopics") || []
    t = domain.find { |x| x["name"] == "analytics.projection.rebuild_requested" }
    d = dlq.find { |x| x["name"] == "analytics.projection.rebuild_requested.analytics.dlq" }
    abort "FAIL: rebuild topic missing" unless t
    abort "FAIL: rebuild partitions=#{t["partitions"]} want 32" unless t["partitions"].to_i == 32
    abort "FAIL: rebuild RF=#{t["replicationFactor"]} want 1" unless t["replicationFactor"].to_i == 1
    abort "FAIL: rebuild DLQ missing" unless d
    abort "FAIL: rebuild DLQ partitions=#{d["partitions"]} want 32" unless d["partitions"].to_i == 32
  ' "${PLAN}" || fail=1
fi
if [[ -f "${CREATE}" ]]; then
  check "${CREATE}" 'create_or_assert_topic "analytics.projection.rebuild_requested" 32'
  check "${CREATE}" 'create_or_assert_topic "analytics.projection.rebuild_requested.analytics.dlq" 32'
fi

if command -v helm >/dev/null 2>&1; then
  reb_out="$(helm template analytics-kind-rebuilder "${CHART}" -f "${CHART}/values.kind.yaml" --set projectionRebuilder.enabled=true)"
  echo "${reb_out}" | grep -q 'analytics-projection-rebuilder'
  echo "${reb_out}" | grep -q 'analytics.projection.rebuild_requested'
  echo "${reb_out}" | grep -q 'ANALYTICS_ROOM_CREDENTIAL'
  # Isolate rebuilder Deployment YAML for least-privilege assertions.
  reb_dep="$(echo "${reb_out}" | awk '/name: analytics-kind-rebuilder-projection-rebuilder$/{p=1} p; /^---$/{if(p&&seen++){exit}}')"
  echo "${reb_dep}" | grep -q 'WORKER_ROLE'
  ! echo "${reb_dep}" | grep -q 'ANALYTICS_OPS_CREDENTIAL' \
    || { echo "FAIL: rendered rebuilder must not include ops credential" >&2; fail=1; }
  ! echo "${reb_dep}" | grep -q 'containerPort' \
    || { echo "FAIL: rendered rebuilder must not expose ports" >&2; fail=1; }
  ! echo "${reb_dep}" | grep -q 'readinessProbe' \
    || { echo "FAIL: rendered rebuilder must not define readinessProbe" >&2; fail=1; }
fi

[[ "${fail}" -eq 0 ]] || exit 1
echo "ok analytics-projection-rebuilder-structure"
