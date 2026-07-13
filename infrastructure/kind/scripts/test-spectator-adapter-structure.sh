#!/usr/bin/env bash
# Offline structure checks for Spectator View kind deploy + adapter scripts.
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

DEPLOY="${SCRIPT_DIR}/deploy-spectator-view.sh"
LIVE="${SCRIPT_DIR}/test-spectator-adapter.sh"
PF="${SCRIPT_DIR}/port-forward-spectator-view.sh"
STRUCT="${SCRIPT_DIR}/test-spectator-adapter-structure.sh"
INTEG="${SCRIPT_DIR}/test-spectator-integration.sh"
INTEG_STRUCT="${SCRIPT_DIR}/test-spectator-integration-structure.sh"

for f in "${DEPLOY}" "${LIVE}" "${PF}" "${STRUCT}" "${INTEG}" "${INTEG_STRUCT}"; do
  [[ -f "${f}" ]] || { echo "FAIL: missing ${f}" >&2; fail=1; }
done

check "${DEPLOY}" "helm upgrade --install"
check "${DEPLOY}" "values.kind.yaml"
check "${DEPLOY}" "assert_kind_context"
check "${DEPLOY}" "PENDING"
! grep -qE 'type:\s*(NodePort|LoadBalancer)' "${DEPLOY}" || { echo "FAIL: deploy must not set public Service type" >&2; fail=1; }

check "${PF}" 'LOCAL_PORT_SPEC="0:'
check "${PF}" "jobs -pr"
check "${PF}" "trap cleanup EXIT"
check "${PF}" "port-forward --address=127.0.0.1"
check "${PF}" "SPECTATOR_BASE_URL"

check "${LIVE}" "SPECTATOR_BASE_URL"
check "${LIVE}" "port-forward-spectator-view"
check "${LIVE}" "PENDING"

CHART="${REPO_ROOT}/services/spectator-view/helm/spectator-view"
[[ -f "${CHART}/values.kind.yaml" ]] || { echo "FAIL: missing values.kind.yaml" >&2; fail=1; }
[[ -f "${CHART}/helm-test.sh" ]] || { echo "FAIL: missing helm-test.sh" >&2; fail=1; }
[[ -f "${CHART}/templates/_helpers.tpl" ]] || { echo "FAIL: missing _helpers.tpl" >&2; fail=1; }

check "${CHART}/values.kind.yaml" "REDIS_URL"
check "${CHART}/values.kind.yaml" "6379/5"
check "${CHART}/values.kind.yaml" "SPECTATOR_REDIS_STREAM_MAXLEN"
check "${CHART}/values.kind.yaml" "uno-arena-local-credentials"
check "${CHART}/values.kind.yaml" "KAFKA_BROKERS"
check "${CHART}/values.kind.yaml" "kafka.uno-arena.svc.cluster.local:9092"
check "${CHART}/values.kind.yaml" "KAFKA_CONSUMER_GROUP"
check "${CHART}/values.kind.yaml" "KAFKA_SPECTATOR_SAFE_TOPIC"
check "${CHART}/values.kind.yaml" "KAFKA_SPECTATOR_SAFE_DLQ_TOPIC"
check "${CHART}/values.kind.yaml" "projectionRebuilder"
# Kind enables the rebuilder after its live recovery proof.
if ! grep -A2 '^projectionRebuilder:' "${CHART}/values.kind.yaml" | grep -q 'enabled: true'; then
  echo "FAIL: values.kind.yaml must enable projectionRebuilder after live recovery proof" >&2
  fail=1
fi
check "${CHART}/values.kind.yaml" "ROOM_GAMEPLAY_URL"
check "${CHART}/values.kind.yaml" "ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL"
check "${CHART}/values.kind.yaml" "KAFKA_PROJECTION_REBUILD_TOPIC"
check "${CHART}/values.yaml" "enabled: false"

SECRETS="${MANIFESTS_DIR}/01-local-secrets.yaml"
check "${SECRETS}" "spectator-view-internal-credential"
check "${SECRETS}" "room-spectator-recovery-service-credential"

# Kind render includes the proven rebuilder; privilege checks remain least-privilege.
if command -v helm >/dev/null 2>&1; then
  kind_out="$(helm template spectator-kind "${CHART}" -f "${CHART}/values.kind.yaml")"
  echo "${kind_out}" | grep -q 'spectator-projection-rebuilder' \
    || { echo "FAIL: kind render must include projection-rebuilder after live recovery proof" >&2; fail=1; }
  reb_out="$(helm template spectator-kind-rebuilder "${CHART}" -f "${CHART}/values.kind.yaml" --set projectionRebuilder.enabled=true)"
  echo "${reb_out}" | grep -q 'WORKER_ROLE'
  echo "${reb_out}" | grep -q 'spectator-projection-rebuilder'
  echo "${reb_out}" | grep -q 'ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL'
  ! echo "${reb_out}" | awk '/name: projection-rebuilder/,/^[[:space:]]*- name: /' | grep -q 'SPECTATOR_VIEW_INTERNAL_CREDENTIAL' \
    || { echo "FAIL: rebuilder must not receive SPECTATOR_VIEW_INTERNAL_CREDENTIAL" >&2; fail=1; }
  ! echo "${reb_out}" | awk '/name: projection-rebuilder/,/resources:/' | grep -q 'readinessProbe' \
    || { echo "FAIL: rebuilder must not define readinessProbe" >&2; fail=1; }
  ! echo "${reb_out}" | awk '/name: projection-rebuilder/,/resources:/' | grep -q 'containerPort' \
    || { echo "FAIL: rebuilder must not expose HTTP ports" >&2; fail=1; }
fi

[[ "${fail}" -eq 0 ]] || exit 1
echo "ok spectator-adapter-structure"
