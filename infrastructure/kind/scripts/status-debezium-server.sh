#!/usr/bin/env bash
# Status for Debezium Server (Room realtime → Redis). Read-only.
# Reports Deployment/Pod/health readiness only — does NOT claim Postgres→Redis delivery.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
assert_kind_context

NAME=debezium-server-room-realtime
NS="${KIND_NAMESPACE}"

echo "=== deployment/${NAME} ==="
kubectl -n "${NS}" get deployment "${NAME}" -o wide 2>/dev/null || {
  echo "deployment/${NAME} not found"
  exit 1
}

echo
echo "=== pods ==="
kubectl -n "${NS}" get pods -l "app.kubernetes.io/name=${NAME}" -o wide

echo
echo "=== rollout ==="
kubectl -n "${NS}" rollout status "deployment/${NAME}" --timeout=30s || true

echo
echo "=== quarkus health (in-pod; readiness only, not CDC delivery) ==="
POD="$(kubectl -n "${NS}" get pods -l "app.kubernetes.io/name=${NAME}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
if [[ -n "${POD}" ]]; then
  kubectl -n "${NS}" exec "${POD}" -- curl -sf "http://127.0.0.1:8080/q/health/ready" && echo || echo "(health probe unavailable)"
else
  echo "(no pod)"
fi

echo
echo "ok status-debezium-server (no live delivery claim)"
