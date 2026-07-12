#!/usr/bin/env bash
# EXPLICIT/NETWORKED: query Kafka Connect REST for the four outbox connectors.
# Verifies connector registration/status in kind — does NOT alone claim end-to-end
# Postgres→Kafka durable delivery (no outbox insert + topic consume proof here).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
assert_kind_context

die() {
  echo "FAIL: $*" >&2
  exit 1
}

EXPECTED=(
  identity-outbox
  room-integration-outbox
  tournament-outbox
  ranking-outbox
)

echo "checking deployment/debezium-connect ready"
kubectl -n "${KIND_NAMESPACE}" rollout status deployment/debezium-connect --timeout=120s

echo "listing connectors via Connect REST (in-cluster curl)"
listed="$(
  kubectl -n "${KIND_NAMESPACE}" exec deploy/debezium-connect -- \
    curl -sf http://127.0.0.1:8083/connectors
)"

python3 - "${listed}" <<'PY'
import json, sys
expected = {
    "identity-outbox",
    "room-integration-outbox",
    "tournament-outbox",
    "ranking-outbox",
}
names = set(json.loads(sys.argv[1]))
missing = expected - names
extras = names - expected
if missing:
    raise SystemExit(f"missing connectors: {sorted(missing)}")
if extras:
    raise SystemExit(f"unexpected connectors: {sorted(extras)}")
if len(names) != 4:
    raise SystemExit(f"expected exactly 4 connectors, got {len(names)}")
print("ok connector set")
PY

for name in "${EXPECTED[@]}"; do
  echo "status ${name}"
  status="$(
    kubectl -n "${KIND_NAMESPACE}" exec deploy/debezium-connect -- \
      curl -sf "http://127.0.0.1:8083/connectors/${name}/status"
  )"
  python3 - "${name}" "${status}" <<'PY'
import json, sys
name = sys.argv[1]
status = json.loads(sys.argv[2])
connector = status.get("connector") or {}
cstate = connector.get("state")
tasks = status.get("tasks") or []
if cstate != "RUNNING":
    raise SystemExit(f"{name} connector state={cstate!r} body={status}")
for t in tasks:
    if t.get("state") != "RUNNING":
        raise SystemExit(f"{name} task not RUNNING: {t}")
if not tasks:
    raise SystemExit(f"{name} has no tasks: {status}")
print(f"ok {name} RUNNING tasks={len(tasks)}")
PY
done

echo "ok kind-test-debezium-connectors (Connect status only; no delivery claim)"
