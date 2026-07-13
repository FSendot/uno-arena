#!/usr/bin/env bash
# Offline assertions for the clean kind build/deploy/probe orchestration.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

build="${SCRIPT_DIR}/build-load-services.sh"
deploy="${SCRIPT_DIR}/deploy-services.sh"
probes="${SCRIPT_DIR}/run-live-probes.sh"
clean="${SCRIPT_DIR}/clean-deploy.sh"

for file in "${build}" "${deploy}" "${probes}" "${clean}" "${SCRIPT_DIR}/deploy-game-integrity.sh"; do
  [[ -f "${file}" ]] || { echo "FAIL: missing ${file}" >&2; exit 1; }
  bash -n "${file}"
done

services=(analytics game-integrity gateway identity ranking room-gameplay spectator-view tournament-orchestration)
for service in "${services[@]}"; do
  grep -Fq "  ${service}" "${build}" || { echo "FAIL: build lane omits ${service}" >&2; exit 1; }
done

grep -Fq 'unsupported: the selected local KurrentDB image is ARM64-only' "${build}" || {
  echo "FAIL: service builder must fail closed for non-ARM64 kind nodes" >&2
  exit 1
}
grep -Fq -- '--confirm-reset uno-arena' "${clean}" || { echo "FAIL: reset confirmation missing" >&2; exit 1; }
grep -Fq "docker info --format '{{.Architecture}}'" "${clean}" || { echo "FAIL: pre-reset architecture preflight missing" >&2; exit 1; }
grep -Fq 'load-debezium-connect.sh' "${clean}" || { echo "FAIL: Connect image staging missing" >&2; exit 1; }
grep -Fq 'load-debezium-server.sh' "${clean}" || { echo "FAIL: Server image staging missing" >&2; exit 1; }
grep -Fq 'run-live-probes.sh' "${clean}" || { echo "FAIL: live probes missing" >&2; exit 1; }
grep -Fq 'test-room-integration.sh' "${probes}" || { echo "FAIL: Room live integration probe missing" >&2; exit 1; }
grep -Fq 'test-analytics-projection-rebuilder-live.sh' "${probes}" || { echo "FAIL: Analytics recovery live probe missing" >&2; exit 1; }
grep -Fq 'test-spectator-projection-rebuilder-live.sh' "${probes}" || { echo "FAIL: Spectator recovery live probe missing" >&2; exit 1; }
grep -Fq 'bash "${SCRIPT_DIR}/${probe}"' "${probes}" || {
  echo "FAIL: probe runner must support checked-in non-executable scripts" >&2
  exit 1
}

plan="$(${clean} --dry-run)"
first_plan="$(head -n 1 <<<"${plan}")"
grep -Fq '/validate.sh' <<<"${first_plan}" || { echo "FAIL: offline validation must precede reset" >&2; exit 1; }
grep -Fq '/reset.sh' <<<"${plan}" || { echo "FAIL: dry-run omits reset" >&2; exit 1; }
grep -Fq '/deploy-services.sh' <<<"${plan}" || { echo "FAIL: dry-run omits service deploy" >&2; exit 1; }
grep -Fq '/run-live-probes.sh' <<<"${plan}" || { echo "FAIL: dry-run omits probes" >&2; exit 1; }

echo "ok kind-clean-deployment-structure services=${#services[@]}"
