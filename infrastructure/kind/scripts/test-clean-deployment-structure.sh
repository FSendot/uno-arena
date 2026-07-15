#!/usr/bin/env bash
# Offline assertions for the clean kind build/deploy/probe orchestration.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

build="${SCRIPT_DIR}/build-load-services.sh"
deploy="${SCRIPT_DIR}/deploy-services.sh"
probes="${SCRIPT_DIR}/run-live-probes.sh"
clean="${SCRIPT_DIR}/clean-deploy.sh"

capacity="${SCRIPT_DIR}/preflight-host-capacity.sh"
for file in "${build}" "${deploy}" "${probes}" "${clean}" "${capacity}" "${SCRIPT_DIR}/deploy-game-integrity.sh" "${SCRIPT_DIR}/verify-portable-images.sh"; do
  [[ -f "${file}" ]] || { echo "FAIL: missing ${file}" >&2; exit 1; }
  bash -n "${file}"
done

services=(analytics game-integrity gateway identity ranking room-gameplay spectator-view tournament-orchestration)
for service in "${services[@]}"; do
  grep -Fq "  ${service}" "${build}" || { echo "FAIL: build lane omits ${service}" >&2; exit 1; }
done

grep -Fq 'x86_64|amd64) platform="linux/amd64"' "${build}" || {
  echo "FAIL: service builder must support reviewed AMD64 nodes" >&2
  exit 1
}
grep -Fq 'aarch64|arm64) platform="linux/arm64"' "${build}" || {
  echo "FAIL: service builder must support reviewed ARM64 nodes" >&2
  exit 1
}
grep -Fq -- '--confirm-reset uno-arena' "${clean}" || { echo "FAIL: reset confirmation missing" >&2; exit 1; }
grep -Fq "docker info --format '{{.Architecture}}'" "${clean}" || { echo "FAIL: pre-reset architecture preflight missing" >&2; exit 1; }
grep -Fq "has no reviewed KurrentDB 26.0.3 image" "${clean}" || { echo "FAIL: unknown architecture must fail closed" >&2; exit 1; }
grep -Fq '__KURRENTDB_IMAGE_BY_NODE_ARCH__' "${SCRIPT_DIR}/../manifests/40-kurrentdb/kurrentdb.yaml" || {
  echo "FAIL: KurrentDB manifest must defer reviewed image selection to apply.sh" >&2
  exit 1
}
for digest in b4d0665a78269cd7184971c4d1fad38265277901f3d3730d89dcfba8f3d37fe9 8498556a8ba7a74f8d4ea31a149b1e5216e167d6884b630a68b3e1eb9e6e870e; do
  grep -Fq "${digest}" "${SCRIPT_DIR}/apply.sh" || { echo "FAIL: apply.sh missing reviewed KurrentDB digest ${digest}" >&2; exit 1; }
done
grep -Fq 'load-debezium-connect.sh' "${clean}" || { echo "FAIL: Connect image staging missing" >&2; exit 1; }
grep -Fq 'load-debezium-server.sh' "${clean}" || { echo "FAIL: Server image staging missing" >&2; exit 1; }
grep -Fq 'run-live-probes.sh' "${clean}" || { echo "FAIL: live probes missing" >&2; exit 1; }
grep -Fq 'test-client-parity-live.sh' "${probes}" || { echo "FAIL: live client parity probe missing" >&2; exit 1; }
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
portable_line="$(grep -n '/verify-portable-images.sh' <<<"${plan}" | cut -d: -f1)"
reset_line="$(grep -n '/reset.sh' <<<"${plan}" | cut -d: -f1)"
(( portable_line < reset_line )) || { echo "FAIL: portable index preflight must precede reset" >&2; exit 1; }
grep -Fq '/reset.sh' <<<"${plan}" || { echo "FAIL: dry-run omits reset" >&2; exit 1; }
capacity_line="$(grep -n '/preflight-host-capacity.sh' <<<"${plan}" | cut -d: -f1)"
create_line="$(grep -n '/create-cluster.sh' <<<"${plan}" | cut -d: -f1)"
(( reset_line < capacity_line && capacity_line < create_line )) || {
  echo "FAIL: host capacity preflight must run after reset and before create" >&2
  exit 1
}
grep -Fq 'KIND_MIN_HOST_FREE_GIB:-20' "${capacity}" || { echo "FAIL: 20 GiB capacity floor missing" >&2; exit 1; }
grep -Fq 'KIND_MIN_ENGINE_MEMORY_GIB:-7' "${capacity}" || { echo "FAIL: 7 GiB engine-memory floor missing" >&2; exit 1; }
if KIND_MIN_HOST_FREE_GIB=19 KIND_MIN_ENGINE_MEMORY_GIB=7 "${capacity}" >/dev/null 2>&1; then
  echo "FAIL: host capacity override must not lower the 20 GiB floor" >&2
  exit 1
fi
if KIND_MIN_HOST_FREE_GIB=20 KIND_MIN_ENGINE_MEMORY_GIB=6 "${capacity}" >/dev/null 2>&1; then
  echo "FAIL: engine-memory override must not lower the 7 GiB floor" >&2
  exit 1
fi
grep -Fq '/deploy-services.sh' <<<"${plan}" || { echo "FAIL: dry-run omits service deploy" >&2; exit 1; }
grep -Fq '/run-live-probes.sh' <<<"${plan}" || { echo "FAIL: dry-run omits probes" >&2; exit 1; }

echo "ok kind-clean-deployment-structure services=${#services[@]}"
