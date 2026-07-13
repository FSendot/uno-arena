#!/usr/bin/env bash
# Explicit/networked: run the established kind delivery, adapter, and store probes.
# These checks mutate only the disposable uno-arena cluster and ephemeral test DBs.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

assert_kind_context

probes=(
  test-debezium-connectors.sh
  test-debezium-postgres-to-kafka-live.sh
  test-debezium-postgres-to-redis-live.sh
  test-game-integrity-adapter.sh
  test-identity-adapter.sh
  test-identity-integration.sh
  test-room-integration.sh
  test-tournament-adapter.sh
  test-tournament-integration.sh
  test-tournament-redis-integration.sh
  test-ranking-adapter.sh
  test-ranking-integration.sh
  test-ranking-redis-integration.sh
  test-analytics-adapter.sh
  test-analytics-integration.sh
  test-analytics-kafka-to-clickhouse-live.sh
  test-analytics-projection-rebuilder-live.sh
  test-spectator-adapter.sh
  test-spectator-integration.sh
  test-spectator-projection-rebuilder-live.sh
  test-gateway-adapter.sh
  test-gateway-si-redis-admission-live.sh
  test-gateway-integration.sh
  test-redis-aof.sh
  # Re-check every connector task after probes have emitted real outbox rows.
  test-debezium-connectors.sh
)

for probe in "${probes[@]}"; do
  [[ -f "${SCRIPT_DIR}/${probe}" ]] || die "missing probe: ${SCRIPT_DIR}/${probe}"
  echo "running ${probe}"
  bash "${SCRIPT_DIR}/${probe}"
done

echo "ok kind-live-probes count=${#probes[@]}"
