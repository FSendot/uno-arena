#!/usr/bin/env bash
# Explicit/networked: run the established kind delivery, adapter, and store probes.
# These checks mutate only the disposable uno-arena cluster and ephemeral test DBs.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

assert_kind_context

probes=(
  # Read-only baseline must pass before observability acceptance generates data.
  test-observability-live.sh
  test-debezium-connectors.sh
  # Prove business metrics and their canonical causal trace before the
  # single-node lane accumulates load from the adapter/rebuilder probes.
  test-observability-business-live.sh
  test-observability-trace-live.sh
  test-observability-security-live.sh
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
  # Destructive observability recovery and persistence proofs stay last so
  # their deliberate pod replacements cannot perturb functional probes.
  test-observability-outage-live.sh
  test-observability-persistence-live.sh
)

for probe in "${probes[@]}"; do
  [[ -f "${SCRIPT_DIR}/${probe}" ]] || die "missing probe: ${SCRIPT_DIR}/${probe}"
  if [[ "${probe}" == "test-observability-persistence-live.sh" ]]; then
    # The outage proof ends as soon as one fresh complete trace is queryable.
    # Let every application exporter discard its stale connection and settle
    # on the replacement Alloy endpoint before creating the persistence marker.
    sleep "${OBS_OUTAGE_SETTLE_SECONDS:-15}"
  fi
  echo "running ${probe}"
  bash "${SCRIPT_DIR}/${probe}"
done

echo "ok kind-live-probes count=${#probes[@]}"
