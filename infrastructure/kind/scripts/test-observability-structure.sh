#!/usr/bin/env bash
# Offline observability, storage, and clean-lane structure checks.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

"${ROOT}/infrastructure/istio/verify.sh"
"${ROOT}/infrastructure/observability/helm/uno-arena-observability/helm-test.sh"

acceptance=(
  test-observability-live.sh
  test-observability-business-live.sh
  test-observability-trace-live.sh
  test-observability-security-live.sh
  test-observability-outage-live.sh
  test-observability-persistence-live.sh
)
for file in install-istio.sh deploy-observability.sh wait-observability.sh port-forward-grafana.sh observability-acceptance-lib.sh "${acceptance[@]}"; do
  bash -n "${SCRIPT_DIR}/${file}"
done
grep -Fq 'KIND_ISTIO_TIMEOUT:-600s' "${SCRIPT_DIR}/install-istio.sh" || {
  echo "cold-cache Istio installation must retain the empty-cluster pull budget" >&2
  exit 1
}
grep -Fq 'get job/minio-create-observability-buckets' "${SCRIPT_DIR}/wait-observability.sh" || {
  echo "observability wait must tolerate the completed bucket job TTL" >&2
  exit 1
}
grep -Fq 'get job/minio-create-observability-buckets' "${SCRIPT_DIR}/deploy-observability.sh" || {
  echo "observability redeploy must tolerate the completed bucket job TTL" >&2
  exit 1
}
grep -Fq 'get job/minio-create-observability-buckets' "${SCRIPT_DIR}/wait.sh" || {
  echo "shared readiness must tolerate the completed bucket job TTL" >&2
  exit 1
}
for file in "${acceptance[@]}"; do
  grep -Fq "${file}" "${SCRIPT_DIR}/run-live-probes.sh" || { echo "missing acceptance probe ${file}" >&2; exit 1; }
done
runner="${SCRIPT_DIR}/run-live-probes.sh"
baseline_line="$(grep -n 'test-observability-live.sh' "${runner}" | head -n1 | cut -d: -f1)"
business_line="$(grep -n 'test-observability-business-live.sh' "${runner}" | head -n1 | cut -d: -f1)"
trace_line="$(grep -n 'test-observability-trace-live.sh' "${runner}" | head -n1 | cut -d: -f1)"
first_adapter_line="$(grep -n 'test-debezium-postgres-to-kafka-live.sh' "${runner}" | head -n1 | cut -d: -f1)"
persist_line="$(grep -n 'test-observability-persistence-live.sh' "${runner}" | head -n1 | cut -d: -f1)"
(( baseline_line < business_line && business_line < trace_line && trace_line < first_adapter_line && first_adapter_line < persist_line )) || {
  echo "invalid observability acceptance order" >&2
  exit 1
}

business="${SCRIPT_DIR}/test-observability-business-live.sh"
[[ "$(grep -Fc '"${SCRIPT_DIR}/wait-observability.sh"' "${business}")" -ge 2 ]] || {
  echo "business proof must restore observability readiness between independent flows" >&2
  exit 1
}
for contract in \
  'unoarena_players_created_total' \
  'unoarena_games_completed_total' \
  'unoarena_tournaments_completed_total' \
  'sum(increase(${metric}[30m]))' \
  '"code":"room_starting"' \
  '"reason":"stale_sequence"' \
  'for attempt in $(seq 1 60)' \
  'OBS_BOT_POLL_INTERVAL_SECONDS:-5' \
  'OBS_BOT_ACTION_INTERVAL_SECONDS:-2' \
  '--action-interval "${bot_action_interval}"' \
  'client-checkpoint/bin/unoarena'; do
  grep -Fq -- "${contract}" "${business}" || { echo "business proof missing ${contract}" >&2; exit 1; }
done
grep -Fq 'OTEL_TRACES_SAMPLER: x_parentbased_mutations' \
  "${ROOT}/services/gateway/helm/gateway/values.kind.yaml" || {
  echo "kind Gateway must deterministically sample mutation roots" >&2
  exit 1
}
grep -Fq 'OTEL_TRACES_SAMPLER_ARG: "0.001"' \
  "${ROOT}/services/gateway/helm/gateway/values.kind.yaml" || {
  echo "kind Gateway read roots must use the production-like sampling ratio" >&2
  exit 1
}
if grep -Fq 'OTEL_TRACES_SAMPLER: parentbased_always_on' \
  "${ROOT}/services/gateway/helm/gateway/values.kind.yaml"; then
  echo "kind Gateway must not trace every polling read" >&2
  exit 1
fi
for service in analytics game-integrity gateway identity ranking room-gameplay spectator-view tournament-orchestration; do
  chart="${ROOT}/services/${service}/helm/${service}"
  rendered="$(helm template "${service}-kind" "${chart}" -f "${chart}/values.kind.yaml")"
  [[ "$(grep -Fc 'periodSeconds: 20' <<<"${rendered}")" -ge 2 ]] || {
    echo "kind ${service} API probes must use the local 20-second cadence" >&2
    exit 1
  }
  grep -Fq 'periodSeconds: 10' "${chart}/values.yaml" || {
    echo "base ${service} probe cadence must remain ten seconds" >&2
    exit 1
  }
done
for postgres in identity ranking room-gameplay tournament; do
  manifest="${ROOT}/infrastructure/kind/manifests/10-postgres/postgres-${postgres}.yaml"
  grep -A8 'readinessProbe:' "${manifest}" | grep -Fq 'periodSeconds: 15' || {
    echo "kind ${postgres} Postgres readiness must use the local 15-second cadence" >&2
    exit 1
  }
  grep -A8 'livenessProbe:' "${manifest}" | grep -Fq 'periodSeconds: 30' || {
    echo "kind ${postgres} Postgres liveness must use the local 30-second cadence" >&2
    exit 1
  }
done
grep -A8 'readinessProbe:' "${ROOT}/infrastructure/kind/manifests/20-redis/redis.yaml" | \
  grep -Fq 'periodSeconds: 15' || { echo "kind Redis readiness cadence invalid" >&2; exit 1; }
grep -A8 'livenessProbe:' "${ROOT}/infrastructure/kind/manifests/20-redis/redis.yaml" | \
  grep -Fq 'periodSeconds: 30' || { echo "kind Redis liveness cadence invalid" >&2; exit 1; }
grep -A10 'readinessProbe:' "${ROOT}/infrastructure/kind/manifests/80-debezium/connect.yaml" | \
  grep -Fq 'periodSeconds: 20' || { echo "kind Debezium Connect readiness cadence invalid" >&2; exit 1; }
grep -A10 'livenessProbe:' "${ROOT}/infrastructure/kind/manifests/80-debezium/connect.yaml" | \
  grep -Fq 'periodSeconds: 30' || { echo "kind Debezium Connect liveness cadence invalid" >&2; exit 1; }
for span in room-gameplay.command.handle room-gameplay.command.commit room-gameplay.outbox.persist \
  ranking.kafka.process ranking.database.commit ranking.kafka.commit; do
  grep -Fq "${span//./\\.}" "${SCRIPT_DIR}/test-observability-trace-live.sh" || {
    echo "canonical trace assertion missing ${span}" >&2
    exit 1
  }
done
for contract in \
  'services/identity/src/telemetry_runtime.go:identity.postgres.query' \
  'services/room-gameplay/src/store/telemetry.go:room-gameplay.postgres.query' \
  'services/tournament-orchestration/src/store/telemetry.go:tournament-orchestration.postgres.query' \
  'services/ranking/src/runtime.go:ranking.postgres.query'; do
  file="${contract%%:*}"
  span="${contract#*:}"
  grep -Fq "WithSpanNameFunc(func(string) string { return \"${span}\" })" "${ROOT}/${file}" || {
    echo "Postgres telemetry must use bounded span name ${span}" >&2
    exit 1
  }
done
for file in \
  services/gateway/src/telemetry_runtime.go \
  services/room-gameplay/src/store/telemetry.go \
  services/tournament-orchestration/src/store/telemetry.go \
  services/ranking/src/runtime.go \
  services/spectator-view/src/runtime.go \
  services/spectator-view/src/projection_rebuild_worker.go; do
  grep -Fq 'redisotel.WithDBStatement(false)' "${ROOT}/${file}" || {
    echo "Redis telemetry must suppress raw command statements in ${file}" >&2
    exit 1
  }
done
grep -Fq "scale deployment/alloy-otlp --replicas=0" "${SCRIPT_DIR}/test-observability-outage-live.sh"
grep -Fq 'KIND_OBSERVABILITY_TIMEOUT="${OBS_OUTAGE_RECOVERY_TIMEOUT:-300s}"' \
  "${SCRIPT_DIR}/test-observability-outage-live.sh" || {
  echo "outage proof must restore full observability readiness before recovery traffic" >&2
  exit 1
}
for workload in loki tempo minio; do
  grep -Fq "ready_pod_name ${workload}" "${SCRIPT_DIR}/test-observability-persistence-live.sh"
done
grep -Fq '/ingester/shutdown?flush=true&delete_ring_tokens=false&terminate=true' \
  "${SCRIPT_DIR}/test-observability-persistence-live.sh"
grep -Fq 'Loki replacement could not recover accepted log from S3' \
  "${SCRIPT_DIR}/test-observability-persistence-live.sh"
grep -Fq 'shutdown_rc != 52' "${SCRIPT_DIR}/test-observability-persistence-live.sh"
grep -Fq 'OBS_KUBECTL_MUTATION_RETRIES:-5' "${SCRIPT_DIR}/test-observability-persistence-live.sh"
grep -Fq 'delete pod "${old_loki_name}" --ignore-not-found' "${SCRIPT_DIR}/test-observability-persistence-live.sh"
grep -Fq 'wait_loki_graceful_restart' "${SCRIPT_DIR}/test-observability-persistence-live.sh" || {
  echo "Loki persistence proof must wait for full-process TSDB index shipping" >&2
  exit 1
}
grep -Fq 'delete pod "${old_minio_name}" --ignore-not-found' "${SCRIPT_DIR}/test-observability-persistence-live.sh"
grep -Fq 'client-checkpoint/bin/unoarena' "${SCRIPT_DIR}/test-observability-persistence-live.sh"
grep -Fq 'CORRELATION_ID="${LOG_MARKER}" "${CLI}" seed' "${SCRIPT_DIR}/test-observability-persistence-live.sh"
if grep -Fq 'OBS_LOKI_SHIP_SETTLE_SECONDS' "${SCRIPT_DIR}/test-observability-persistence-live.sh"; then
  echo "Loki persistence proof must not substitute a sleep for graceful index shipping" >&2
  exit 1
fi
rendered="$(helm template uno-arena-observability \
  "${ROOT}/infrastructure/observability/helm/uno-arena-observability" \
  -f "${ROOT}/infrastructure/observability/helm/uno-arena-observability/values.kind.yaml")"
grep -Fq 'flush_on_shutdown: true' <<<"${rendered}"
grep -Fq 'active_index_directory: /var/loki/tsdb-index' <<<"${rendered}"
grep -Fq 'cache_location: /var/loki/tsdb-cache' <<<"${rendered}"
grep -Fq 'min_input_blocks: 1000' <<<"${rendered}"
grep -Fq 'max_input_blocks: 1000' <<<"${rendered}"
grep -Fq 'max_block_duration: 10m' <<<"${rendered}"
grep -Fq 'blocklist_poll: 1m' <<<"${rendered}"
grep -Fq 'timeout = "30s"' <<<"${rendered}"
grep -Fq 'num_consumers = 2' <<<"${rendered}"
grep -Fq 'queue_size = 2048' <<<"${rendered}"
grep -Fq 'max_elapsed_time = "10m"' <<<"${rendered}"

storage="${ROOT}/infrastructure/kind/manifests/90-observability-storage"
grep -Fq 'storage: 5Gi' "${storage}/minio.yaml"
grep -Fq '@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e' "${storage}/minio.yaml"
grep -Fq '@sha256:aead63c77f9db9107f1696fb08ecb0faeda23729cde94b0f663edf4fe09728e3' "${storage}/job-buckets.yaml"
grep -Fq 'mc --config-dir /tmp/mc mb --ignore-existing local/loki-logs' "${storage}/job-buckets.yaml"
grep -Fq 'mc --config-dir /tmp/mc mb --ignore-existing local/tempo-traces' "${storage}/job-buckets.yaml"
grep -Fq 'mountPath: /tmp/mc' "${storage}/job-buckets.yaml"
grep -Fq 'emptyDir: {sizeLimit: 8Mi}' "${storage}/job-buckets.yaml"
grep -Fq 'serviceAccountName: minio' "${storage}/minio.yaml"
grep -Fq 'serviceAccountName: minio-bucket-bootstrap' "${storage}/job-buckets.yaml"

clean="${SCRIPT_DIR}/clean-deploy.sh"
plan="$(${clean} --dry-run)"
istio_line="$(grep -n '/install-istio.sh' <<<"${plan}" | cut -d: -f1)"
apply_line="$(grep -n '/apply.sh' <<<"${plan}" | cut -d: -f1)"
obs_line="$(grep -n '/deploy-observability.sh' <<<"${plan}" | cut -d: -f1)"
apps_line="$(grep -n '/deploy-services.sh' <<<"${plan}" | cut -d: -f1)"
(( istio_line < apply_line && apply_line < obs_line && obs_line < apps_line )) || { echo "invalid clean-lane order" >&2; exit 1; }

echo "ok kind-observability-structure"
