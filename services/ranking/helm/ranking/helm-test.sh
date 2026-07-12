#!/usr/bin/env bash
# Offline helm lint/template checks for Ranking chart overlays.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
CHART="${ROOT}/services/ranking/helm/ranking"
HELM="${HELM:-helm}"

command -v "${HELM}" >/dev/null 2>&1 || { echo "helm required" >&2; exit 1; }

"${HELM}" lint "${CHART}" -f "${CHART}/values.kind.yaml" >/dev/null

kind_out="$("${HELM}" template ranking-kind "${CHART}" -f "${CHART}/values.kind.yaml")"
echo "${kind_out}" | grep -q 'image: "uno-arena/ranking:local"'
echo "${kind_out}" | grep -q 'imagePullPolicy: IfNotPresent'
! echo "${kind_out}" | grep -q 'image: "uno-arena/ranking@"'
echo "${kind_out}" | grep -q 'name: "uno-arena-local-credentials"'
echo "${kind_out}" | grep -q 'DATABASE_URL'
echo "${kind_out}" | grep -q 'ranking-database-url'
echo "${kind_out}" | grep -q 'DEPLOYMENT_ENV'
echo "${kind_out}" | grep -q 'KAFKA_BROKERS'
echo "${kind_out}" | grep -q 'kafka.uno-arena.svc.cluster.local:9092'
echo "${kind_out}" | grep -q 'KAFKA_CONSUMER_GROUP'
echo "${kind_out}" | grep -q 'KAFKA_GAME_COMPLETED_TOPIC'
echo "${kind_out}" | grep -q 'KAFKA_GAME_COMPLETED_DLQ_TOPIC'
echo "${kind_out}" | grep -q 'KAFKA_PLAYERS_ADVANCED_TOPIC'
echo "${kind_out}" | grep -q 'KAFKA_PLAYERS_ADVANCED_DLQ_TOPIC'
echo "${kind_out}" | grep -q 'KAFKA_TOURNAMENT_COMPLETED_TOPIC'
echo "${kind_out}" | grep -q 'KAFKA_TOURNAMENT_COMPLETED_DLQ_TOPIC'
echo "${kind_out}" | grep -q 'KAFKA_PLAYER_RATING_UPDATED_TOPIC'
echo "${kind_out}" | grep -q 'KAFKA_PLAYER_RATING_UPDATED_DLQ_TOPIC'
echo "${kind_out}" | grep -q 'REDIS_URL'
echo "${kind_out}" | grep -q 'redis://redis.uno-arena.svc.cluster.local:6379/4'
echo "${kind_out}" | grep -q 'RANKING_LEADERBOARD_CURSOR_SECRET'
echo "${kind_out}" | grep -q 'ranking-leaderboard-cursor-secret'
echo "${kind_out}" | grep -q 'RANKING_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL'
echo "${kind_out}" | grep -q 'key: "analytics-ranking-credential"'
echo "${kind_out}" | grep -q 'RANKING_ANALYTICS_BACKFILL_CURSOR_SECRET'
echo "${kind_out}" | grep -q 'ranking-analytics-backfill-cursor-secret'
test "$(echo "${kind_out}" | grep -c 'name: RANKING_ANALYTICS_BACKFILL_CURSOR_SECRET')" -eq 1
test "$(echo "${kind_out}" | grep -c 'name: RANKING_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL')" -eq 1
echo "${kind_out}" | grep -q 'type: ClusterIP'
! echo "${kind_out}" | grep -E 'type:\s*(NodePort|LoadBalancer)'
echo "${kind_out}" | grep -q 'WORKER_ROLE'
echo "${kind_out}" | grep -q 'ranking-leaderboard-snapshotter'
echo "${kind_out}" | grep -q 'name: ranking-kind-leaderboard-snapshotter'
echo "${kind_out}" | grep -q 'replicas: 1'
# Internal Stop grace is 35s; pod termination must outlive it (default K8s grace is only 30s).
echo "${kind_out}" | grep -q 'terminationGracePeriodSeconds: 45'
# WORKER_ROLE appears only on the snapshotter Deployment (not the API).
test "$(echo "${kind_out}" | grep -c 'name: WORKER_ROLE')" -eq 1
# Snapshotter must remain DB-only: no REDIS_URL / cursor / analytics-backfill secrets on that Deployment.
snap_doc="$(echo "${kind_out}" | awk '/name: ranking-kind-leaderboard-snapshotter$/,/^---$/')"
! echo "${snap_doc}" | grep -q 'REDIS_URL'
! echo "${snap_doc}" | grep -q 'RANKING_LEADERBOARD_CURSOR_SECRET'
! echo "${snap_doc}" | grep -q 'RANKING_ANALYTICS_BACKFILL_CURSOR_SECRET'
! echo "${snap_doc}" | grep -q 'RANKING_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL'
! echo "${snap_doc}" | grep -q 'KAFKA_'

staging_out="$("${HELM}" template ranking-staging "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --set image.tag=)"
! echo "${staging_out}" | grep -q 'ranking-leaderboard-snapshotter'
! echo "${staging_out}" | grep -q 'leaderboard-snapshotter'
echo "${staging_out}" | grep -q 'DEPLOYMENT_ENV'
echo "${staging_out}" | grep -q 'value: "staging"'
echo "${staging_out}" | grep -q 'name: RANKING_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL'
echo "${staging_out}" | grep -q 'name: RANKING_ANALYTICS_BACKFILL_CURSOR_SECRET'

prod_out="$("${HELM}" template ranking-prod "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" \
  --set image.digest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
  --set image.tag=)"
! echo "${prod_out}" | grep -q 'ranking-leaderboard-snapshotter'
! echo "${prod_out}" | grep -q 'leaderboard-snapshotter'
echo "${prod_out}" | grep -q 'DEPLOYMENT_ENV'
echo "${prod_out}" | grep -q 'value: "production"'
echo "${prod_out}" | grep -q 'name: RANKING_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL'
echo "${prod_out}" | grep -q 'name: RANKING_ANALYTICS_BACKFILL_CURSOR_SECRET'

if "${HELM}" template ranking-staging "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" >/dev/null 2>&1; then
  echo "staging without digest must fail" >&2
  exit 1
fi
if "${HELM}" template ranking-prod "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" >/dev/null 2>&1; then
  echo "production without digest must fail" >&2
  exit 1
fi

if "${HELM}" template ranking-staging-tag "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with tag and empty digest must fail" >&2
  exit 1
fi
if "${HELM}" template ranking-prod-tag "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" \
  --set image.tag=latest >/dev/null 2>&1; then
  echo "production with tag and empty digest must fail" >&2
  exit 1
fi

if "${HELM}" template ranking-staging-kind-override "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set kind=true --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with --set kind=true and tag must still fail (DEPLOYMENT_ENV!=local)" >&2
  exit 1
fi

pin_out="$("${HELM}" template ranking-pin "${CHART}" -f "${CHART}/values.yaml" \
  --set image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --set image.tag=)"
echo "${pin_out}" | grep -q 'image: "registry.gitlab.com/group/uno-arena/ranking@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"'
! echo "${pin_out}" | grep -q 'ranking-leaderboard-snapshotter'

echo "ok ranking-helm"
