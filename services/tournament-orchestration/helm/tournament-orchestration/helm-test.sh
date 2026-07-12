#!/usr/bin/env bash
# Offline helm lint/template checks for Tournament Orchestration chart overlays.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
CHART="${ROOT}/services/tournament-orchestration/helm/tournament-orchestration"
HELM="${HELM:-helm}"

command -v "${HELM}" >/dev/null 2>&1 || { echo "helm required" >&2; exit 1; }

"${HELM}" lint "${CHART}" -f "${CHART}/values.kind.yaml" >/dev/null

kind_out="$("${HELM}" template tournament-kind "${CHART}" -f "${CHART}/values.kind.yaml")"
echo "${kind_out}" | grep -q 'image: "uno-arena/tournament-orchestration:local"'
echo "${kind_out}" | grep -q 'imagePullPolicy: IfNotPresent'
! echo "${kind_out}" | grep -q 'image: "uno-arena/tournament-orchestration@"'
echo "${kind_out}" | grep -q 'name: "uno-arena-local-credentials"'
echo "${kind_out}" | grep -q 'DATABASE_URL'
echo "${kind_out}" | grep -q 'tournament-database-url'
echo "${kind_out}" | grep -q 'DEPLOYMENT_ENV'
echo "${kind_out}" | grep -q 'KAFKA_BROKERS'
echo "${kind_out}" | grep -q 'kafka.uno-arena.svc.cluster.local:9092'
echo "${kind_out}" | grep -q 'KAFKA_CONSUMER_GROUP'
echo "${kind_out}" | grep -q 'tournament-orchestration'
echo "${kind_out}" | grep -q 'KAFKA_MATCH_COMPLETED_TOPIC'
echo "${kind_out}" | grep -q 'room.match.completed'
echo "${kind_out}" | grep -q 'KAFKA_MATCH_COMPLETED_DLQ_TOPIC'
echo "${kind_out}" | grep -q 'room.match.completed.tournament-orchestration.dlq'
echo "${kind_out}" | grep -q 'REDIS_URL'
echo "${kind_out}" | grep -q 'redis.uno-arena.svc.cluster.local:6379/7'
echo "${kind_out}" | grep -q 'type: ClusterIP'
! echo "${kind_out}" | grep -E 'type:\s*(NodePort|LoadBalancer)'
echo "${kind_out}" | grep -q 'WORKER_ROLE'
echo "${kind_out}" | grep -q 'tournament-provisioning'
echo "${kind_out}" | grep -q 'tournament-seeding'
echo "${kind_out}" | grep -q 'tournament-round-completion'
echo "${kind_out}" | grep -q 'name: tournament-kind-provisioning-worker'
echo "${kind_out}" | grep -q 'name: tournament-kind-seeding-worker'
echo "${kind_out}" | grep -q 'name: tournament-kind-completion-worker'
echo "${kind_out}" | grep -q 'replicas: 2'
# WORKER_ROLE appears only on worker Deployments (not the API) — provisioning + seeding + completion.
test "$(echo "${kind_out}" | grep -c 'name: WORKER_ROLE')" -eq 3
# Durable API binds bracket + analytics-backfill cursor secrets; workers must not receive them.
echo "${kind_out}" | grep -q 'name: TOURNAMENT_BRACKET_CURSOR_SECRET'
echo "${kind_out}" | grep -q 'key: "tournament-bracket-cursor-secret"'
test "$(echo "${kind_out}" | grep -c 'name: TOURNAMENT_BRACKET_CURSOR_SECRET')" -eq 1
echo "${kind_out}" | grep -q 'name: TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL'
echo "${kind_out}" | grep -q 'key: "analytics-tournament-credential"'
echo "${kind_out}" | grep -q 'name: TOURNAMENT_ANALYTICS_BACKFILL_CURSOR_SECRET'
echo "${kind_out}" | grep -q 'key: "tournament-analytics-backfill-cursor-secret"'
test "$(echo "${kind_out}" | grep -c 'name: TOURNAMENT_ANALYTICS_BACKFILL_CURSOR_SECRET')" -eq 1
test "$(echo "${kind_out}" | grep -c 'name: TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL')" -eq 1
worker_out="$(echo "${kind_out}" | awk '/name: tournament-kind-provisioning-worker/{p=1} p{print} /^---$/{if(p&&seen++){exit}}')"
! echo "${worker_out}" | grep -q 'TOURNAMENT_BRACKET_CURSOR_SECRET'
! echo "${worker_out}" | grep -q 'TOURNAMENT_ANALYTICS_BACKFILL_CURSOR_SECRET'
! echo "${worker_out}" | grep -q 'TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL'
echo "${worker_out}" | grep -q 'name: DATABASE_URL'
echo "${worker_out}" | grep -q 'name: REDIS_URL'
echo "${worker_out}" | grep -q 'name: TOURNAMENT_INTERNAL_CREDENTIAL'
echo "${worker_out}" | grep -q 'name: SERVICE_CREDENTIAL'
echo "${worker_out}" | grep -q 'name: ROOM_SERVICE_CREDENTIAL'
seed_out="$(echo "${kind_out}" | awk '/name: tournament-kind-seeding-worker/{p=1} p{print} /^---$/{if(p&&seen++){exit}}')"
! echo "${seed_out}" | grep -q 'TOURNAMENT_BRACKET_CURSOR_SECRET'
! echo "${seed_out}" | grep -q 'TOURNAMENT_ANALYTICS_BACKFILL_CURSOR_SECRET'
! echo "${seed_out}" | grep -q 'TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL'
! echo "${seed_out}" | grep -q 'ROOM_SERVICE_CREDENTIAL'
! echo "${seed_out}" | grep -q 'ROOM_GAMEPLAY_URL'
! echo "${seed_out}" | grep -q 'TOURNAMENT_INTERNAL_CREDENTIAL'
! echo "${seed_out}" | grep -q 'SERVICE_CREDENTIAL'
echo "${seed_out}" | grep -q 'name: DATABASE_URL'
echo "${seed_out}" | grep -q 'name: REDIS_URL'
# Seeding worker least privilege: only DATABASE_URL secret mapping (plus static REDIS_URL / WORKER_ROLE).
test "$(echo "${seed_out}" | grep -c 'secretKeyRef')" -eq 1
echo "${seed_out}" | grep -q 'tournament-seeding'
# Completion worker is emitted before the API Deployment; extract one document only.
comp_out="$(echo "${kind_out}" | awk '
  /^---$/ { if (in_doc) exit; next }
  /name: tournament-kind-completion-worker$/ { in_doc=1 }
  in_doc { print }
')"
! echo "${comp_out}" | grep -q 'TOURNAMENT_BRACKET_CURSOR_SECRET'
! echo "${comp_out}" | grep -q 'TOURNAMENT_ANALYTICS_BACKFILL_CURSOR_SECRET'
! echo "${comp_out}" | grep -q 'TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL'
! echo "${comp_out}" | grep -q 'ROOM_SERVICE_CREDENTIAL'
! echo "${comp_out}" | grep -q 'ROOM_GAMEPLAY_URL'
! echo "${comp_out}" | grep -q 'TOURNAMENT_INTERNAL_CREDENTIAL'
! echo "${comp_out}" | grep -q 'SERVICE_CREDENTIAL'
echo "${comp_out}" | grep -q 'name: DATABASE_URL'
echo "${comp_out}" | grep -q 'name: REDIS_URL'
# Completion worker least privilege: only DATABASE_URL secret mapping (plus static env including REDIS_URL).
test "$(echo "${comp_out}" | grep -c 'secretKeyRef')" -eq 1
echo "${comp_out}" | grep -q 'tournament-round-completion'

staging_out="$("${HELM}" template tournament-staging "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --set image.tag=)"
! echo "${staging_out}" | grep -q 'tournament-provisioning'
! echo "${staging_out}" | grep -q 'provisioning-worker'
! echo "${staging_out}" | grep -q 'tournament-seeding'
! echo "${staging_out}" | grep -q 'seeding-worker'
! echo "${staging_out}" | grep -q 'tournament-round-completion'
! echo "${staging_out}" | grep -q 'completion-worker'
echo "${staging_out}" | grep -q 'name: TOURNAMENT_BRACKET_CURSOR_SECRET'
echo "${staging_out}" | grep -q 'key: "tournament-bracket-cursor-secret"'
echo "${staging_out}" | grep -q 'name: TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL'
echo "${staging_out}" | grep -q 'name: TOURNAMENT_ANALYTICS_BACKFILL_CURSOR_SECRET'

prod_out="$("${HELM}" template tournament-prod "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" \
  --set image.digest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
  --set image.tag=)"
! echo "${prod_out}" | grep -q 'tournament-provisioning'
! echo "${prod_out}" | grep -q 'provisioning-worker'
! echo "${prod_out}" | grep -q 'tournament-seeding'
! echo "${prod_out}" | grep -q 'seeding-worker'
! echo "${prod_out}" | grep -q 'tournament-round-completion'
! echo "${prod_out}" | grep -q 'completion-worker'
echo "${prod_out}" | grep -q 'name: TOURNAMENT_BRACKET_CURSOR_SECRET'
echo "${prod_out}" | grep -q 'key: "tournament-bracket-cursor-secret"'
echo "${prod_out}" | grep -q 'name: TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL'
echo "${prod_out}" | grep -q 'name: TOURNAMENT_ANALYTICS_BACKFILL_CURSOR_SECRET'

if "${HELM}" template tournament-staging "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" >/dev/null 2>&1; then
  echo "staging without digest must fail" >&2
  exit 1
fi
if "${HELM}" template tournament-prod "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" >/dev/null 2>&1; then
  echo "production without digest must fail" >&2
  exit 1
fi

if "${HELM}" template tournament-staging-tag "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with tag and empty digest must fail" >&2
  exit 1
fi
if "${HELM}" template tournament-prod-tag "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" \
  --set image.tag=latest >/dev/null 2>&1; then
  echo "production with tag and empty digest must fail" >&2
  exit 1
fi

if "${HELM}" template tournament-staging-kind-override "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set kind=true --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with --set kind=true and tag must still fail (DEPLOYMENT_ENV!=local)" >&2
  exit 1
fi

pin_out="$("${HELM}" template tournament-pin "${CHART}" -f "${CHART}/values.yaml" \
  --set image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --set image.tag=)"
echo "${pin_out}" | grep -q 'image: "registry.gitlab.com/group/uno-arena/tournament-orchestration@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"'
! echo "${pin_out}" | grep -q 'tournament-provisioning'
! echo "${pin_out}" | grep -q 'tournament-seeding'
! echo "${pin_out}" | grep -q 'tournament-round-completion'

echo "ok tournament-helm"
