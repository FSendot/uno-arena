#!/usr/bin/env bash
# Offline helm lint/template checks for Room Gameplay chart overlays.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
CHART="${ROOT}/services/room-gameplay/helm/room-gameplay"
HELM="${HELM:-helm}"

command -v "${HELM}" >/dev/null 2>&1 || { echo "helm required" >&2; exit 1; }

"${HELM}" lint "${CHART}" -f "${CHART}/values.kind.yaml" >/dev/null

kind_out="$("${HELM}" template room-kind "${CHART}" -f "${CHART}/values.kind.yaml")"
echo "${kind_out}" | grep -q 'image: "uno-arena/room-gameplay:local"'
echo "${kind_out}" | grep -q 'imagePullPolicy: IfNotPresent'
! echo "${kind_out}" | grep -q 'image: "uno-arena/room-gameplay@"'
echo "${kind_out}" | grep -q 'name: "uno-arena-local-credentials"'
echo "${kind_out}" | grep -q 'DATABASE_URL'
echo "${kind_out}" | grep -q 'room-database-url'
echo "${kind_out}" | grep -q 'DEPLOYMENT_ENV'
echo "${kind_out}" | grep -q 'WORKER_ROLE'
echo "${kind_out}" | grep -q 'room-timer'
echo "${kind_out}" | grep -q 'type: ClusterIP'
! echo "${kind_out}" | grep -E 'type:\s*(NodePort|LoadBalancer)'

echo "${kind_out}" | grep -q 'ROOM_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL'
echo "${kind_out}" | grep -q 'key: "analytics-room-credential"'
echo "${kind_out}" | grep -q 'ROOM_ANALYTICS_BACKFILL_CURSOR_SECRET'
echo "${kind_out}" | grep -q 'key: "room-analytics-backfill-cursor-secret"'
echo "${kind_out}" | grep -q 'ROOM_PUBLIC_LIST_CURSOR_SECRET'
echo "${kind_out}" | grep -q 'key: "room-public-list-cursor-secret"'
# Timer worker must not receive Analytics backfill auth, public-list cursor, or cursor MAC secrets.
timer_doc="$(echo "${kind_out}" | awk '/name: room-kind-timer-worker$/,/^---$/')"
! echo "${timer_doc}" | grep -q 'ROOM_ANALYTICS_BACKFILL_CURSOR_SECRET'
! echo "${timer_doc}" | grep -q 'ROOM_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL'
! echo "${timer_doc}" | grep -q 'ROOM_PUBLIC_LIST_CURSOR_SECRET'

if "${HELM}" template room-staging "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" >/dev/null 2>&1; then
  echo "staging without digest must fail" >&2
  exit 1
fi
if "${HELM}" template room-prod "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" >/dev/null 2>&1; then
  echo "production without digest must fail" >&2
  exit 1
fi

if "${HELM}" template room-staging-tag "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with tag and empty digest must fail" >&2
  exit 1
fi
if "${HELM}" template room-prod-tag "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" \
  --set image.tag=latest >/dev/null 2>&1; then
  echo "production with tag and empty digest must fail" >&2
  exit 1
fi

if "${HELM}" template room-staging-kind-override "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set kind=true --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with --set kind=true and tag must still fail (DEPLOYMENT_ENV!=local)" >&2
  exit 1
fi
if "${HELM}" template room-prod-kind-override "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" \
  --set kind=true --set image.tag=latest >/dev/null 2>&1; then
  echo "production with --set kind=true and tag must still fail (DEPLOYMENT_ENV!=local)" >&2
  exit 1
fi

for bad in \
  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaG" \
  "sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" \
  "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" \
  "latest" \
  "sha256:" \
  "SHA256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
do
  if "${HELM}" template room-bad-digest "${CHART}" -f "${CHART}/values.yaml" \
    --set image.digest="${bad}" --set image.tag= >/dev/null 2>&1; then
    echo "malformed digest must fail: ${bad}" >&2
    exit 1
  fi
done

pin_out="$("${HELM}" template room-pin "${CHART}" -f "${CHART}/values.yaml" \
  --set image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --set image.tag=)"
echo "${pin_out}" | grep -q 'image: "registry.gitlab.com/group/uno-arena/room-gameplay@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"'

pin_tag_staging="$("${HELM}" template room-pin-staging "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set image.digest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
  --set image.tag=latest)"
echo "${pin_tag_staging}" | grep -q 'image: "registry.gitlab.com/group/uno-arena/room-gameplay@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"'
! echo "${pin_tag_staging}" | grep -q 'image: "registry.gitlab.com/group/uno-arena/room-gameplay:latest"'

pin_staging_full="$("${HELM}" template room-pin-staging-full "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set image.digest=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc \
  --set image.tag=)"
echo "${pin_staging_full}" | grep -q 'ROOM_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL'
echo "${pin_staging_full}" | grep -q 'ROOM_ANALYTICS_BACKFILL_CURSOR_SECRET'
echo "${pin_staging_full}" | grep -q 'ROOM_PUBLIC_LIST_CURSOR_SECRET'

pin_prod_full="$("${HELM}" template room-pin-prod-full "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" \
  --set image.digest=sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd \
  --set image.tag=)"
echo "${pin_prod_full}" | grep -q 'ROOM_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL'
echo "${pin_prod_full}" | grep -q 'ROOM_ANALYTICS_BACKFILL_CURSOR_SECRET'
echo "${pin_prod_full}" | grep -q 'ROOM_PUBLIC_LIST_CURSOR_SECRET'

echo "ok room-gameplay-helm"
