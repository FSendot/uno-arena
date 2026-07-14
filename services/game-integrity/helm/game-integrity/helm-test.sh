#!/usr/bin/env bash
# Offline helm lint/template checks for Game Integrity chart overlays.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
CHART="${ROOT}/services/game-integrity/helm/game-integrity"
HELM="${HELM:-helm}"

command -v "${HELM}" >/dev/null 2>&1 || { echo "helm required" >&2; exit 1; }

"${HELM}" lint "${CHART}" -f "${CHART}/values.kind.yaml" >/dev/null

kind_out="$("${HELM}" template gi-kind "${CHART}" -f "${CHART}/values.kind.yaml")"
echo "${kind_out}" | grep -q 'cpu: 500m'
echo "${kind_out}" | grep -q 'timeoutSeconds: 3'
echo "${kind_out}" | grep -q 'failureThreshold: 6'
echo "${kind_out}" | grep -q 'serviceAccountName: game-integrity'
echo "${kind_out}" | grep -q 'automountServiceAccountToken: false'
echo "${kind_out}" | grep -q 'unoarena.io/metrics-exposed: "true"'
echo "${kind_out}" | grep -q 'unoarena.io/component: api'
echo "${kind_out}" | grep -q 'unoarena.io/metrics-scrape: service'
echo "${kind_out}" | grep -q 'containerPort: 9090'
echo "${kind_out}" | grep -q 'port: 9090'
echo "${kind_out}" | grep -q 'name: SERVICE_VERSION'
echo "${kind_out}" | grep -q 'name: POD_UID'
echo "${kind_out}" | grep -q 'name: TELEMETRY_MODE'
echo "${kind_out}" | grep -q 'image: "uno-arena/game-integrity:local"'
echo "${kind_out}" | grep -q 'imagePullPolicy: IfNotPresent'
! echo "${kind_out}" | grep -q 'image: "uno-arena/game-integrity@"'

# Digest-absent staging/prod must fail closed.
if "${HELM}" template gi-staging "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" >/dev/null 2>&1; then
  echo "staging without digest must fail" >&2
  exit 1
fi
if "${HELM}" template gi-prod "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" >/dev/null 2>&1; then
  echo "production without digest must fail" >&2
  exit 1
fi

# Tag alone (no digest) must fail outside explicit kind mode — even if caller supplies image.tag.
if "${HELM}" template gi-staging-tag "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with tag and empty digest must fail" >&2
  exit 1
fi
if "${HELM}" template gi-prod-tag "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" \
  --set image.tag=latest >/dev/null 2>&1; then
  echo "production with tag and empty digest must fail" >&2
  exit 1
fi

# String "false" must NOT unlock kind tag fallback (Helm string truthiness trap).
if "${HELM}" template gi-kind-string-false "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set-string kind=false --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with --set-string kind=false and tag must fail" >&2
  exit 1
fi
# Boolean false must also keep staging fail-closed.
if "${HELM}" template gi-kind-bool-false "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set kind=false --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with --set kind=false and tag must fail" >&2
  exit 1
fi

# Staging/production remain digest-only even if caller overrides --set kind=true
# (tag fallback also requires effective DEPLOYMENT_ENV=local).
if "${HELM}" template gi-staging-kind-override "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set kind=true --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with --set kind=true and tag must still fail (DEPLOYMENT_ENV!=local)" >&2
  exit 1
fi
if "${HELM}" template gi-prod-kind-override "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" \
  --set kind=true --set image.tag=latest >/dev/null 2>&1; then
  echo "production with --set kind=true and tag must still fail (DEPLOYMENT_ENV!=local)" >&2
  exit 1
fi
# kind=true alone is insufficient when env stays non-local (base chart default).
if "${HELM}" template gi-base-kind-only "${CHART}" -f "${CHART}/values.yaml" \
  --set kind=true --set image.tag=latest --set image.digest= >/dev/null 2>&1; then
  echo "base values with kind=true but DEPLOYMENT_ENV!=local must fail" >&2
  exit 1
fi

# Malformed digests must fail closed (exact sha256: + 64 lowercase hex).
for bad in \
  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaG" \
  "sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" \
  "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" \
  "latest" \
  "sha256:" \
  "SHA256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
do
  if "${HELM}" template gi-bad-digest "${CHART}" -f "${CHART}/values.yaml" \
    --set image.digest="${bad}" --set image.tag= >/dev/null 2>&1; then
    echo "malformed digest must fail: ${bad}" >&2
    exit 1
  fi
done

# Digest-pinned render succeeds (tag empty).
pin_out="$("${HELM}" template gi-pin "${CHART}" -f "${CHART}/values.yaml" \
  --set image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --set image.tag=)"
echo "${pin_out}" | grep -q 'image: "registry.gitlab.com/group/uno-arena/game-integrity@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"'

# Digest wins when both digest and tag are set (staging/production).
pin_tag_staging="$("${HELM}" template gi-pin-staging "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set image.digest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
  --set image.tag=latest)"
echo "${pin_tag_staging}" | grep -q 'image: "registry.gitlab.com/group/uno-arena/game-integrity@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"'
! echo "${pin_tag_staging}" | grep -q 'image: "registry.gitlab.com/group/uno-arena/game-integrity:latest"'

pin_tag_prod="$("${HELM}" template gi-pin-prod "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" \
  --set image.digest=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc \
  --set image.tag=latest)"
echo "${pin_tag_prod}" | grep -q 'image: "registry.gitlab.com/group/uno-arena/game-integrity@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"'
! echo "${pin_tag_prod}" | grep -q 'image: "registry.gitlab.com/group/uno-arena/game-integrity:latest"'

echo "ok game-integrity-helm"
