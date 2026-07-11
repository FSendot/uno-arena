#!/usr/bin/env bash
# Offline helm lint/template checks for Identity chart overlays.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
CHART="${ROOT}/services/identity/helm/identity"
HELM="${HELM:-helm}"

command -v "${HELM}" >/dev/null 2>&1 || { echo "helm required" >&2; exit 1; }

"${HELM}" lint "${CHART}" -f "${CHART}/values.kind.yaml" >/dev/null

kind_out="$("${HELM}" template identity-kind "${CHART}" -f "${CHART}/values.kind.yaml")"
echo "${kind_out}" | grep -q 'image: "uno-arena/identity:local"'
echo "${kind_out}" | grep -q 'imagePullPolicy: IfNotPresent'
! echo "${kind_out}" | grep -q 'image: "uno-arena/identity@"'
echo "${kind_out}" | grep -q 'name: "uno-arena-local-credentials"'
echo "${kind_out}" | grep -q 'DATABASE_URL'
echo "${kind_out}" | grep -q 'identity-database-url'
echo "${kind_out}" | grep -q 'OIDC_ISSUER_URL'
echo "${kind_out}" | grep -q 'DEPLOYMENT_ENV'
# No public Service type / NodePort / LoadBalancer in kind render.
! echo "${kind_out}" | grep -E 'type:\s*(NodePort|LoadBalancer)'

# Digest-absent staging/prod must fail closed.
if "${HELM}" template identity-staging "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" >/dev/null 2>&1; then
  echo "staging without digest must fail" >&2
  exit 1
fi
if "${HELM}" template identity-prod "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" >/dev/null 2>&1; then
  echo "production without digest must fail" >&2
  exit 1
fi

if "${HELM}" template identity-staging-tag "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with tag and empty digest must fail" >&2
  exit 1
fi
if "${HELM}" template identity-prod-tag "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" \
  --set image.tag=latest >/dev/null 2>&1; then
  echo "production with tag and empty digest must fail" >&2
  exit 1
fi

if "${HELM}" template identity-kind-string-false "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set-string kind=false --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with --set-string kind=false and tag must fail" >&2
  exit 1
fi
if "${HELM}" template identity-kind-bool-false "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set kind=false --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with --set kind=false and tag must fail" >&2
  exit 1
fi

if "${HELM}" template identity-staging-kind-override "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set kind=true --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with --set kind=true and tag must still fail (DEPLOYMENT_ENV!=local)" >&2
  exit 1
fi
if "${HELM}" template identity-prod-kind-override "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" \
  --set kind=true --set image.tag=latest >/dev/null 2>&1; then
  echo "production with --set kind=true and tag must still fail (DEPLOYMENT_ENV!=local)" >&2
  exit 1
fi
if "${HELM}" template identity-base-kind-only "${CHART}" -f "${CHART}/values.yaml" \
  --set kind=true --set image.tag=latest --set image.digest= >/dev/null 2>&1; then
  echo "base values with kind=true but DEPLOYMENT_ENV!=local must fail" >&2
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
  if "${HELM}" template identity-bad-digest "${CHART}" -f "${CHART}/values.yaml" \
    --set image.digest="${bad}" --set image.tag= >/dev/null 2>&1; then
    echo "malformed digest must fail: ${bad}" >&2
    exit 1
  fi
done

pin_out="$("${HELM}" template identity-pin "${CHART}" -f "${CHART}/values.yaml" \
  --set image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --set image.tag=)"
echo "${pin_out}" | grep -q 'image: "registry.gitlab.com/group/uno-arena/identity@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"'

pin_tag_staging="$("${HELM}" template identity-pin-staging "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set image.digest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
  --set image.tag=latest)"
echo "${pin_tag_staging}" | grep -q 'image: "registry.gitlab.com/group/uno-arena/identity@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"'
! echo "${pin_tag_staging}" | grep -q 'image: "registry.gitlab.com/group/uno-arena/identity:latest"'

pin_tag_prod="$("${HELM}" template identity-pin-prod "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" \
  --set image.digest=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc \
  --set image.tag=latest)"
echo "${pin_tag_prod}" | grep -q 'image: "registry.gitlab.com/group/uno-arena/identity@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"'
! echo "${pin_tag_prod}" | grep -q 'image: "registry.gitlab.com/group/uno-arena/identity:latest"'

echo "ok identity-helm"
