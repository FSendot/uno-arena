#!/usr/bin/env bash
# Offline structure checks for Identity kind deploy + live adapter scripts.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

fail=0
check() {
  local file="$1"
  local needle="$2"
  if ! grep -qF "${needle}" "${file}"; then
    echo "FAIL: ${file} missing ${needle}" >&2
    fail=1
  fi
}

DEPLOY="${SCRIPT_DIR}/deploy-identity.sh"
LIVE="${SCRIPT_DIR}/test-identity-adapter.sh"
PF="${SCRIPT_DIR}/port-forward-identity.sh"
STRUCT="${SCRIPT_DIR}/test-identity-adapter-structure.sh"

for f in "${DEPLOY}" "${LIVE}" "${PF}" "${STRUCT}"; do
  [[ -f "${f}" ]] || { echo "FAIL: missing ${f}" >&2; fail=1; }
done

check "${DEPLOY}" "helm upgrade --install"
check "${DEPLOY}" "values.kind.yaml"
check "${DEPLOY}" "assert_kind_context"
! grep -qE 'type:\s*(NodePort|LoadBalancer)' "${DEPLOY}" || { echo "FAIL: deploy must not set public Service type" >&2; fail=1; }

check "${PF}" 'LOCAL_PORT_SPEC="0:'
check "${PF}" "jobs -pr"
check "${PF}" "trap cleanup EXIT"
check "${PF}" "port-forward --address=127.0.0.1"

check "${LIVE}" "IDENTITY_BASE_URL"
check "${LIVE}" "/v1/auth/login"
check "${LIVE}" "outbox_events"
check "${LIVE}" "port-forward-identity"
check "${PF}" "LOCAL_PORT"

# No static Identity Deployment duplicate under manifests (Helm-only).
if find "${MANIFESTS_DIR}" -type f \( -name '*identity*deploy*' -o -name '*identity*deployment*' \) 2>/dev/null | grep -q .; then
  echo "FAIL: static Identity Deployment under manifests; use Helm accept script" >&2
  fail=1
fi
if grep -R --include='*.yaml' -l 'kind: Deployment' "${MANIFESTS_DIR}" 2>/dev/null | xargs grep -l 'name: identity' 2>/dev/null | grep -v keycloak | grep -q .; then
  echo "FAIL: found static identity Deployment manifest" >&2
  fail=1
fi

CHART="${REPO_ROOT}/services/identity/helm/identity"
[[ -f "${CHART}/values.kind.yaml" ]] || { echo "FAIL: missing values.kind.yaml" >&2; fail=1; }
[[ -f "${CHART}/helm-test.sh" ]] || { echo "FAIL: missing helm-test.sh" >&2; fail=1; }
[[ -f "${CHART}/templates/_helpers.tpl" ]] || { echo "FAIL: missing _helpers.tpl" >&2; fail=1; }

[[ "${fail}" -eq 0 ]] || exit 1
echo "ok identity-adapter-structure"
