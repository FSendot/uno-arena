#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
expected="${SCRIPT_DIR}/foundation/desired.yaml"
tmp="$(mktemp)"
trap 'rm -f "${tmp}"' EXIT
"${SCRIPT_DIR}/render-foundation.sh" "${tmp}"
cmp -s "${tmp}" "${expected}" || {
  echo "local-production foundation is stale; run render-foundation.sh ${expected}" >&2
  diff -u "${expected}" "${tmp}" >&2 || true
  exit 1
}
echo "ok local-production-gitops-foundation"
