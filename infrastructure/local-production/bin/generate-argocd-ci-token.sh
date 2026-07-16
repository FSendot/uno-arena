#!/usr/bin/env bash
# Operator-only generation of the scoped GitLab CI account token. Token goes to a mode-0600 file.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

for command in argocd kubectl; do
  require_cmd "${command}"
done
assert_simulator_cluster_exists
assert_exact_context

: "${ARGOCD_OPERATOR_AUTH_TOKEN:?ARGOCD_OPERATOR_AUTH_TOKEN is required}"
: "${ARGOCD_CA_FILE:?ARGOCD_CA_FILE is required}"
: "${ARGOCD_CI_TOKEN_OUTPUT:?ARGOCD_CI_TOKEN_OUTPUT is required}"
ARGOCD_CI_TOKEN_TTL="${ARGOCD_CI_TOKEN_TTL:-168h}"
[[ "${ARGOCD_CI_TOKEN_TTL}" =~ ^[1-9][0-9]*(m|h)$ ]] ||
  die "ARGOCD_CI_TOKEN_TTL must be a positive duration in minutes or hours"
[[ -f "${ARGOCD_CA_FILE}" && -r "${ARGOCD_CA_FILE}" ]] || die "ARGOCD_CA_FILE must be a readable file"
[[ -d "$(dirname "${ARGOCD_CI_TOKEN_OUTPUT}")" ]] || die "ARGOCD_CI_TOKEN_OUTPUT parent directory does not exist"
[[ ! -e "${ARGOCD_CI_TOKEN_OUTPUT}" && ! -L "${ARGOCD_CI_TOKEN_OUTPUT}" ]] ||
  die "refusing to overwrite ARGOCD_CI_TOKEN_OUTPUT"

token="$(
  ARGOCD_SERVER="127.0.0.1:9443" \
    ARGOCD_AUTH_TOKEN="${ARGOCD_OPERATOR_AUTH_TOKEN}" \
    SSL_CERT_FILE="${ARGOCD_CA_FILE}" \
    argocd account generate-token --account ci-readonly --expires-in "${ARGOCD_CI_TOKEN_TTL}"
)"
[[ -n "${token}" && "${token}" != *$'\n'* ]] || die "Argo CD returned an invalid CI token"

umask 077
printf '%s' "${token}" >"${ARGOCD_CI_TOKEN_OUTPUT}"
unset token
echo "ok local-production-argocd-ci-token output=${ARGOCD_CI_TOKEN_OUTPUT}"
