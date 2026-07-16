#!/usr/bin/env bash
# Operator-only bootstrap. Uses verified artifacts and renders fixed inventory inputs.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
require_cmd helm
require_cmd rg
assert_simulator_cluster_exists
assert_exact_context
: "${ARGOCD_GIT_REPO_URL:?set ARGOCD_GIT_REPO_URL to the GitLab repository HTTPS URL}"
: "${ARGOCD_HELM_REPO_URL:?set ARGOCD_HELM_REPO_URL to the GitLab Helm registry HTTPS URL}"
case "${ARGOCD_GIT_REPO_URL}${ARGOCD_HELM_REPO_URL}" in
  *[[:space:]\\\|]*) die "Argo source URLs must not contain whitespace, backslashes, or pipes" ;;
esac
[[ "${ARGOCD_GIT_REPO_URL}" == https://*.git && "${ARGOCD_GIT_REPO_URL}" != *example.invalid* ]] ||
  die "ARGOCD_GIT_REPO_URL must be a non-placeholder HTTPS .git URL"
[[ "${ARGOCD_HELM_REPO_URL}" == https://* && "${ARGOCD_HELM_REPO_URL}" != *example.invalid* && "${ARGOCD_HELM_REPO_URL}" != *PROJECT_ID* ]] ||
  die "ARGOCD_HELM_REPO_URL must be a non-placeholder HTTPS URL"

ARGOCD_ROOT_CHART="${REPO_ROOT}/environments/local-production/argocd"
[[ -f "${ARGOCD_ROOT_CHART}/Chart.yaml" && ! -L "${ARGOCD_ROOT_CHART}" ]] ||
  die "missing fixed local-production Argo root chart"
"${LOCAL_PRODUCTION_DIR}/gitops/verify-foundation.sh"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT
helm template uno-arena-local-production-root "${ARGOCD_ROOT_CHART}" \
  --set-string "gitRepoURL=${ARGOCD_GIT_REPO_URL}" \
  --set-string "helmRepoURL=${ARGOCD_HELM_REPO_URL}" \
  --show-only templates/bootstrap-project.yaml \
  --show-only templates/root-application.yaml >"${tmp_dir}/root-seed.yaml"
if rg -n 'example\.invalid|GROUP/PROJECT|PROJECT_ID' "${tmp_dir}/root-seed.yaml"; then
  die "rendered Argo root seed contains unresolved placeholders"
fi
grep -Fq "name: uno-arena-local-production-root" "${tmp_dir}/root-seed.yaml" ||
  die "rendered Argo root Application missing"
EXPECTED_GIT_REPO_URL="${ARGOCD_GIT_REPO_URL}" ruby -ryaml -e '
  documents = YAML.load_stream(File.read(ARGV.fetch(0))).compact
  root = documents.find do |document|
    document["kind"] == "Application" &&
      document.dig("metadata", "name") == "uno-arena-local-production-root"
  end
  abort "rendered Argo root Application missing" unless root
  actual = root.dig("spec", "source", "repoURL")
  abort "rendered Argo root repository mismatch" unless actual == ENV.fetch("EXPECTED_GIT_REPO_URL")
' "${tmp_dir}/root-seed.yaml"

"${SCRIPT_DIR}/install-argocd-core.sh"
"${SCRIPT_DIR}/configure-argocd-repositories.sh"
# Argo CD 3.4 repo-server can retain a cached client for a repository URL after
# its Secret changes. Recycle it before asking the root Application to compare.
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd rollout restart \
  deployment/argocd-repo-server
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd rollout status \
  deployment/argocd-repo-server --timeout=300s
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" apply -f "${tmp_dir}/root-seed.yaml"
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd wait \
  --for=jsonpath='{.status.sync.status}'=Synced \
  application/uno-arena-local-production-root --timeout=300s
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd wait \
  --for=jsonpath='{.status.health.status}'=Healthy \
  application/uno-arena-local-production-root --timeout=300s
echo "ok local-production-bootstrap-argocd context=${LOCAL_PRODUCTION_CONTEXT}"
