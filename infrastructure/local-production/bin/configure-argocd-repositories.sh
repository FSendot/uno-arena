#!/usr/bin/env bash
# Operator-only creation of Argo CD credentials for the private GitLab sources.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

for command in kubectl ruby; do
  require_cmd "${command}"
done
assert_simulator_cluster_exists
assert_exact_context

: "${ARGOCD_GIT_REPO_URL:?ARGOCD_GIT_REPO_URL is required}"
: "${ARGOCD_HELM_REPO_URL:?ARGOCD_HELM_REPO_URL is required}"
: "${ARGOCD_GIT_READ_USERNAME:?ARGOCD_GIT_READ_USERNAME is required}"
: "${ARGOCD_GIT_READ_PASSWORD:?ARGOCD_GIT_READ_PASSWORD is required}"
: "${ARGOCD_HELM_READ_USERNAME:?ARGOCD_HELM_READ_USERNAME is required}"
: "${ARGOCD_HELM_READ_PASSWORD:?ARGOCD_HELM_READ_PASSWORD is required}"

apply_repository_secret() {
  local secret_name="$1"
  local repository_type="$2"
  local repository_url="$3"
  local username="$4"
  local password="$5"

  SECRET_NAME="${secret_name}" REPOSITORY_TYPE="${repository_type}" \
    REPOSITORY_URL="${repository_url}" REPOSITORY_USERNAME="${username}" \
    REPOSITORY_PASSWORD="${password}" ruby -rjson -e '
      document = {
        apiVersion: "v1",
        kind: "Secret",
        metadata: {
          name: ENV.fetch("SECRET_NAME"),
          namespace: "argocd",
          labels: {"argocd.argoproj.io/secret-type" => "repository"}
        },
        type: "Opaque",
        stringData: {
          type: ENV.fetch("REPOSITORY_TYPE"),
          url: ENV.fetch("REPOSITORY_URL"),
          username: ENV.fetch("REPOSITORY_USERNAME"),
          password: ENV.fetch("REPOSITORY_PASSWORD")
        }
      }
      STDOUT.write(JSON.generate(document))
    ' | kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" apply -f - >/dev/null
}

apply_repository_secret uno-arena-git-repository git "${ARGOCD_GIT_REPO_URL}" \
  "${ARGOCD_GIT_READ_USERNAME}" "${ARGOCD_GIT_READ_PASSWORD}"
apply_repository_secret uno-arena-helm-repository helm "${ARGOCD_HELM_REPO_URL}" \
  "${ARGOCD_HELM_READ_USERNAME}" "${ARGOCD_HELM_READ_PASSWORD}"

echo "ok local-production-argocd-repositories context=${LOCAL_PRODUCTION_CONTEXT}"
