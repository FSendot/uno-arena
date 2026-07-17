#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
LOCAL="${ROOT}/infrastructure/local-production"

for script in "${LOCAL}/bin/configure-argocd-repositories.sh" \
  "${LOCAL}/bin/generate-argocd-ci-token.sh" "${LOCAL}/bin/install-argocd-core.sh" \
  "${LOCAL}/bin/bootstrap-argocd.sh" "${LOCAL}/bin/acceptance.sh" \
  "${LOCAL}/bin/check-argocd-controller-network.sh"; do
  bash -n "${script}"
done

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT
fake_bin="${tmp_dir}/bin"
mkdir -p "${fake_bin}"

cat >"${fake_bin}/kind" <<'FAKE_KIND'
#!/usr/bin/env bash
case "$*" in
  "get clusters") echo uno-arena-production ;;
  "get kubeconfig --name uno-arena-production") echo 'fake kind kubeconfig' ;;
  *) exit 1 ;;
esac
FAKE_KIND
cat >"${fake_bin}/kubectl" <<'FAKE_KUBECTL'
#!/usr/bin/env bash
if [[ "$*" == "config current-context" ]]; then
  echo kind-uno-arena-production
  exit 0
fi
if [[ "$*" == config\ view* ]]; then
  if [[ "$*" == *'--kubeconfig=/dev/stdin'* ]]; then
    cat >/dev/null
  fi
  case "$*" in
    *'.contexts['*) printf '%s' 'kind-uno-arena-production' ;;
    *'cluster.server}'*) printf '%s' 'https://127.0.0.1:6443' ;;
    *'certificate-authority-data}'*) printf '%s' 'fake-kind-ca' ;;
    *) exit 1 ;;
  esac
  exit 0
fi
if [[ "$*" == *"apply"*"-f -"* ]]; then
  ruby -e 'File.open(ENV.fetch("CAPTURE_FILE"), "a", 0600) { |file| file.puts(STDIN.read) }'
  exit 0
fi
if [[ "$*" == *"-n argocd exec argocd-application-controller-0 --"* &&
  "${FAIL_ARGOCD_CONTROLLER_NETWORK:-0}" == "1" ]]; then
  if [[ -n "${KUBECTL_ARGS_FILE:-}" ]]; then
    printf '%s\n' "$*" >>"${KUBECTL_ARGS_FILE}"
  fi
  exit 124
fi
if [[ "$*" == *"-n argocd get service argocd-redis"*".spec.clusterIP"* ]]; then
  if [[ -n "${KUBECTL_ARGS_FILE:-}" ]]; then
    printf '%s\n' "$*" >>"${KUBECTL_ARGS_FILE}"
  fi
  printf '%s' '10.96.0.10'
  exit 0
fi
if [[ -n "${KUBECTL_ARGS_FILE:-}" ]]; then
  printf '%s\n' "$*" >>"${KUBECTL_ARGS_FILE}"
  exit 0
fi
exit 1
FAKE_KUBECTL
cat >"${fake_bin}/argocd" <<'FAKE_ARGOCD'
#!/usr/bin/env bash
printf '%s\n' "$*" >"${ARGOCD_ARGS_FILE}"
printf '%s' 'generated-ci-account-token'
FAKE_ARGOCD
cat >"${fake_bin}/sleep" <<'FAKE_SLEEP'
#!/usr/bin/env bash
exit 0
FAKE_SLEEP
chmod 700 "${fake_bin}/kind" "${fake_bin}/kubectl" "${fake_bin}/argocd" "${fake_bin}/sleep"

capture_file="${tmp_dir}/repositories.jsonl"
operator_log="${tmp_dir}/repositories.log"
git_password='git-read-secret-value'
helm_password='helm-read-secret-value'
PATH="${fake_bin}:${PATH}" CAPTURE_FILE="${capture_file}" \
  ARGOCD_GIT_REPO_URL='https://gitlab.test/group/project.git' \
  ARGOCD_HELM_REPO_URL='https://gitlab.test/api/v4/projects/17/packages/helm/stable' \
  ARGOCD_GIT_READ_USERNAME='git-reader' ARGOCD_GIT_READ_PASSWORD="${git_password}" \
  ARGOCD_HELM_READ_USERNAME='helm-reader' ARGOCD_HELM_READ_PASSWORD="${helm_password}" \
  "${LOCAL}/bin/configure-argocd-repositories.sh" >"${operator_log}" 2>&1

if rg -F -e "${git_password}" -e "${helm_password}" "${operator_log}"; then
  echo "repository helper printed a credential" >&2
  exit 1
fi
ruby -rjson -e '
  documents = File.readlines(ARGV.fetch(0), chomp: true).reject(&:empty?).map { |line| JSON.parse(line) }
  abort "expected two separate repository Secrets" unless documents.length == 2
  by_name = documents.to_h { |document| [document.dig("metadata", "name"), document] }
  expected = {
    "uno-arena-git-repository" => ["git", "git-reader", "git-read-secret-value"],
    "uno-arena-helm-repository" => ["helm", "helm-reader", "helm-read-secret-value"]
  }
  expected.each do |name, (type, username, password)|
    document = by_name.fetch(name)
    abort "wrong repository label" unless document.dig("metadata", "labels", "argocd.argoproj.io/secret-type") == "repository"
    abort "wrong repository type" unless document.dig("stringData", "type") == type
    abort "wrong repository username" unless document.dig("stringData", "username") == username
    abort "wrong repository password" unless document.dig("stringData", "password") == password
  end
' "${capture_file}"

bootstrap_log="${tmp_dir}/bootstrap.log"
bootstrap_git_url='https://gitlab.com/itba-73-40-microservicios/alumnos/2026-s1/grupo-6/uno-arena.git'
bootstrap_helm_url='https://gitlab.com/api/v4/projects/17/packages/helm/stable'
if ! PATH="${fake_bin}:${PATH}" CAPTURE_FILE="${capture_file}" \
  KUBECTL_ARGS_FILE="${tmp_dir}/bootstrap-kubectl.args" \
  ARGOCD_GIT_REPO_URL="${bootstrap_git_url}" ARGOCD_HELM_REPO_URL="${bootstrap_helm_url}" \
  ARGOCD_GIT_READ_USERNAME='git-reader' ARGOCD_GIT_READ_PASSWORD="${git_password}" \
  ARGOCD_HELM_READ_USERNAME='helm-reader' ARGOCD_HELM_READ_PASSWORD="${helm_password}" \
  "${LOCAL}/bin/bootstrap-argocd.sh" >"${bootstrap_log}" 2>&1; then
  sed -e "s/${git_password}/REDACTED/g" -e "s/${helm_password}/REDACTED/g" \
    "${bootstrap_log}" >&2
  exit 1
fi
grep -Fq 'ok local-production-bootstrap-argocd' "${bootstrap_log}"
if rg -F -e "${git_password}" -e "${helm_password}" "${bootstrap_log}"; then
  echo "bootstrap helper printed a credential" >&2
  exit 1
fi
ruby -e '
  commands = File.readlines(ARGV.fetch(0), chomp: true)
  restart = commands.index { |command| command.include?("rollout restart deployment/argocd-repo-server") }
  rollout = commands.index { |command| command.include?("rollout status deployment/argocd-repo-server") }
  root_apply = commands.index { |command| command.include?("root-seed.yaml") }
  foundation_wait = commands.index do |command|
    command.include?("wait") && command.include?("application/uno-arena-local-production-foundations")
  end
  abort "bootstrap must restart repo-server after writing repository Secrets" unless restart
  abort "bootstrap must wait for repo-server after restart" unless rollout && restart < rollout
  abort "bootstrap must refresh repo-server before applying the root Application" unless root_apply && rollout < root_apply
  abort "bootstrap must adopt the root seed with server-side apply" unless commands.fetch(root_apply).include?("apply --server-side -f")
  abort "bootstrap must wait for the repository-owned foundation" unless foundation_wait && root_apply < foundation_wait
' "${tmp_dir}/bootstrap-kubectl.args"

failed_bootstrap_log="${tmp_dir}/failed-bootstrap.log"
failed_bootstrap_args="${tmp_dir}/failed-bootstrap-kubectl.args"
if PATH="${fake_bin}:${PATH}" CAPTURE_FILE="${capture_file}" KUBECTL_ARGS_FILE="${failed_bootstrap_args}" \
  FAIL_ARGOCD_CONTROLLER_NETWORK=1 \
  ARGOCD_GIT_REPO_URL="${bootstrap_git_url}" ARGOCD_HELM_REPO_URL="${bootstrap_helm_url}" \
  ARGOCD_GIT_READ_USERNAME='git-reader' ARGOCD_GIT_READ_PASSWORD="${git_password}" \
  ARGOCD_HELM_READ_USERNAME='helm-reader' ARGOCD_HELM_READ_PASSWORD="${helm_password}" \
  "${LOCAL}/bin/bootstrap-argocd.sh" >"${failed_bootstrap_log}" 2>&1; then
  echo "bootstrap continued after the Argo controller network probe failed" >&2
  exit 1
fi
grep -Fq 'Argo application controller cannot reach DNS, Kubernetes API, and Redis' \
  "${failed_bootstrap_log}"
grep -Fq -- '-n argocd exec argocd-application-controller-0 --' "${failed_bootstrap_args}"
if grep -Fq 'root-seed.yaml' "${failed_bootstrap_args}"; then
  echo "bootstrap seeded the root Application after the controller network probe failed" >&2
  exit 1
fi

kubectl_args_file="${tmp_dir}/kubectl.args"
PATH="${fake_bin}:${PATH}" KUBECTL_ARGS_FILE="${kubectl_args_file}" \
  "${LOCAL}/bin/configure-argocd-ci-account.sh" >"${tmp_dir}/account.log" 2>&1
grep -Fq 'accounts.ci-readonly' "${kubectl_args_file}"
grep -Fq 'p, ci-readonly, applications, get, uno-arena-workloads/*, allow' "${kubectl_args_file}"
grep -Fq 'p, ci-readonly, applications, get, uno-arena-stateful-platform/*, allow' "${kubectl_args_file}"
grep -Fq 'p, ci-readonly, applications, get, uno-arena-bootstrap/*, allow' "${kubectl_args_file}"
grep -Fq 'p, ci-readonly, applications, get, uno-arena-foundations/*, allow' "${kubectl_args_file}"
if rg -n 'applications, (create|delete|sync|update)|clusters|repositories|secrets' "${kubectl_args_file}"; then
  echo "ci-readonly account received mutating or cluster-wide RBAC" >&2
  exit 1
fi

ca_file="${tmp_dir}/local-ca.crt"
printf '%s\n' 'test-ca' >"${ca_file}"
token_file="${tmp_dir}/ci-account.token"
token_log="${tmp_dir}/token.log"
argocd_args_file="${tmp_dir}/argocd.args"
operator_token='operator-auth-secret-value'
PATH="${fake_bin}:${PATH}" ARGOCD_ARGS_FILE="${argocd_args_file}" \
  ARGOCD_OPERATOR_AUTH_TOKEN="${operator_token}" ARGOCD_CA_FILE="${ca_file}" \
  ARGOCD_CI_TOKEN_OUTPUT="${token_file}" \
  "${LOCAL}/bin/generate-argocd-ci-token.sh" >"${token_log}" 2>&1

[[ "$(<"${token_file}")" == 'generated-ci-account-token' ]] || { echo "wrong generated token output" >&2; exit 1; }
[[ "$(ruby -e 'printf "%o", File.stat(ARGV.fetch(0)).mode & 0777' "${token_file}")" == "600" ]] || {
  echo "generated token file must be mode 0600" >&2
  exit 1
}
if rg -F -e 'generated-ci-account-token' -e "${operator_token}" "${token_log}" "${argocd_args_file}"; then
  echo "token helper printed or passed a token as a command argument" >&2
  exit 1
fi
grep -Fqx 'account generate-token --account ci-readonly --expires-in 168h' "${argocd_args_file}"

invalid_ttl_log="${tmp_dir}/invalid-ttl.log"
invalid_token_file="${tmp_dir}/invalid-ttl.token"
if PATH="${fake_bin}:${PATH}" ARGOCD_ARGS_FILE="${argocd_args_file}" \
  ARGOCD_OPERATOR_AUTH_TOKEN="${operator_token}" ARGOCD_CA_FILE="${ca_file}" \
  ARGOCD_CI_TOKEN_OUTPUT="${invalid_token_file}" ARGOCD_CI_TOKEN_TTL='0h' \
  "${LOCAL}/bin/generate-argocd-ci-token.sh" >"${invalid_ttl_log}" 2>&1; then
  echo "token helper accepted an invalid zero TTL" >&2
  exit 1
fi
grep -Fq 'ARGOCD_CI_TOKEN_TTL must be a positive duration in minutes or hours' "${invalid_ttl_log}"
[[ ! -e "${invalid_token_file}" ]] || { echo "invalid TTL created a token file" >&2; exit 1; }

grep -Fq 'uno-arena-git-repository' "${LOCAL}/bin/acceptance.sh"
grep -Fq 'uno-arena-helm-repository' "${LOCAL}/bin/acceptance.sh"
grep -Fq 'gitlab-registry' "${LOCAL}/bin/acceptance.sh"
grep -Fq 'ParametersGenerated' "${LOCAL}/bin/acceptance.sh"
grep -Fq 'ResourcesUpToDate' "${LOCAL}/bin/acceptance.sh"
grep -Fq 'EXPECTED_ARGO_APPLICATIONS' "${LOCAL}/bin/acceptance.sh"
grep -Fq 'if [[ "${pre_source}" == false ]]' "${LOCAL}/bin/acceptance.sh"
grep -Fq 'uno-arena-bootstrap uno-arena-foundations uno-arena-workloads uno-arena-stateful-platform' \
  "${LOCAL}/bin/acceptance.sh"
grep -Fq 'application=uno-arena-local-production-root' "${LOCAL}/bin/acceptance.sh"
grep -Fq 'application=uno-arena-local-production-foundations' "${LOCAL}/bin/acceptance.sh"
grep -Fq 'uno-arena-local-production-services uno-arena-local-production-platform' "${LOCAL}/bin/acceptance.sh"
grep -Fq 'wait_for_applicationset uno-arena-local-production-services' "${LOCAL}/bin/acceptance.sh"
grep -Fq 'wait_for_applicationset uno-arena-local-production-platform' "${LOCAL}/bin/acceptance.sh"
grep -Fq 'validate-argocd-control-plane-application' "${LOCAL}/bin/acceptance.sh"
grep -Fq 'argocd app get "${application}" --core --app-namespace argocd --output json' \
  "${LOCAL}/bin/acceptance.sh"
grep -Fq 'get appprojects.argoproj.io' "${LOCAL}/bin/acceptance.sh"
if grep -Fq 'applicationprojects.argoproj.io' "${LOCAL}/bin/acceptance.sh"; then
  echo "acceptance uses the nonexistent applicationprojects resource type" >&2
  exit 1
fi
grep -Fqx '  "${SCRIPT_DIR}/acceptance.sh" --foundation-only' "${LOCAL}/bin/install-foundations.sh"
grep -Fqx '  "${SCRIPT_DIR}/acceptance.sh" --pre-source' "${LOCAL}/bin/install-foundations.sh"
grep -Fq -- '--defer-private-sources' "${LOCAL}/bin/install-foundations.sh"
grep -Fq '"${SCRIPT_DIR}/install-argocd-core.sh"' "${LOCAL}/bin/bootstrap-argocd.sh"
grep -Fq 'templates/bootstrap-project.yaml' "${LOCAL}/bin/bootstrap-argocd.sh"
grep -Fq 'templates/root-application.yaml' "${LOCAL}/bin/bootstrap-argocd.sh"
if grep -Eq 'app-project\.yaml|services-applicationset\.yaml|platform-applicationset\.yaml' \
  "${LOCAL}/bin/bootstrap-argocd.sh"; then
  echo "operator bootstrap still imperatively references root-owned Argo children" >&2
  exit 1
fi

echo "ok local-production-argocd-operator-contracts"
