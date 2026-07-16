#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
LOCAL="${ROOT}/infrastructure/local-production"
SCHEMA="${LOCAL}/charts/platform-secrets/seed.schema.json"

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
printf '%s\n' "$*" >>"$KUBECTL_LOG"
if [[ "$*" == "config current-context" ]]; then
  echo "${CURRENT_CONTEXT:-kind-uno-arena-production}"
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
if [[ "$*" == "--context kind-uno-arena-production get namespace secret-seed" ]]; then
  exit 0
fi
if [[ "$*" == "--context kind-uno-arena-production apply -f "* ]]; then
  exit 0
fi
exit 1
FAKE_KUBECTL
chmod 0700 "${fake_bin}/kind" "${fake_bin}/kubectl"

template="${tmp_dir}/seed-template.yaml"
complete="${tmp_dir}/seed-complete.yaml"
missing="${tmp_dir}/seed-missing.yaml"
wrong_name="${tmp_dir}/seed-wrong-name.yaml"
"${LOCAL}/bin/render-secret-seed-template" >"${template}"
ruby -rjson -ryaml -e '
  schema = JSON.parse(File.read(ARGV.fetch(0)))
  document = YAML.load_file(ARGV.fetch(1))
  abort "template keys differ from schema" unless document.fetch("stringData").keys == schema.fetch("required")
  document["stringData"].transform_values! { |value| raise "unexpected template value" unless value == "REPLACE_WITH_SECRET"; "test-secret-value-for-local-validation" }
  document["stringData"]["alertmanager-webhook-url"] = "http://alert-webhook-sink.observability.svc.cluster.local:8080/alerts"
  File.write(ARGV.fetch(2), YAML.dump(document))
  document["stringData"].delete("room-service-credential")
  File.write(ARGV.fetch(3), YAML.dump(document))
  document = YAML.load_file(ARGV.fetch(2))
  document.fetch("metadata")["name"] = "wrong-seed"
  File.write(ARGV.fetch(4), YAML.dump(document))
' "${SCHEMA}" "${template}" "${complete}" "${missing}" "${wrong_name}"

run_seed() {
  local manifest="$1"
  local output="$2"
  CURRENT_CONTEXT="${CURRENT_CONTEXT:-}" PATH="${fake_bin}:${PATH}" KUBECTL_LOG="${tmp_dir}/kubectl.log" \
    SECRET_SEED_MANIFEST="${manifest}" "${LOCAL}/bin/seed-secrets.sh" >"${output}" 2>&1
}

: >"${tmp_dir}/kubectl.log"
if run_seed "${missing}" "${tmp_dir}/missing.log"; then
  echo "seed helper accepted a manifest missing a mapped property" >&2
  exit 1
fi
grep -Fq 'seed Secret is missing mapped properties: room-service-credential' "${tmp_dir}/missing.log"
if grep -Fq 'apply -f' "${tmp_dir}/kubectl.log"; then
  echo "missing-property validation reached kubectl apply" >&2
  exit 1
fi

: >"${tmp_dir}/kubectl.log"
if run_seed "${template}" "${tmp_dir}/placeholder.log"; then
  echo "seed helper accepted generated placeholders" >&2
  exit 1
fi
grep -Fq 'seed Secret still has template placeholders:' "${tmp_dir}/placeholder.log"
if grep -Fq 'apply -f' "${tmp_dir}/kubectl.log"; then
  echo "placeholder validation reached kubectl apply" >&2
  exit 1
fi

: >"${tmp_dir}/kubectl.log"
if run_seed "${wrong_name}" "${tmp_dir}/wrong-name.log"; then
  echo "seed helper accepted the wrong source Secret name" >&2
  exit 1
fi
grep -Fq 'seed Secret name must be uno-arena-local-credentials' "${tmp_dir}/wrong-name.log"
if grep -Fq 'apply -f' "${tmp_dir}/kubectl.log"; then
  echo "wrong-name validation reached kubectl apply" >&2
  exit 1
fi

: >"${tmp_dir}/kubectl.log"
CURRENT_CONTEXT='kind-wrong-cluster'
export CURRENT_CONTEXT
if run_seed "${complete}" "${tmp_dir}/context.log"; then
  echo "seed helper accepted the wrong active context" >&2
  exit 1
fi
unset CURRENT_CONTEXT
grep -Fq "expected kind-uno-arena-production" "${tmp_dir}/context.log"
if grep -Fq 'apply -f' "${tmp_dir}/kubectl.log"; then
  echo "wrong-context validation reached kubectl apply" >&2
  exit 1
fi

: >"${tmp_dir}/kubectl.log"
run_seed "${complete}" "${tmp_dir}/complete.log"
grep -Fq -- '--context kind-uno-arena-production get namespace secret-seed' "${tmp_dir}/kubectl.log"
grep -Fq -- "--context kind-uno-arena-production apply -f ${complete}" "${tmp_dir}/kubectl.log"
grep -Fq 'ok local-production-seed-secrets context=kind-uno-arena-production' "${tmp_dir}/complete.log"

echo "ok local-production-seed-secrets-contract"
