#!/usr/bin/env bash
# Offline semantic validation using the chart's digest-pinned runtime images.
set -euo pipefail
CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HELM="${HELM:-helm}"
for command in "${HELM}" docker ruby; do
  command -v "${command}" >/dev/null 2>&1 || { echo "required command not found: ${command}" >&2; exit 1; }
done

prometheus_image="$(ruby -ryaml -e 'puts YAML.safe_load(File.read(ARGV.fetch(0))).dig("images", "prometheus")' "${CHART}/values.yaml")"
alertmanager_image="$(ruby -ryaml -e 'puts YAML.safe_load(File.read(ARGV.fetch(0))).dig("images", "alertmanager")' "${CHART}/values.yaml")"
for image in "${prometheus_image}" "${alertmanager_image}"; do
  [[ "${image}" == *@sha256:* ]] || { echo "validator image must be digest-pinned: ${image}" >&2; exit 1; }
  if ! inspect_error="$(docker image inspect "${image}" 2>&1)"; then
    if grep -Fqi 'no such image' <<<"${inspect_error}"; then
      echo "validator image is not cached: ${image}" >&2
      echo "run ${CHART}/validation/fetch-images.sh explicitly, then retry" >&2
    else
      echo "cannot inspect the local Docker image cache: ${inspect_error}" >&2
    fi
    exit 1
  fi
done

docker run --rm --pull=never --network=none \
  --entrypoint=/bin/sh \
  "${prometheus_image}" -ec '
    for command in wget awk sed grep date; do
      command -v "${command}" >/dev/null || { echo "missing evidence command: ${command}" >&2; exit 1; }
    done
  '

tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT

render_profile() {
  local profile="$1" values="$2"
  shift 2
  local profile_dir="${tmp}/${profile}"
  mkdir -p "${profile_dir}/prometheus/rules" "${profile_dir}/alertmanager/receiver" "${profile_dir}/serviceaccount"
  "${HELM}" template "${profile}" "${CHART}" -n observability -f "${values}" "$@" >"${profile_dir}/rendered.yaml"
  ruby -ryaml -e '
    documents=YAML.load_stream(File.read(ARGV.fetch(0))).compact
    mappings={
      "prometheus-config" => [ARGV.fetch(1), "prometheus.yml"],
      "prometheus-rules" => [ARGV.fetch(2), "uno-arena-rules.yml"],
      "alertmanager-config" => [ARGV.fetch(3), "alertmanager.yml"]
    }
    mappings.each do |name, (path, key)|
      config=documents.find { |document| document["kind"] == "ConfigMap" && document.dig("metadata", "name") == name }
      abort "missing rendered ConfigMap #{name}" unless config
      File.write(path, config.fetch("data").fetch(key))
    end
    job=documents.find { |document| document["kind"] == "Job" && document.dig("metadata", "name") == "uno-arena-observability-postsync-evidence" }
    File.write(ARGV.fetch(4), job.dig("spec", "template", "spec", "containers", 0, "args", 0)) if job
  ' "${profile_dir}/rendered.yaml" \
    "${profile_dir}/prometheus/prometheus.yml" \
    "${profile_dir}/prometheus/rules/uno-arena-rules.yml" \
    "${profile_dir}/alertmanager/alertmanager.yml" \
    "${profile_dir}/postsync-evidence.sh"
  printf '%s\n' 'https://receiver.invalid/alerts' >"${profile_dir}/alertmanager/receiver/webhook-url"
  printf '%s\n' 'offline-validation-token' >"${profile_dir}/serviceaccount/token"
  : >"${profile_dir}/serviceaccount/ca.crt"

  docker run --rm --pull=never --network=none \
    --entrypoint=/bin/promtool \
    -v "${profile_dir}/prometheus:/etc/prometheus:ro" \
    -v "${profile_dir}/serviceaccount:/var/run/secrets/kubernetes.io/serviceaccount:ro" \
    "${prometheus_image}" check config /etc/prometheus/prometheus.yml
  docker run --rm --pull=never --network=none \
    --entrypoint=/bin/promtool \
    -v "${profile_dir}/prometheus:/etc/prometheus:ro" \
    "${prometheus_image}" check rules /etc/prometheus/rules/uno-arena-rules.yml
  docker run --rm --pull=never --network=none \
    --entrypoint=/bin/amtool \
    -v "${profile_dir}/alertmanager:/etc/alertmanager:ro" \
    "${alertmanager_image}" check-config /etc/alertmanager/alertmanager.yml
  if [[ -f "${profile_dir}/postsync-evidence.sh" ]]; then
    docker run --rm --pull=never --network=none \
      --entrypoint=/bin/sh \
      -v "${profile_dir}:/validation:ro" \
      "${prometheus_image}" -n /validation/postsync-evidence.sh
  fi
}

render_profile kind "${CHART}/values.kind.yaml"
render_profile local-production "${CHART}/values.local-production.yaml"
render_profile production "${CHART}/values.production.yaml" \
  --set storage.s3.region=us-east-1 \
  --set storage.s3.lokiBucket=validation-loki \
  --set storage.s3.tempoBucket=validation-tempo

echo "ok observability-semantic-config"
