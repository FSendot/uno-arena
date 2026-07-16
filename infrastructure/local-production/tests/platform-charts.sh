#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
LOCAL="${ROOT}/infrastructure/local-production"
CHARTS="${LOCAL}/charts"
RENDERS="$(mktemp -d)"
PACKAGES="$(mktemp -d)"
trap 'rm -rf "${RENDERS}" "${PACKAGES}"' EXIT

chart_dirs=()
while IFS= read -r chart; do
  chart_dirs+=("${chart}")
done < <(find "${CHARTS}" -mindepth 1 -maxdepth 1 -type d | LC_ALL=C sort)
[[ "${#chart_dirs[@]}" -eq 12 ]] || { echo "expected 12 platform charts" >&2; exit 1; }

for chart in "${chart_dirs[@]}"; do
  name="$(basename "${chart}")"
  grep -Fqx 'version: 0.1.0' "${chart}/Chart.yaml"
  args=()
  if [[ "${name}" == "context-bootstrap" ]]; then
    args=(--set image.repository=registry.example/bootstrap --set image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa)
  fi
  helm lint "${chart}" "${args[@]}" >/dev/null
  helm template "${name}" "${chart}" -n uno-arena "${args[@]}" >"${RENDERS}/${name}.yaml"
  helm package "${chart}" --destination "${PACKAGES}" >/dev/null
done
OBSERVABILITY_CHART="${ROOT}/infrastructure/observability/helm/uno-arena-observability"
helm lint "${OBSERVABILITY_CHART}" -f "${OBSERVABILITY_CHART}/values.local-production.yaml" >/dev/null
helm template observability "${OBSERVABILITY_CHART}" -n observability \
  -f "${OBSERVABILITY_CHART}/values.local-production.yaml" >"${RENDERS}/../observability.yaml"
helm package "${OBSERVABILITY_CHART}" --destination "${PACKAGES}" >/dev/null
[[ "$(find "${PACKAGES}" -type f -name '*.tgz' | wc -l | tr -d ' ')" -eq 13 ]]

ruby -ryaml -e '
  files = ARGV
  docs = files.flat_map { |file| YAML.load_stream(File.read(file)).compact }
  images = docs.flat_map do |doc|
    pod = case doc["kind"]
          when "Deployment", "StatefulSet", "DaemonSet" then doc.dig("spec", "template", "spec")
          when "Job" then doc.dig("spec", "template", "spec")
          end
    (pod && pod["containers"] || []).map { |container| container["image"] }
  end.compact
  abort "all rendered images must be digest pinned: #{images.inspect}" unless images.all? { |image| image.match?(%r{@sha256:[a-f0-9]{64}\z}) }
  abort "charts must not render credential Secrets" if docs.any? { |doc| doc["kind"] == "Secret" }

  pvcs = docs.select { |doc| doc["kind"] == "PersistentVolumeClaim" }
  abort "expected ten retained PVCs, got #{pvcs.length}" unless pvcs.length == 10
  pvcs.each do |pvc|
    abort "PVC must use retain class" unless pvc.dig("spec", "storageClassName") == "uno-arena-local-retain"
    abort "PVC missing Helm keep" unless pvc.dig("metadata", "annotations", "helm.sh/resource-policy") == "keep"
    abort "PVC missing Argo prune protection" unless pvc.dig("metadata", "annotations", "argocd.argoproj.io/sync-options") == "Prune=false"
  end
  storage = docs.find { |doc| doc["kind"] == "StorageClass" }
  abort "Retain StorageClass missing" unless storage&.dig("reclaimPolicy") == "Retain"

  stateful = %w[postgres-identity postgres-room-gameplay postgres-tournament postgres-ranking redis kafka kurrentdb clickhouse keycloak minio]
  stateful.each do |name|
    deployment = docs.find { |doc| doc["kind"] == "Deployment" && doc.dig("metadata", "name") == name }
    abort "missing stateful Deployment #{name}" unless deployment
    abort "#{name} must be prune protected" unless deployment.dig("metadata", "annotations", "argocd.argoproj.io/sync-options") == "Prune=false"
    volumes = deployment.dig("spec", "template", "spec", "volumes") || []
    abort "#{name} data must use PVC" unless volumes.any? { |volume| volume["persistentVolumeClaim"] }
    abort "#{name} must use a dedicated ServiceAccount" unless deployment.dig("spec", "template", "spec", "serviceAccountName") == name
    abort "#{name} must not automount an API token" unless deployment.dig("spec", "template", "spec", "automountServiceAccountToken") == false
  end

  externals = docs.select { |doc| doc["kind"] == "ExternalSecret" }
  registry = externals.find { |doc| doc.dig("metadata", "name") == "gitlab-registry" }
  abort "registry pull Secret type missing" unless registry&.dig("spec", "target", "template", "type") == "kubernetes.io/dockerconfigjson"
  %w[identity-secrets room-gameplay-secrets].each do |target|
    secret = externals.find { |doc| doc.dig("metadata", "name") == target }
    %w[internal-principal-hmac-key-current internal-principal-hmac-key-previous].each do |key|
      mapping = secret.fetch("spec").fetch("data").find { |entry| entry["secretKey"] == key }
      abort "#{target} missing dedicated principal key #{key}" unless mapping&.dig("remoteRef", "property") == key
    end
  end
  alertmanager = externals.find { |doc| doc.dig("metadata", "name") == "uno-arena-local-observability-alertmanager" }
  mapping = alertmanager&.fetch("spec", {})&.fetch("data", [])&.find { |entry| entry["secretKey"] == "webhook-url" }
  abort "local Alertmanager receiver mapping missing" unless mapping&.dig("remoteRef", "property") == "alertmanager-webhook-url"

  hooks = docs.select { |doc| doc["kind"] == "Job" }
  abort "bootstrap Jobs must be replace-safe Argo hooks" unless hooks.all? { |job| job.dig("metadata", "annotations", "argocd.argoproj.io/hook-delete-policy") == "BeforeHookCreation" }
' "${RENDERS}"/*.yaml

ruby -ryaml -e '
  renders = Dir[File.join(ARGV.fetch(0), "*.yaml")]
  workloads = renders.flat_map { |file| YAML.load_stream(File.read(file)).compact }
  policies = YAML.load_stream(File.read(ARGV.fetch(1))).compact.select { |doc| doc["kind"] == "AuthorizationPolicy" }

  job_accounts = {
    "bootstrap-postgres-identity" => "context-bootstrap",
    "bootstrap-postgres-room-gameplay" => "context-bootstrap",
    "bootstrap-postgres-tournament" => "context-bootstrap",
    "bootstrap-postgres-ranking" => "context-bootstrap",
    "bootstrap-clickhouse-analytics" => "context-bootstrap",
    "register-debezium-connectors" => "debezium-connect-registrar",
    "bootstrap-kafka-topics" => "kafka-bootstrap",
    "minio-create-observability-buckets" => "minio-bucket-bootstrap"
  }
  jobs = workloads.select { |doc| doc["kind"] == "Job" }
  abort "platform Job inventory mismatch" unless jobs.map { |job| job.dig("metadata", "name") }.sort == job_accounts.keys.sort
  jobs.each do |job|
    name = job.dig("metadata", "name")
    spec = job.dig("spec", "template", "spec")
    abort "#{name} has the wrong ServiceAccount" unless spec["serviceAccountName"] == job_accounts.fetch(name)
    abort "#{name} must not automount an API token" unless spec["automountServiceAccountToken"] == false
  end

  policy_ports = {
    "postgres-identity-callers" => "5432",
    "postgres-room-gameplay-callers" => "5432",
    "postgres-tournament-callers" => "5432",
    "postgres-ranking-callers" => "5432",
    "redis-callers" => "6379",
    "kafka-callers" => "9092",
    "kurrentdb-callers" => "2113",
    "clickhouse-callers" => "8123",
    "keycloak-callers" => "8080",
    "debezium-connect-callers" => "8083"
  }
  policy_ports.each do |name, port|
    policy = policies.find { |doc| doc.dig("metadata", "name") == name }
    abort "missing platform policy #{name}" unless policy
    selector = policy.dig("spec", "selector", "matchLabels")
    deployment = workloads.find do |doc|
      labels = doc.dig("spec", "template", "metadata", "labels") || {}
      doc["kind"] == "Deployment" && selector.all? { |key, value| labels[key] == value }
    end
    abort "#{name} selector does not target a rendered Deployment" unless deployment
    ports = policy.fetch("spec").fetch("rules").flat_map { |rule| rule.fetch("to", []) }.flat_map { |to| to.dig("operation", "ports") || [] }
    abort "#{name} must allow only port #{port}" unless ports == [port]
  end
' "${RENDERS}" "${LOCAL}/manifests/40-ambient-security.yaml"

ruby -ryaml -e '
  documents = YAML.load_stream(File.read(ARGV.fetch(0))).compact
  images = documents.flat_map do |document|
    pod = case document["kind"]
          when "Deployment", "StatefulSet", "DaemonSet" then document.dig("spec", "template", "spec")
          end
    (pod && pod["containers"] || []).map { |container| container["image"] }
  end.compact
  abort "observability images must be digest pinned" unless images.all? { |image| image.match?(%r{@sha256:[a-f0-9]{64}\z}) }
  abort "local webhook sink missing" unless documents.any? { |document| document["kind"] == "Deployment" && document.dig("metadata", "name") == "alert-webhook-sink" }
' "${RENDERS}/../observability.yaml"

ruby -ryaml -e '
  descriptors = Dir[File.join(ARGV.fetch(0), "*.yaml")].sort
  abort "expected 13 disabled release descriptors" unless descriptors.length == 13
  descriptors.each do |file|
    doc = YAML.load_file(file)
    abort "descriptor must remain disabled: #{file}" unless doc["enabled"] == false
    abort "chart version must be immutable: #{file}" unless doc.dig("chart", "version") == "0.1.0"
    abort "moving Git values source forbidden: #{file}" if doc.key?("valuesRepository") || doc.dig("values", "repository")
    abort "missing RollingSync stage: #{file}" unless %w[00-foundation 10-stateful 20-bootstrap 30-cdc 40-observability].include?(doc["stage"])
    Array(doc.dig("values", "images")&.values).each do |image|
      abort "descriptor image must be digest pinned: #{file}" unless image.match?(%r{@sha256:[a-f0-9]{64}\z})
    end
  end
  enabled = Dir[File.join(ARGV.fetch(0), "enabled", "*.yaml")]
  abort "enabled inventory must be release-created" unless enabled.empty?
' "${ROOT}/environments/local-production/platform"

ruby -rjson -ryaml -e '
  schema = JSON.parse(File.read(File.join(ARGV.fetch(0), "contract.schema.json")))
  abort "production schema mode guard missing" unless schema.dig("properties", "mode", "const") == "external"
  contracts = Dir[File.join(ARGV.fetch(0), "*.yaml")]
  abort "expected eight production external contracts" unless contracts.length == 8
  contracts.each do |file|
    doc = YAML.load_file(file)
    abort "production contract deploys simulator: #{file}" unless doc["mode"] == "external" && doc["managedInCluster"] == false
    abort "production contract missing preflight: #{file}" if doc.fetch("preflight", []).empty?
    abort "production contract must use references, not endpoints: #{file}" unless doc.fetch("configuration").values.all? { |ref| %w[Secret ConfigMap].include?(ref["kind"]) && ref["name"] && ref["key"] }
  end
' "${ROOT}/environments/production/platform"

ruby -ryaml -e '
  appset = YAML.load_file(ARGV.fetch(0))
  abort "platform must use RollingSync" unless appset.dig("spec", "strategy", "type") == "RollingSync"
  steps = appset.dig("spec", "strategy", "rollingSync", "steps").map { |step| step.dig("matchExpressions", 0, "values", 0) }
  abort "platform ordering mismatch" unless steps == %w[00-foundation 10-stateful 20-bootstrap 30-cdc 40-observability]
  abort "RollingSync template must not enable automated sync" if appset.dig("spec", "template", "spec", "syncPolicy", "automated")
  path = appset.dig("spec", "generators", 0, "git", "files", 0, "path")
  abort "generator must watch released copies only" unless path == "environments/local-production/platform/enabled/*.yaml"
' "${ROOT}/environments/local-production/argocd/platform-applicationset.yaml"

grep -Fq 'applicationsetcontroller.enable.progressive.syncs' "${LOCAL}/bin/install-argocd-core.sh"
if grep -Fq 'roles:' "${ROOT}/environments/local-production/argocd/platform-app-project.yaml"; then
  echo "platform project must not define a misleading project-only CI token role" >&2
  exit 1
fi

ruby -rjson -ryaml -e '
  schema = JSON.parse(File.read(ARGV.fetch(0)))
  required = schema.fetch("required").sort
  documents = YAML.load_stream(File.read(ARGV.fetch(1))).compact
  mapped = documents.select { |document| document["kind"] == "ExternalSecret" }.flat_map do |document|
    Array(document.dig("spec", "data")).map { |entry| entry.dig("remoteRef", "property") }
  end.compact.uniq.sort
  abort "seed schema and local ExternalSecret properties differ: missing=#{mapped - required} stale=#{required - mapped}" unless required == mapped
' "${CHARTS}/platform-secrets/seed.schema.json" "${RENDERS}/platform-secrets.yaml"

redis_render="${RENDERS}/redis.yaml"
grep -Fq -- '--maxmemory' "${redis_render}"
grep -Fq -- '- "192mb"' "${redis_render}"
grep -Fq -- '--maxmemory-policy' "${redis_render}"
grep -Fq -- '- "noeviction"' "${redis_render}"
echo "ok local-production-platform-charts"
