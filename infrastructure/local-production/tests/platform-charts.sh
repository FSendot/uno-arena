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
  kafka = docs.find { |doc| doc["kind"] == "Deployment" && doc.dig("metadata", "name") == "kafka" }
  kafka_startup = kafka&.dig("spec", "template", "spec", "containers", 0, "startupProbe")
  abort "kafka startup probe must protect retained KRaft recovery" unless
    kafka_startup&.dig("tcpSocket", "port") == 9092 &&
    kafka_startup["initialDelaySeconds"].to_i >= 300 &&
    kafka_startup["periodSeconds"] == 10 &&
    kafka_startup["timeoutSeconds"] == 5 &&
    kafka_startup["failureThreshold"].to_i >= 60

  keycloak = docs.find { |doc| doc["kind"] == "Deployment" && doc.dig("metadata", "name") == "keycloak" }
  startup_probe = keycloak&.dig("spec", "template", "spec", "containers", 0, "startupProbe")
  abort "keycloak startup probe must protect first-start augmentation" unless
    startup_probe&.dig("httpGet", "path") == "/realms/master" &&
    startup_probe&.dig("httpGet", "port") == 8080 &&
    startup_probe["periodSeconds"] == 10 &&
    startup_probe["timeoutSeconds"] == 5 &&
    startup_probe["failureThreshold"].to_i >= 60

  minio = docs.find { |doc| doc["kind"] == "Deployment" && doc.dig("metadata", "name") == "minio" }
  minio_readiness = minio&.dig("spec", "template", "spec", "containers", 0, "readinessProbe")
  abort "minio readiness probe must tolerate local storage contention" unless
    minio_readiness&.dig("httpGet", "path") == "/minio/health/ready" &&
    minio_readiness["timeoutSeconds"] == 10

  redis = docs.find { |doc| doc["kind"] == "Deployment" && doc.dig("metadata", "name") == "redis" }
  abort "Redis must let the pod sandbox reap exec-probe children" unless
    redis&.dig("spec", "template", "spec", "shareProcessNamespace") == true
  %w[readinessProbe livenessProbe].each do |probe|
    abort "Redis #{probe} must tolerate local CPU contention" unless
      redis&.dig("spec", "template", "spec", "containers", 0, probe, "timeoutSeconds") == 10
  end
  debezium_connect = docs.find { |doc| doc["kind"] == "Deployment" && doc.dig("metadata", "name") == "debezium-connect" }
  connect_container = debezium_connect&.dig("spec", "template", "spec", "containers", 0)
  %w[startupProbe readinessProbe livenessProbe].each do |probe|
    abort "Debezium Connect #{probe} must use the REST endpoint" unless
      connect_container&.dig(probe, "httpGet", "path") == "/" &&
      connect_container&.dig(probe, "httpGet", "port") == "rest" &&
      connect_container&.dig(probe, "timeoutSeconds") == 10
  end
  abort "Debezium Connect startup probe must protect plugin scanning" unless
    connect_container.dig("startupProbe", "failureThreshold").to_i >= 60

  clickhouse = docs.find { |doc| doc["kind"] == "Deployment" && doc.dig("metadata", "name") == "clickhouse" }
  clickhouse_config = docs.find { |doc| doc["kind"] == "ConfigMap" && doc.dig("metadata", "name") == "clickhouse-kind-memory" }
  local_config = clickhouse_config&.dig("data", "99-kind-memory.xml").to_s
  %w[
    <level>warning</level>
    <console>true</console>
    <max_thread_pool_size>256</max_thread_pool_size>
    <background_pool_size>4</background_pool_size>
    <background_schedule_pool_size>8</background_schedule_pool_size>
    <number_of_free_entries_in_pool_to_lower_max_size_of_merge>1</number_of_free_entries_in_pool_to_lower_max_size_of_merge>
    <number_of_free_entries_in_pool_to_execute_mutation>1</number_of_free_entries_in_pool_to_execute_mutation>
    <number_of_free_entries_in_pool_to_execute_optimize_entire_partition>1</number_of_free_entries_in_pool_to_execute_optimize_entire_partition>
    <metric_log\ remove="1"/>
    <asynchronous_metric_log\ remove="1"/>
    <text_log\ remove="1"/>
  ].each do |required|
    abort "clickhouse local concurrency/log suppression missing #{required}" unless local_config.include?(required.tr("\\", ""))
  end
  clickhouse_startup = clickhouse&.dig("spec", "template", "spec", "containers", 0, "startupProbe")
  abort "clickhouse progress deadline must cover the retained-data startup budget" unless
    clickhouse&.dig("spec", "progressDeadlineSeconds").to_i >= 7200
  abort "clickhouse must reserve CPU for retained-data startup" unless
    clickhouse&.dig("spec", "template", "spec", "containers", 0, "resources", "requests", "cpu") == "500m"
  clickhouse_resources = clickhouse&.dig("spec", "template", "spec", "containers", 0, "resources")
  abort "clickhouse must reserve enough memory to keep the arm64 binary resident" unless
    clickhouse_resources&.dig("requests", "memory") == "1Gi" &&
    clickhouse_resources&.dig("limits", "memory") == "2Gi"
  abort "clickhouse startup probe must protect retained-data recovery" unless
    clickhouse_startup&.dig("httpGet", "path") == "/ping" &&
    clickhouse_startup&.dig("httpGet", "port") == 8123 &&
    clickhouse_startup["periodSeconds"] == 10 &&
    clickhouse_startup["timeoutSeconds"] == 10 &&
    clickhouse_startup["failureThreshold"].to_i >= 720
  clickhouse_liveness = clickhouse&.dig("spec", "template", "spec", "containers", 0, "livenessProbe")
  abort "clickhouse liveness probe must tolerate local CPU saturation" unless
    clickhouse_liveness&.dig("httpGet", "path") == "/ping" &&
    clickhouse_liveness["periodSeconds"] == 20 &&
    clickhouse_liveness["timeoutSeconds"] == 10 &&
    clickhouse_liveness["failureThreshold"].to_i >= 30

  debezium_server = docs.find do |doc|
    doc["kind"] == "Deployment" && doc.dig("metadata", "name") == "debezium-server-room-realtime"
  end
  server_mount = debezium_server&.dig("spec", "template", "spec", "containers", 0, "volumeMounts")&.find do |mount|
    mount["name"] == "config"
  end
  abort "Debezium Server config mount must preserve image-owned JMX rules" unless
    server_mount == {
      "name" => "config",
      "mountPath" => "/debezium/config/application.properties",
      "subPath" => "application.properties",
      "readOnly" => true
    }
  server_container = debezium_server.dig("spec", "template", "spec", "containers", 0)
  server_startup = server_container["startupProbe"]
  abort "Debezium Server startup probe must protect Java-agent initialization" unless
    server_startup&.dig("httpGet", "path") == "/q/health/live" &&
    server_startup&.dig("httpGet", "port") == 8080 &&
    server_startup["periodSeconds"] == 10 &&
    server_startup["timeoutSeconds"] == 10 &&
    server_startup["failureThreshold"].to_i >= 60
  %w[readinessProbe livenessProbe].each do |probe|
    abort "Debezium Server #{probe} must tolerate local CPU contention" unless
      server_container.dig(probe, "timeoutSeconds") == 10
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
  context_bootstrap = hooks.select { |job| job.dig("metadata", "name").start_with?("bootstrap-postgres-", "bootstrap-clickhouse-") }
  abort "context bootstrap Jobs must run after their ServiceAccount" unless context_bootstrap.all? do |job|
    job.dig("metadata", "annotations", "argocd.argoproj.io/hook") == "Sync" &&
      job.dig("metadata", "annotations", "argocd.argoproj.io/sync-wave") == "1"
  end
  context_service_account = docs.find do |doc|
    doc["kind"] == "ServiceAccount" && doc.dig("metadata", "name") == "context-bootstrap"
  end
  abort "context bootstrap ServiceAccount must precede Sync hooks" unless
    context_service_account&.dig("metadata", "annotations", "argocd.argoproj.io/sync-wave") == "0"
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

  room_postgres = policies.find { |doc| doc.dig("metadata", "name") == "postgres-room-gameplay-callers" }
  room_postgres_principals = room_postgres.fetch("spec").fetch("rules").flat_map do |rule|
    rule.fetch("from", []).flat_map { |source| source.dig("source", "principals") || [] }
  end
  abort "Room PgBouncer must be authorized to reach its Postgres backend" unless
    room_postgres_principals.include?("cluster.local/ns/uno-arena/sa/room-gameplay-pgbouncer")

  pgbouncer = policies.find { |doc| doc.dig("metadata", "name") == "room-gameplay-pgbouncer-callers" }
  abort "Room PgBouncer policy selector drift" unless
    pgbouncer&.dig("spec", "selector", "matchLabels") == {"app" => "room-gameplay-pgbouncer"}
  pgbouncer_ports = pgbouncer.fetch("spec").fetch("rules").flat_map do |rule|
    rule.fetch("to", []).flat_map { |entry| entry.dig("operation", "ports") || [] }
  end
  abort "Room PgBouncer policy must allow only port 6432" unless pgbouncer_ports == ["6432"]
  pgbouncer_principals = pgbouncer.fetch("spec").fetch("rules").flat_map do |rule|
    rule.fetch("from", []).flat_map { |source| source.dig("source", "principals") || [] }
  end
  required_pgbouncer_principals = %w[
    room-gameplay-router
    room-gameplay-runtime-controller
    room-gameplay-runtime
    room-gameplay-timer-worker
    room-gameplay-integrity-reconciler
    room-gameplay-player-stream-compactor
  ].map { |account| "cluster.local/ns/uno-arena/sa/#{account}" }
  abort "Room PgBouncer caller policy is incomplete" unless
    (required_pgbouncer_principals - pgbouncer_principals).empty?

  redis = policies.find { |doc| doc.dig("metadata", "name") == "redis-callers" }
  redis_principals = redis.fetch("spec").fetch("rules").flat_map do |rule|
    rule.fetch("from", []).flat_map { |source| source.dig("source", "principals") || [] }
  end
  abort "Room integrity reconciler must be authorized to reach Redis" unless
    redis_principals.include?("cluster.local/ns/uno-arena/sa/room-gameplay-integrity-reconciler")
' "${RENDERS}" "${LOCAL}/manifests/40-ambient-security.yaml"

ruby -ryaml -e '
  documents = YAML.load_stream(File.read(ARGV.fetch(0))).compact
  abort "observability must not compete with foundations for PeerAuthentication ownership" if
    documents.any? { |document| document["kind"] == "PeerAuthentication" }
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
  abort "expected 13 release descriptors" unless descriptors.length == 13
  enabled_directory = File.join(ARGV.fetch(0), "enabled")
  expected_enabled = []
  descriptors.each do |file|
    doc = YAML.load_file(file)
    enabled_file = File.join(enabled_directory, File.basename(file))
    case doc["status"]
    when "awaiting-immutable-package-publication"
      abort "awaiting descriptor must remain disabled: #{file}" unless doc["enabled"] == false
      abort "awaiting chart version must remain at source version: #{file}" unless doc.dig("chart", "version") == "0.1.0"
      abort "awaiting descriptor has an enabled copy: #{file}" if File.exist?(enabled_file)
    when "released"
      abort "released descriptor must be enabled: #{file}" unless doc["enabled"] == true
      abort "released descriptor lacks an enabled copy: #{file}" unless File.file?(enabled_file)
      abort "enabled copy differs from released descriptor: #{file}" unless YAML.load_file(enabled_file) == doc
      expected_enabled << enabled_file
    else
      abort "invalid descriptor status: #{file}"
    end
    abort "moving Git values source forbidden: #{file}" if doc.key?("valuesRepository") || doc.dig("values", "repository")
    abort "missing RollingSync stage: #{file}" unless %w[00-foundation 10-stateful 20-bootstrap 30-cdc 40-observability].include?(doc["stage"])
    Array(doc.dig("values", "images")&.values).each do |image|
      abort "descriptor image must be digest pinned: #{file}" unless image.match?(%r{@sha256:[a-f0-9]{64}\z})
    end
    image_digest = doc.dig("values", "image", "digest")
    if image_digest
      abort "descriptor image digest must be sha256 plus 64 hex characters: #{file}" unless
        image_digest.match?(%r{\Asha256:[a-f0-9]{64}\z})
    end
    if doc["component"] == "observability"
      abort "observability must delegate PeerAuthentication ownership to foundations" unless
        doc.dig("values", "security", "managePeerAuthentication") == false
      abort "observability must retain its PostSync deployment evidence" unless
        doc.dig("values", "postSyncEvidence", "enabled") == true
    end
  end
  enabled = Dir[File.join(enabled_directory, "*.yaml")].sort
  abort "enabled inventory does not match released descriptors" unless enabled == expected_enabled.sort
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
  max_updates = appset.dig("spec", "strategy", "rollingSync", "steps").map { |step| step["maxUpdate"] }
  abort "local platform reconciliation must advance one Application at a time" unless max_updates == [1, 1, 1, 1, 1]
  abort "RollingSync template must not enable automated sync" if appset.dig("spec", "template", "spec", "syncPolicy", "automated")
  path = appset.dig("spec", "generators", 0, "git", "files", 0, "path")
  abort "generator must watch released copies only" unless path == "environments/local-production/platform/enabled/*.yaml"
' "${ROOT}/environments/local-production/argocd/platform-applicationset.yaml"

grep -Fq '\"applicationsetcontroller.enable.progressive.syncs\":\"true\"' "${LOCAL}/bin/install-argocd-core.sh"
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
