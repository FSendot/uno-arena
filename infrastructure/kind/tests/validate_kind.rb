#!/usr/bin/env ruby
# frozen_string_literal: true

# Offline kind foundation validation. Ruby stdlib only. Never pulls or contacts a cluster.

require "yaml"
require "set"
require "json"
require "digest"
require "rbconfig"

ROOT = File.expand_path("../../..", __dir__)
KIND = File.join(ROOT, "infrastructure/kind")
BOOTSTRAP = File.join(ROOT, "infrastructure/bootstrap")
MANIFESTS = File.join(KIND, "manifests")
GENERATED = File.join(KIND, "generated")
ASYNCAPI = File.join(ROOT, "contracts/asyncapi/kafka-v1.yaml")
DOCKERIGNORE = File.join(ROOT, ".dockerignore")

require_relative "../../bootstrap/lib/fingerprint"

KURRENT_ARM_DIGEST = "sha256:8498556a8ba7a74f8d4ea31a149b1e5216e167d6884b630a68b3e1eb9e6e870e"
KURRENT_AMD_DIGEST = "sha256:b4d0665a78269cd7184971c4d1fad38265277901f3d3730d89dcfba8f3d37fe9"
EXPECTED_BOOTSTRAP_JOBS = {
  "bootstrap-postgres-identity" => "identity",
  "bootstrap-postgres-room-gameplay" => "room-gameplay",
  "bootstrap-postgres-tournament" => "tournament",
  "bootstrap-postgres-ranking" => "ranking",
  "bootstrap-clickhouse-analytics" => "analytics",
  "bootstrap-kafka-topics" => nil
}.freeze

POSTGRES_SERVICES = %w[
  postgres-identity
  postgres-room-gameplay
  postgres-tournament
  postgres-ranking
].freeze

POSTGRES_DBS = {
  "postgres-identity" => "identity",
  "postgres-room-gameplay" => "room_gameplay",
  "postgres-tournament" => "tournament",
  "postgres-ranking" => "ranking"
}.freeze

MIGRATION_COPY_PATHS = [
  "services/identity/migrations/001_init.sql",
  "services/room-gameplay/migrations/001_init.sql",
  "services/tournament-orchestration/migrations/001_init.sql",
  "services/ranking/migrations/001_init.sql",
  "services/analytics/migrations/001_init.sql"
].freeze

failures = []

def fail_collect(failures, msg)
  failures << msg
  warn "FAIL: #{msg}"
end

def load_all_yaml(path)
  docs = []
  YAML.load_stream(File.read(path)) { |doc| docs << doc unless doc.nil? }
  docs
rescue StandardError => e
  raise "YAML parse failed for #{path}: #{e}"
end

def each_manifest(dir)
  Dir.glob(File.join(dir, "**/*.{yaml,yml}")).sort.each do |path|
    load_all_yaml(path).each { |doc| yield path, doc }
  end
end

def env_names(doc)
  ((doc.dig("spec", "template", "spec", "containers") || []).flat_map { |c| c["env"] || [] }).map { |e| e["name"] }
end

# --- YAML parse ---
begin
  each_manifest(MANIFESTS) { |_p, _d| }
  each_manifest(GENERATED) { |_p, _d| } if File.directory?(GENERATED)
  load_all_yaml(File.join(KIND, "cluster.yaml"))
  puts "ok yaml-parse"
rescue StandardError => e
  fail_collect(failures, e.message)
end

# --- Duplicate resource detection ---
seen = {}
[MANIFESTS, GENERATED].each do |dir|
  next unless File.directory?(dir)
  each_manifest(dir) do |path, doc|
    next unless doc.is_a?(Hash)
    key = [doc["apiVersion"], doc["kind"], doc.dig("metadata", "namespace"), doc.dig("metadata", "name")]
    next if key[1].nil? || key[3].nil?
    if seen.key?(key)
      fail_collect(failures, "duplicate resource #{key.inspect} in #{path} and #{seen[key]}")
    else
      seen[key] = path
    end
  end
end
puts "ok duplicate-resources" if failures.none? { |f| f.include?("duplicate") }

# --- Image pins ---
images_cm = File.read(File.join(MANIFESTS, "01-local-secrets.yaml"))

{
  "POSTGRES_IMAGE" => "postgres:18.4-alpine3.24@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15",
  "KAFKA_IMAGE" => "apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837",
  "REDIS_IMAGE" => "redis:8.8.0-alpine@sha256:9d317178eceac8454a2284a9e6df2466b93c745529947f0cd42a0fa9609d7005",
  "CLICKHOUSE_IMAGE" => "clickhouse/clickhouse-server:26.6.1.1193@sha256:1d1f6508eba2dccce2cee9913907c5f7766327debc57a6b1991f2c9e3176c163",
  "KEYCLOAK_IMAGE" => "quay.io/keycloak/keycloak:26.7.0@sha256:2eb3cd316835c990e69e26ade292ffa78f6fb0db7d5fc6377463c162e1979ac0",
  "BOOTSTRAP_IMAGE" => "uno-arena/bootstrap:local"
}.each do |key, value|
  fail_collect(failures, "images ConfigMap missing #{key}: #{value}") unless images_cm.include?("#{key}: #{value}")
end

kurrent_manifest = File.read(File.join(MANIFESTS, "40-kurrentdb/kurrentdb.yaml"))
fail_collect(failures, "Kurrent manifest must defer architecture selection") unless kurrent_manifest.include?("__KURRENTDB_IMAGE_BY_NODE_ARCH__")
apply_text = File.read(File.join(KIND, "scripts/apply.sh"))
fail_collect(failures, "Kurrent apply selection missing reviewed ARM digest") unless apply_text.include?(KURRENT_ARM_DIGEST)
fail_collect(failures, "Kurrent apply selection missing reviewed AMD digest") unless apply_text.include?(KURRENT_AMD_DIGEST)
fail_collect(failures, "Kurrent image ConfigMap must defer architecture selection") unless images_cm.include?("__KURRENTDB_IMAGE_BY_NODE_ARCH__")
fail_collect(failures, "Kurrent readiness must use supported /health/live") unless kurrent_manifest.match?(%r{readinessProbe:[\s\S]*?path:\s*/health/live})
fail_collect(failures, "Kurrent liveness must be /health/live") unless kurrent_manifest.match?(%r{livenessProbe:[\s\S]*?path:\s*/health/live})
fail_collect(failures, "Kurrent missing data mount /var/lib/kurrentdb") unless kurrent_manifest.include?("/var/lib/kurrentdb")
%w[KURRENTDB_CLUSTER_SIZE KURRENTDB_INSECURE KURRENTDB_RUN_PROJECTIONS].each do |k|
  fail_collect(failures, "Kurrent missing env #{k}") unless kurrent_manifest.include?(k)
end
# Service-link env vars are prefixed with the Service name uppercased (KURRENTDB_*),
# which collides with Kurrent configuration options (ServicePortHttpGrpc, Port, …).
kurrent_dep = load_all_yaml(File.join(MANIFESTS, "40-kurrentdb/kurrentdb.yaml")).find do |doc|
  doc.is_a?(Hash) && doc["kind"] == "Deployment"
end
kurrent_pod = kurrent_dep&.dig("spec", "template", "spec") || {}
unless kurrent_pod["enableServiceLinks"] == false
  fail_collect(
    failures,
    "Kurrent pod spec must set enableServiceLinks: false " \
    "(Service-link KURRENTDB_* env vars collide with Kurrent config)"
  )
end

# The complete single-node profile must remain below the reviewed local runtime
# budget. These are kind-only ceilings; production sizing is environment-owned.
kind_infra_budgets = {
  "30-kafka/kafka.yaml" => ["kafka", "kafka", "384Mi", "768Mi"],
  "40-kurrentdb/kurrentdb.yaml" => ["kurrentdb", "kurrentdb", "512Mi", "1024Mi"],
  "50-clickhouse/clickhouse.yaml" => ["clickhouse", "clickhouse", "512Mi", "1024Mi"],
  "60-keycloak/keycloak.yaml" => ["keycloak", "keycloak", "256Mi", "768Mi"],
  "80-debezium/connect.yaml" => ["debezium-connect", "connect", "384Mi", "1024Mi"],
  "80-debezium-server/debezium-server-room-realtime.yaml" => ["debezium-server-room-realtime", "debezium-server", "256Mi", "768Mi"]
}.freeze
kind_infra_budgets.each do |relative_path, (deployment_name, container_name, request_memory, limit_memory)|
  deployment = load_all_yaml(File.join(MANIFESTS, relative_path)).find do |doc|
    doc.is_a?(Hash) && doc["kind"] == "Deployment" && doc.dig("metadata", "name") == deployment_name
  end
  container = (deployment&.dig("spec", "template", "spec", "containers") || []).find { |item| item["name"] == container_name }
  fail_collect(failures, "#{deployment_name} container missing for kind memory budget") unless container
  fail_collect(failures, "#{deployment_name} must not surge a second heavy kind pod") unless deployment&.dig("spec", "strategy", "type") == "Recreate"
  next unless container

  fail_collect(failures, "#{deployment_name} request memory drift") unless container.dig("resources", "requests", "memory") == request_memory
  fail_collect(failures, "#{deployment_name} limit memory drift") unless container.dig("resources", "limits", "memory") == limit_memory
end

minio_docs = load_all_yaml(File.join(MANIFESTS, "90-observability-storage/minio.yaml"))
minio_dep = minio_docs.find { |doc| doc.is_a?(Hash) && doc["kind"] == "Deployment" && doc.dig("metadata", "name") == "minio" }
minio_container = minio_dep&.dig("spec", "template", "spec", "containers", 0) || {}
minio_env = minio_container.fetch("env", []).to_h { |entry| [entry["name"], entry["value"]] }
fail_collect(failures, "MinIO must not surge a second heavy kind pod") unless minio_dep&.dig("spec", "strategy", "type") == "Recreate"
fail_collect(failures, "MinIO kind memory limit drift") unless minio_container.dig("resources", "limits", "memory") == "512Mi"
fail_collect(failures, "MinIO kind Go memory target drift") unless minio_env["GOMEMLIMIT"] == "256MiB"

kafka_budget_dep = load_all_yaml(File.join(MANIFESTS, "30-kafka/kafka.yaml")).find { |doc| doc.is_a?(Hash) && doc["kind"] == "Deployment" }
kafka_budget_container = kafka_budget_dep&.dig("spec", "template", "spec", "containers", 0) || {}
kafka_env = kafka_budget_container.fetch("env", []).to_h { |entry| [entry["name"], entry["value"]] }
fail_collect(failures, "Kafka kind heap budget drift") unless kafka_env["KAFKA_HEAP_OPTS"] == "-Xms256M -Xmx512M"
keycloak_dep = load_all_yaml(File.join(MANIFESTS, "60-keycloak/keycloak.yaml")).find { |doc| doc.is_a?(Hash) && doc["kind"] == "Deployment" }
keycloak_container = keycloak_dep&.dig("spec", "template", "spec", "containers", 0) || {}
keycloak_env = keycloak_container.fetch("env", []).to_h { |entry| [entry["name"], entry["value"]] }
fail_collect(failures, "Keycloak kind heap budget drift") unless keycloak_env["JAVA_OPTS_KC_HEAP"] == "-Xms128m -Xmx384m"
server_budget_dep = load_all_yaml(File.join(MANIFESTS, "80-debezium-server/debezium-server-room-realtime.yaml")).find { |doc| doc.is_a?(Hash) && doc["kind"] == "Deployment" }
server_budget_container = server_budget_dep&.dig("spec", "template", "spec", "containers", 0) || {}
server_env = server_budget_container.fetch("env", []).to_h { |entry| [entry["name"], entry["value"]] }
fail_collect(failures, "Debezium Server kind heap budget drift") unless server_env["JAVA_TOOL_OPTIONS"] == "-Xms64m -Xmx256m"

clickhouse_docs = load_all_yaml(File.join(MANIFESTS, "50-clickhouse/clickhouse.yaml"))
clickhouse_memory = clickhouse_docs.find { |doc| doc.is_a?(Hash) && doc["kind"] == "ConfigMap" && doc.dig("metadata", "name") == "clickhouse-kind-memory" }
clickhouse_memory_xml = clickhouse_memory&.dig("data", "99-kind-memory.xml").to_s
fail_collect(failures, "ClickHouse kind server memory cap drift") unless clickhouse_memory_xml.include?("<max_server_memory_usage>805306368</max_server_memory_usage>")
fail_collect(failures, "ClickHouse kind mark cache cap drift") unless clickhouse_memory_xml.include?("<mark_cache_size>67108864</mark_cache_size>")
fail_collect(failures, "ClickHouse kind uncompressed cache cap drift") unless clickhouse_memory_xml.include?("<uncompressed_cache_size>33554432</uncompressed_cache_size>")
puts "ok image-pins-and-kurrent"

# --- Redis local AOF (same-pod emptyDir restart; disposable / not authoritative) ---
redis_manifest_path = File.join(MANIFESTS, "20-redis/redis.yaml")
fail_collect(failures, "missing redis manifest") unless File.file?(redis_manifest_path)
redis_docs = load_all_yaml(redis_manifest_path)
redis_dep = redis_docs.find { |d| d.is_a?(Hash) && d["kind"] == "Deployment" && d.dig("metadata", "name") == "redis" }
fail_collect(failures, "redis Deployment missing") if redis_dep.nil?
redis_container = (redis_dep&.dig("spec", "template", "spec", "containers") || []).find { |c| c["name"] == "redis" }
fail_collect(failures, "redis container missing") if redis_container.nil?
redis_args = Array(redis_container&.fetch("args", []))
redis_args_s = redis_args.join(" ")
fail_collect(failures, "redis must set --appendonly yes") unless redis_args_s.match?(/--appendonly\s+yes\b/) || (redis_args.include?("--appendonly") && redis_args[redis_args.index("--appendonly") + 1] == "yes")
fail_collect(failures, "redis must not set --appendonly no") if redis_args_s.match?(/--appendonly\s+no\b/) || (redis_args.include?("--appendonly") && redis_args[redis_args.index("--appendonly") + 1] == "no")
fail_collect(failures, "redis must set --appendfsync everysec") unless redis_args_s.match?(/--appendfsync\s+everysec\b/) || (redis_args.include?("--appendfsync") && redis_args[redis_args.index("--appendfsync") + 1] == "everysec")
fail_collect(failures, "redis must set --dir /data") unless redis_args_s.match?(%r{--dir\s+/data\b}) || (redis_args.include?("--dir") && redis_args[redis_args.index("--dir") + 1] == "/data")
fail_collect(failures, "redis must set --appenddirname appendonlydir") unless redis_args_s.match?(/--appenddirname\s+appendonlydir\b/) || (redis_args.include?("--appenddirname") && redis_args[redis_args.index("--appenddirname") + 1] == "appendonlydir")
fail_collect(failures, "redis must keep --save \"\"") unless redis_args.include?("--save") && redis_args[redis_args.index("--save") + 1] == ""
redis_vols = redis_dep&.dig("spec", "template", "spec", "volumes") || []
redis_data = redis_vols.find { |v| v["name"] == "data" }
fail_collect(failures, "redis must retain emptyDir data volume") unless redis_data&.key?("emptyDir")
redis_mounts = redis_container&.fetch("volumeMounts", []) || []
fail_collect(failures, "redis must mount data at /data") unless redis_mounts.any? { |m| m["name"] == "data" && m["mountPath"] == "/data" }
redis_ready = redis_container&.fetch("readinessProbe", {}) || {}
redis_ready_cmd = Array((redis_ready.dig("exec", "command") || []))
redis_ready_s = redis_ready_cmd.join(" ")
unless redis_ready_s.include?("PONG") || redis_ready_s.include?("pong")
  fail_collect(failures, "redis readinessProbe must require PONG so AOF LOADING fails readiness")
end
redis_text = File.read(redis_manifest_path)
fail_collect(failures, "redis manifest must retain disposable wording") unless redis_text.match?(/Disposable|emptyDir/i)
fail_collect(failures, "redis manifest must not claim Redis is authoritative") if redis_text.match?(/Redis is authoritative|authoritative store/i)
redis_aof_live = File.join(ROOT, "infrastructure/kind/scripts/test-redis-aof.sh")
redis_aof_struct = File.join(ROOT, "infrastructure/kind/scripts/test-redis-aof-structure.sh")
fail_collect(failures, "missing test-redis-aof.sh") unless File.file?(redis_aof_live)
fail_collect(failures, "missing test-redis-aof-structure.sh") unless File.file?(redis_aof_struct)
puts "ok redis-local-aof"

# --- Debezium Server (Room realtime → Redis); structure only, no delivery claim ---
DEBEZIUM_SERVER_INDEX_DIGEST = "sha256:adec18409dff7bcc2d00511f1d5aee5b7677cd5901ef729576ac02728d30ea9d"
DEBEZIUM_SERVER_SOURCE_IMAGE = "quay.io/debezium/server:3.6.0.Final@#{DEBEZIUM_SERVER_INDEX_DIGEST}"
DEBEZIUM_SERVER_STALE_TAG = "docker.io/uno-arena/debezium-server:3.6.0.Final-3754ca3df34b"
DEBEZIUM_SERVER_SHORT_STALE = "uno-arena/debezium-server:3.6.0.Final-3754ca3df34b"
debezium_server_path = File.join(MANIFESTS, "80-debezium-server/debezium-server-room-realtime.yaml")
fail_collect(failures, "missing Debezium Server manifest") unless File.file?(debezium_server_path)
ds_text = File.read(debezium_server_path)
fail_collect(failures, "Debezium Server must use exact source #{DEBEZIUM_SERVER_SOURCE_IMAGE}") unless ds_text.include?(DEBEZIUM_SERVER_SOURCE_IMAGE)
fail_collect(failures, "Debezium Server must record multiarch index #{DEBEZIUM_SERVER_INDEX_DIGEST}") unless ds_text.include?(DEBEZIUM_SERVER_INDEX_DIGEST)
fail_collect(failures, "Debezium Server must use imagePullPolicy Never") unless ds_text.include?("imagePullPolicy: Never")
fail_collect(failures, "Debezium Server must not use IfNotPresent") if ds_text.include?("imagePullPolicy: IfNotPresent")
ds_images = ds_text.scan(/^\s+image:\s+(\S+)\s*$/).flatten
fail_collect(failures, "Debezium Server Deployment image must be #{DEBEZIUM_SERVER_SOURCE_IMAGE}") unless ds_images.include?(DEBEZIUM_SERVER_SOURCE_IMAGE)
fail_collect(failures, "Debezium Server must deploy digest (@sha256) image refs") if ds_images.any? { |i| !i.include?("@") }
fail_collect(failures, "Debezium Server must use quay.io/debezium/server image") if ds_images.any? { |i| !i.start_with?("quay.io/debezium/server:") }
fail_collect(failures, "Debezium Server must not use stale runtime tag #{DEBEZIUM_SERVER_STALE_TAG}") if ds_text.include?(DEBEZIUM_SERVER_STALE_TAG)
fail_collect(failures, "Debezium Server must not use short stale tag #{DEBEZIUM_SERVER_SHORT_STALE}") if ds_text.match?(/^\s+(?:image|RUNTIME_IMAGE):\s+#{Regexp.escape(DEBEZIUM_SERVER_SHORT_STALE)}\s*$/)
fail_collect(failures, "Debezium Server must not claim RUNTIME_IMAGE") if ds_text.include?("RUNTIME_IMAGE")
fail_collect(failures, "Debezium Server must use Redis sink") unless ds_text.include?("debezium.sink.type=redis")
fail_collect(failures, "Debezium Server must use Redis DB 2") unless ds_text.include?("debezium.sink.redis.db.index=2")
fail_collect(failures, "Debezium Server must use extended Redis message format") unless ds_text.include?("message.format=extended")
fail_collect(failures, "Debezium Server must use room_cdc_realtime_outbox publication") unless ds_text.include?("room_cdc_realtime_outbox")
fail_collect(failures, "Debezium Server must capture realtime_outbox_events") unless ds_text.include?("realtime_outbox_events")
fail_collect(failures, "Debezium Server must use unique slot debezium_server_room_realtime") unless ds_text.include?("debezium_server_room_realtime")
fail_collect(failures, "Debezium Server must route by target_stream") unless ds_text.include?("route.by.field=target_stream")
# Property-file escape: Ruby '\\' => one '\'; need four backslashes for file's \\${...}.
fail_collect(failures, "Debezium Server route.topic.replacement must use \\\\${routedByValue} (SmallRye escape)") unless ds_text.include?("route.topic.replacement=\\\\${routedByValue}")
fail_collect(failures, "Debezium Server must not use bare unescaped route.topic.replacement=${routedByValue}") if ds_text.match?(/^\s*debezium\.transforms\.outbox\.route\.topic\.replacement=\$\{routedByValue\}\s*$/)
fail_collect(failures, "Debezium Server offset key must be room-realtime unique") unless ds_text.include?("metadata:debezium:room-realtime:offsets")
fail_collect(failures, "Debezium Server must not set unsupported offset Redis db.index") if ds_text.include?("offset.storage.redis.db.index")
fail_collect(failures, "Debezium Server must not configure Redis schema history") if ds_text.match?(/schema\.history|SchemaHistory|schema_history/i)
fail_collect(failures, "Debezium Server must use snapshot.mode=no_data") unless ds_text.include?("snapshot.mode=no_data")
fail_collect(failures, "Debezium Server must use official format.key=json") unless ds_text.include?("debezium.format.key=json")
fail_collect(failures, "Debezium Server must use official format.value=json") unless ds_text.include?("debezium.format.value=json")
fail_collect(failures, "Debezium Server must use official format.header=json") unless ds_text.include?("debezium.format.header=json")
fail_collect(failures, "Debezium Server must not use source converter aliases") if ds_text.match?(/debezium\.source\.(key|value)\.converter/)
fail_collect(failures, "Debezium Server must not re-place payload via additional.placement") if ds_text.match?(/payload:envelope:(payload|data)/)
fail_collect(failures, "Debezium Server must not be Kafka Connect image") if ds_text.match?(%r{debezium/connect|connect-distributed}i)
fail_collect(failures, "Debezium Server docs must not claim live delivery") if ds_text.match?(/proves live|live Postgres→Redis delivery verified/i)
ds_struct = File.join(KIND, "scripts/test-debezium-server-structure.sh")
ds_load = File.join(KIND, "scripts/load-debezium-server.sh")
ds_status = File.join(KIND, "scripts/status-debezium-server.sh")
fail_collect(failures, "missing test-debezium-server-structure.sh") unless File.file?(ds_struct)
fail_collect(failures, "missing load-debezium-server.sh") unless File.file?(ds_load)
fail_collect(failures, "missing status-debezium-server.sh") unless File.file?(ds_status)
ds_load_text = File.read(ds_load)
fail_collect(failures, "Debezium Server loader must record multiarch index") unless ds_load_text.include?(DEBEZIUM_SERVER_INDEX_DIGEST.delete_prefix("sha256:"))
fail_collect(failures, "Debezium Server loader must stage exact source image") unless ds_load_text.include?(DEBEZIUM_SERVER_SOURCE_IMAGE)
fail_collect(failures, "Debezium Server loader must docker exec into kind node") unless ds_load_text.include?("docker exec")
fail_collect(failures, "Debezium Server loader must crictl pull") unless ds_load_text.include?("crictl pull")
fail_collect(failures, "Debezium Server loader must verify via crictl inspecti") unless ds_load_text.include?("crictl inspecti")
fail_collect(failures, "Debezium Server loader must verify amd64") unless ds_load_text.include?('expected_arch="amd64"')
fail_collect(failures, "Debezium Server loader must verify arm64") unless ds_load_text.include?('expected_arch="arm64"')
fail_collect(failures, "Debezium Server loader must not kind load") if ds_load_text.match?(/kind\s+load\s+docker-image/)
fail_collect(failures, "Debezium Server loader must remove stale runtime tag") unless ds_load_text.include?(DEBEZIUM_SERVER_STALE_TAG)
fail_collect(failures, "Debezium Server loader must fail closed on docker.io/library/import-") unless ds_load_text.include?("docker.io/library/import-")
fail_collect(failures, "Debezium Server loader must fail closed on /import- repoDigests") unless ds_load_text.include?("/import-")
fail_collect(failures, "Debezium Server loader must name stale kind-import metadata") unless ds_load_text.include?("stale kind-import metadata")
fail_collect(failures, "Debezium Server loader import rejection must require reset/recreate") unless ds_load_text.match?(%r{reset/recreate|reset.*recreate})
fail_collect(failures, "Debezium Server loader import rejection must require node-native loader rerun") unless ds_load_text.include?("node-native loader")
fail_collect(failures, "Debezium Server loader must not encode ctr content / containerd restart remediation") if ds_load_text.match?(/ctr\s+content|content\s+remove|restart\s+containerd|containerd\s+restart/i)
puts "ok debezium-server-room-realtime"

# --- Debezium Kafka Connect (four outbox routers); structure only, no delivery claim ---
DEBEZIUM_CONNECT_MULTIARCH_DIGEST = "sha256:61d29e5a0316de5dd0a564ec40eaa662d837a05217523e1a1745ecde3d790455"
DEBEZIUM_CONNECT_SOURCE_IMAGE = "quay.io/debezium/connect:3.6.0.Final@#{DEBEZIUM_CONNECT_MULTIARCH_DIGEST}"
DEBEZIUM_CONNECT_STALE_TAG = "docker.io/uno-arena/debezium-connect:3.6.0.Final-b7ca129320f4"
DEBEZIUM_CONNECT_SHORT_STALE = "uno-arena/debezium-connect:3.6.0.Final-b7ca129320f4"
debezium_connect_path = File.join(MANIFESTS, "80-debezium/connect.yaml")
debezium_register_path = File.join(MANIFESTS, "80-debezium/job-register-connectors.yaml")
fail_collect(failures, "missing Debezium Connect manifest") unless File.file?(debezium_connect_path)
fail_collect(failures, "missing Debezium connector registration Job") unless File.file?(debezium_register_path)
dc_text = File.read(debezium_connect_path)
dr_text = File.read(debezium_register_path)
fail_collect(failures, "Debezium Connect must use exact source #{DEBEZIUM_CONNECT_SOURCE_IMAGE}") unless dc_text.include?(DEBEZIUM_CONNECT_SOURCE_IMAGE)
fail_collect(failures, "Debezium Connect must document multiarch index #{DEBEZIUM_CONNECT_MULTIARCH_DIGEST}") unless dc_text.include?(DEBEZIUM_CONNECT_MULTIARCH_DIGEST)
fail_collect(failures, "Debezium Connect register Job must use same exact source") unless dr_text.include?(DEBEZIUM_CONNECT_SOURCE_IMAGE)
fail_collect(failures, "Debezium Connect register Job must record multiarch index") unless dr_text.include?(DEBEZIUM_CONNECT_MULTIARCH_DIGEST)
fail_collect(failures, "Debezium Connect must use 3.6.0.Final") unless dc_text.include?("3.6.0.Final")
fail_collect(failures, "Debezium Connect must use imagePullPolicy Never") unless dc_text.include?("imagePullPolicy: Never")
fail_collect(failures, "Debezium Connect register Job must use imagePullPolicy Never") unless dr_text.include?("imagePullPolicy: Never")
fail_collect(failures, "Debezium Connect must not use IfNotPresent") if dc_text.include?("imagePullPolicy: IfNotPresent") || dr_text.include?("imagePullPolicy: IfNotPresent")
dc_images = dc_text.scan(/^\s+image:\s+(\S+)\s*$/).flatten
dr_images = dr_text.scan(/^\s+image:\s+(\S+)\s*$/).flatten
fail_collect(failures, "Debezium Connect Deployment image must be #{DEBEZIUM_CONNECT_SOURCE_IMAGE}") unless dc_images.include?(DEBEZIUM_CONNECT_SOURCE_IMAGE)
fail_collect(failures, "Debezium Connect Job image must be #{DEBEZIUM_CONNECT_SOURCE_IMAGE}") unless dr_images.include?(DEBEZIUM_CONNECT_SOURCE_IMAGE)
fail_collect(failures, "Debezium Connect Deployment and Job must share the same image") unless (dc_images & dr_images).include?(DEBEZIUM_CONNECT_SOURCE_IMAGE)
fail_collect(failures, "Debezium Connect must deploy digest (@sha256) image refs") if (dc_images + dr_images).any? { |i| !i.include?("@") }
fail_collect(failures, "Debezium Connect must use quay.io/debezium/connect images") if (dc_images + dr_images).any? { |i| !i.start_with?("quay.io/debezium/connect:") }
fail_collect(failures, "Debezium Connect must not use stale runtime tag #{DEBEZIUM_CONNECT_STALE_TAG}") if (dc_text + dr_text).include?(DEBEZIUM_CONNECT_STALE_TAG)
fail_collect(failures, "Debezium Connect must not use short stale tag #{DEBEZIUM_CONNECT_SHORT_STALE}") if (dc_text + dr_text).match?(/^\s+(?:image|RUNTIME_IMAGE):\s+#{Regexp.escape(DEBEZIUM_CONNECT_SHORT_STALE)}\s*$/)
fail_collect(failures, "Debezium Connect must not claim RUNTIME_IMAGE") if dc_text.include?("RUNTIME_IMAGE")
fail_collect(failures, "Debezium Connect must target kafka.uno-arena.svc.cluster.local:9092") unless dc_text.include?("kafka.uno-arena.svc.cluster.local:9092")
%w[connect-configs connect-offsets connect-status].each do |topic|
  fail_collect(failures, "Debezium Connect must use internal topic #{topic}") unless dc_text.include?(topic)
end
fail_collect(failures, "Debezium Connect must set RF1 for config storage") unless dc_text.include?("CONFIG_STORAGE_REPLICATION_FACTOR")
connector_keys = dc_text.scan(/^[[:space:]]*(connector-[a-z0-9-]+\.json):/).flatten
fail_collect(failures, "Debezium Connect must define exactly 4 connector templates, got #{connector_keys.inspect}") unless connector_keys.size == 4
%w[
  connector-identity-outbox.json
  connector-room-integration-outbox.json
  connector-tournament-outbox.json
  connector-ranking-outbox.json
].each do |key|
  fail_collect(failures, "missing connector template #{key}") unless connector_keys.include?(key)
end
%w[
  identity_cdc_outbox
  room_cdc_integration_outbox
  tournament_cdc_outbox
  ranking_cdc_outbox
].each do |pub|
  fail_collect(failures, "Debezium Connect missing publication #{pub}") unless dc_text.include?(pub)
end
%w[
  identity_outbox_slot
  room_integration_outbox_slot
  tournament_outbox_slot
  ranking_outbox_slot
].each do |slot|
  fail_collect(failures, "Debezium Connect missing unique slot #{slot}") unless dc_text.include?(slot)
end
fail_collect(failures, "publication.autocreate.mode must be disabled on all connectors") unless dc_text.scan(/"publication\.autocreate\.mode":\s*"disabled"/).size == 4
fail_collect(failures, "plugin.name must be pgoutput on all connectors") unless dc_text.scan(/"plugin\.name":\s*"pgoutput"/).size == 4
fail_collect(failures, "snapshot.mode must be no_data on all connectors") unless dc_text.scan(/"snapshot\.mode":\s*"no_data"/).size == 4
fail_collect(failures, "snapshot.mode=never must not remain") if dc_text.include?('"snapshot.mode": "never"')
fail_collect(failures, "Outbox EventRouter required") unless dc_text.include?("io.debezium.transforms.outbox.EventRouter")
fail_collect(failures, "outbox must route by topic column") unless dc_text.include?('"transforms.outbox.route.by.field": "topic"')
fail_collect(failures, "outbox must key by partition_key") unless dc_text.include?('"transforms.outbox.table.field.event.key": "partition_key"')
fail_collect(failures, "outbox must id by event_id") unless dc_text.include?('"transforms.outbox.table.field.event.id": "event_id"')
fail_collect(failures, "outbox must payload by payload") unless dc_text.include?('"transforms.outbox.table.field.event.payload": "payload"')
fail_collect(failures, "outbox must map event_type") unless dc_text.include?("event_type:header:type")
fail_collect(failures, "outbox must not map Postgres timestamptz occurred_at as an INT64 Kafka record timestamp") if dc_text.include?('"transforms.outbox.table.field.event.timestamp"')
fail_collect(failures, "schemas must be disabled for JSON values") unless dc_text.include?('"value.converter.schemas.enable": "false"')
fail_collect(failures, "Connect worker must not set KEY_CONVERTER (connectors own converters)") if dc_text.match?(/^\s+- name: KEY_CONVERTER\s*$/)
fail_collect(failures, "Kafka Connect must not capture realtime_outbox") if dc_text.include?("realtime_outbox_events")
fail_collect(failures, "no Heartbeat SMT / heartbeat.action.query") if dc_text.match?(/Heartbeat|heartbeat\.action\.query/i)
fail_collect(failures, "Connect Deployment missing readinessProbe") unless dc_text.include?("readinessProbe:")
fail_collect(failures, "Connect Deployment missing livenessProbe") unless dc_text.include?("livenessProbe:")
fail_collect(failures, "Connect probes should use curl") unless dc_text.include?("curl")
# Heap must be explicit and safely below the container memory limit (image default -Xmx2G is unbounded for kind).
fail_collect(failures, "Connect must set KAFKA_HEAP_OPTS (reject image-default unbounded heap)") unless dc_text.match?(/^\s+- name: KAFKA_HEAP_OPTS\s*$/)
dc_mem_limit = dc_text[/limits:\s*\n\s+memory:\s*(\d+)Mi/, 1]&.to_i
dc_heap_opts = dc_text[/- name:\s*KAFKA_HEAP_OPTS\s*\n\s+value:\s*"([^"]+)"/, 1]
dc_rebalance_delay = dc_text[/- name:\s*CONNECT_SCHEDULED_REBALANCE_MAX_DELAY_MS\s*\n\s+value:\s*"([^"]+)"/, 1]
fail_collect(failures, "Connect memory limit Mi missing") if dc_mem_limit.nil? || dc_mem_limit <= 0
fail_collect(failures, "Connect KAFKA_HEAP_OPTS value missing") if dc_heap_opts.nil? || dc_heap_opts.empty?
fail_collect(failures, "kind singleton Connect scheduled rebalance delay must be 10000ms") unless dc_rebalance_delay == "10000"
if dc_heap_opts
  fail_collect(failures, "Connect must not use image-default -Xmx2G") if dc_heap_opts.match?(/-Xmx2[Gg]/)
  xmx = dc_heap_opts.match(/-Xmx(\d+)([MmGg])/)
  fail_collect(failures, "Connect KAFKA_HEAP_OPTS must include -XmxNNNM") unless xmx
  if xmx
    xmx_mi = xmx[2].upcase == "G" ? xmx[1].to_i * 1024 : xmx[1].to_i
    fail_collect(failures, "Connect -Xmx #{xmx_mi}Mi must be below memory limit #{dc_mem_limit}Mi") if xmx_mi >= dc_mem_limit
  end
  fail_collect(failures, "Connect KAFKA_HEAP_OPTS must include -Xms") unless dc_heap_opts.match?(/-Xms\d+[Mm]/)
end
fail_collect(failures, "register Job must PUT connector config") unless dr_text.include?("PUT") && dr_text.include?("/connectors/")
fail_collect(failures, "register Job must use uno-arena-local-credentials") unless dr_text.include?("uno-arena-local-credentials")
fail_collect(failures, "register Job must fail closed on FAILED") unless dr_text.match?(/FAILED|fail closed|failed closed/i)
fail_collect(failures, "register Job must capture status checker rc in else branch") unless dr_text.match?(/else\s+rc=\$\?/m) && dr_text.include?('[[ "${rc}" -eq 1 ]]')
fail_collect(failures, "register Job must require snapshot.mode=no_data") unless dr_text.include?("snapshot.mode must be no_data")
local_images_cm = File.read(File.join(MANIFESTS, "01-local-secrets.yaml"))
fail_collect(failures, "local images ConfigMap missing DEBEZIUM_CONNECT_IMAGE") unless local_images_cm.include?("DEBEZIUM_CONNECT_IMAGE")
fail_collect(failures, "local images ConfigMap must record Connect exact source") unless local_images_cm.include?(DEBEZIUM_CONNECT_SOURCE_IMAGE)
fail_collect(failures, "local images ConfigMap missing DEBEZIUM_SERVER_IMAGE") unless local_images_cm.include?("DEBEZIUM_SERVER_IMAGE")
fail_collect(failures, "local images ConfigMap must record Server exact source") unless local_images_cm.include?(DEBEZIUM_SERVER_SOURCE_IMAGE)
fail_collect(failures, "local images ConfigMap must not use Connect stale runtime tag") if local_images_cm.include?(DEBEZIUM_CONNECT_STALE_TAG)
fail_collect(failures, "local images ConfigMap must not use Server stale runtime tag") if local_images_cm.include?(DEBEZIUM_SERVER_STALE_TAG)
dc_struct = File.join(KIND, "scripts/test-debezium-connect-structure.sh")
dc_load = File.join(KIND, "scripts/load-debezium-connect.sh")
dc_status = File.join(KIND, "scripts/test-debezium-connectors.sh")
fail_collect(failures, "missing test-debezium-connect-structure.sh") unless File.file?(dc_struct)
fail_collect(failures, "missing load-debezium-connect.sh") unless File.file?(dc_load)
fail_collect(failures, "missing test-debezium-connectors.sh") unless File.file?(dc_status)
dc_load_text = File.read(dc_load)
fail_collect(failures, "Debezium Connect loader must record multiarch index") unless dc_load_text.include?(DEBEZIUM_CONNECT_MULTIARCH_DIGEST.delete_prefix("sha256:"))
fail_collect(failures, "Debezium Connect loader must stage exact source image") unless dc_load_text.include?(DEBEZIUM_CONNECT_SOURCE_IMAGE)
fail_collect(failures, "Debezium Connect loader must docker exec into kind node") unless dc_load_text.include?("docker exec")
fail_collect(failures, "Debezium Connect loader must crictl pull") unless dc_load_text.include?("crictl pull")
fail_collect(failures, "Debezium Connect loader must verify via crictl inspecti") unless dc_load_text.include?("crictl inspecti")
fail_collect(failures, "Debezium Connect loader must verify amd64") unless dc_load_text.include?('expected_arch="amd64"')
fail_collect(failures, "Debezium Connect loader must verify arm64") unless dc_load_text.include?('expected_arch="arm64"')
fail_collect(failures, "Debezium Connect loader must not kind load") if dc_load_text.match?(/kind\s+load\s+docker-image/)
fail_collect(failures, "Debezium Connect loader must remove stale runtime tag") unless dc_load_text.include?(DEBEZIUM_CONNECT_STALE_TAG)
fail_collect(failures, "Debezium Connect loader must fail closed on docker.io/library/import-") unless dc_load_text.include?("docker.io/library/import-")
fail_collect(failures, "Debezium Connect loader must fail closed on /import- repoDigests") unless dc_load_text.include?("/import-")
fail_collect(failures, "Debezium Connect loader must name stale kind-import metadata") unless dc_load_text.include?("stale kind-import metadata")
fail_collect(failures, "Debezium Connect loader import rejection must require reset/recreate") unless dc_load_text.match?(%r{reset/recreate|reset.*recreate})
fail_collect(failures, "Debezium Connect loader import rejection must require node-native loader rerun") unless dc_load_text.include?("node-native loader")
fail_collect(failures, "Debezium Connect loader must not encode ctr content / containerd restart remediation") if dc_load_text.match?(/ctr\s+content|content\s+remove|restart\s+containerd|containerd\s+restart/i)
fail_collect(failures, "Connect status script must not claim delivery alone") unless File.read(dc_status).match?(/does not|no delivery claim|not claim/i)
puts "ok debezium-kafka-connect"

# --- Game Integrity local envelope key config (ADR-0024) ---
local_secrets = File.read(File.join(MANIFESTS, "01-local-secrets.yaml"))
fail_collect(failures, "local secrets missing GI envelope provider ConfigMap") unless local_secrets.include?("uno-arena-local-gi-envelope")
fail_collect(failures, "local secrets missing GAME_INTEGRITY_ENVELOPE_PROVIDER=dev") unless local_secrets.match?(/GAME_INTEGRITY_ENVELOPE_PROVIDER:\s*dev/)
fail_collect(failures, "local secrets missing GAME_INTEGRITY_ENVELOPE_KEY_VERSION") unless local_secrets.include?("GAME_INTEGRITY_ENVELOPE_KEY_VERSION")
fail_collect(failures, "local secrets missing game-integrity-envelope-dev-keys") unless local_secrets.include?("game-integrity-envelope-dev-keys")
fail_collect(failures, "local secrets missing DEPLOYMENT_ENV=local") unless local_secrets.match?(/DEPLOYMENT_ENV:\s*local/)
gi_keys = local_secrets[/game-integrity-envelope-dev-keys:\s*"?([^"\n]+)"?/, 1]
if gi_keys.nil? || !gi_keys.match?(/\A\d+:[0-9a-fA-F]{64}\z/)
  fail_collect(failures, "GI local keyring must be version:64hex")
end
# Validator must render Helm / inspect binding, not string-only.
gi_chart = File.join(ROOT, "services/game-integrity/helm/game-integrity")
gi_kind_values = File.join(gi_chart, "values.kind.yaml")
fail_collect(failures, "missing values.kind.yaml for GI local binding") unless File.file?(gi_kind_values)
kind_vals = File.read(gi_kind_values)
fail_collect(failures, "values.kind.yaml must use existingSecret=uno-arena-local-credentials") unless kind_vals.include?("existingSecret: uno-arena-local-credentials")
fail_collect(failures, "values.kind.yaml must map GAME_INTEGRITY_ENVELOPE_DEV_KEYS") unless kind_vals.include?("GAME_INTEGRITY_ENVELOPE_DEV_KEYS: game-integrity-envelope-dev-keys")
fail_collect(failures, "values.kind.yaml must set DEPLOYMENT_ENV=local") unless kind_vals.match?(/DEPLOYMENT_ENV:\s*local/)
fail_collect(failures, "values.kind.yaml must set provider=dev") unless kind_vals.match?(/GAME_INTEGRITY_ENVELOPE_PROVIDER:\s*dev/)

helm_bin = ENV.fetch("HELM", "helm")
helm_ok = system("#{helm_bin} version >/dev/null 2>&1")
if helm_ok
  rendered = `#{helm_bin} template gi-local #{gi_chart} -f #{gi_kind_values} 2>&1`
  if $?.success?
    fail_collect(failures, "kind render missing secretKeyRef name uno-arena-local-credentials") unless rendered.include?("name: \"uno-arena-local-credentials\"") || rendered.include?("name: uno-arena-local-credentials")
    fail_collect(failures, "kind render missing GAME_INTEGRITY_ENVELOPE_DEV_KEYS binding") unless rendered.include?("GAME_INTEGRITY_ENVELOPE_DEV_KEYS") && rendered.include?("game-integrity-envelope-dev-keys")
    fail_collect(failures, "kind render missing DEPLOYMENT_ENV=local") unless rendered.include?("DEPLOYMENT_ENV") && rendered.include?("local")
    fail_collect(failures, "kind render must use repository:tag local image") unless rendered.match?(%r{image:\s*"?uno-arena/game-integrity:local"?})
    fail_collect(failures, "kind render must not emit empty digest image ref") if rendered.match?(%r{image:\s*"?uno-arena/game-integrity@"?})
    fail_collect(failures, "kind render missing IfNotPresent pull policy") unless rendered.include?("imagePullPolicy: IfNotPresent")
  else
    fail_collect(failures, "helm template values.kind.yaml failed: #{rendered.lines.first}")
  end
  # Digest-absent staging/prod overlays must fail closed (do not weaken production delivery).
  staging_render = `#{helm_bin} template gi-staging #{gi_chart} -f #{File.join(gi_chart, "values.yaml")} -f #{File.join(gi_chart, "values.staging.yaml")} 2>&1`
  fail_collect(failures, "staging without digest must fail helm render") if $?.success?
  prod_render = `#{helm_bin} template gi-prod #{gi_chart} -f #{File.join(gi_chart, "values.yaml")} -f #{File.join(gi_chart, "values.production.yaml")} 2>&1`
  fail_collect(failures, "production without digest must fail helm render") if $?.success?
else
  # Offline fallback: parse values.kind.yaml YAML structure for binding keys.
  begin
    require "yaml"
    docs = YAML.load_stream(File.read(gi_kind_values))
    doc = docs.find { |d| d.is_a?(Hash) } || {}
    fail_collect(failures, "values.kind existingSecret mismatch") unless doc["existingSecret"] == "uno-arena-local-credentials"
    secret_env = doc["secretEnv"] || {}
    fail_collect(failures, "values.kind secretEnv missing DEV_KEYS mapping") unless secret_env["GAME_INTEGRITY_ENVELOPE_DEV_KEYS"] == "game-integrity-envelope-dev-keys"
    env = doc["env"] || {}
    fail_collect(failures, "values.kind env DEPLOYMENT_ENV") unless env["DEPLOYMENT_ENV"] == "local"
    fail_collect(failures, "values.kind env provider") unless env["GAME_INTEGRITY_ENVELOPE_PROVIDER"] == "dev"
  rescue StandardError => e
    fail_collect(failures, "values.kind.yaml parse failed: #{e}")
  end
end

staging_vals = File.read(File.join(gi_chart, "values.staging.yaml"))
prod_vals = File.read(File.join(gi_chart, "values.production.yaml"))
fail_collect(failures, "staging must not map DEV_KEYS") if staging_vals.match?(/GAME_INTEGRITY_ENVELOPE_DEV_KEYS:\s*game-integrity/)
fail_collect(failures, "production must not map DEV_KEYS") if prod_vals.match?(/GAME_INTEGRITY_ENVELOPE_DEV_KEYS:\s*game-integrity/)
fail_collect(failures, "staging DEPLOYMENT_ENV must be staging") unless staging_vals.match?(/DEPLOYMENT_ENV:\s*staging/)
fail_collect(failures, "production DEPLOYMENT_ENV must be production") unless prod_vals.match?(/DEPLOYMENT_ENV:\s*production/)
fail_collect(failures, "staging provider must remain kms (fail-closed)") unless staging_vals.match?(/GAME_INTEGRITY_ENVELOPE_PROVIDER:\s*kms/)
fail_collect(failures, "production provider must remain kms (fail-closed)") unless prod_vals.match?(/GAME_INTEGRITY_ENVELOPE_PROVIDER:\s*kms/)
puts "ok gi-envelope-local-keys"

# --- Identity local durable + OIDC (ADR-0021/0023/0027) ---
fail_collect(failures, "local secrets missing identity-database-url") unless local_secrets.include?("identity-database-url")
fail_collect(failures, "local secrets missing identity-internal-credential") unless local_secrets.include?("identity-internal-credential")
fail_collect(failures, "local secrets missing Identity OIDC ConfigMap") unless local_secrets.include?("uno-arena-local-identity-oidc")
fail_collect(failures, "local secrets missing OIDC_ISSUER_URL") unless local_secrets.include?("OIDC_ISSUER_URL")

identity_chart = File.join(ROOT, "services/identity/helm/identity")
identity_kind_values = File.join(identity_chart, "values.kind.yaml")
fail_collect(failures, "missing values.kind.yaml for Identity local binding") unless File.file?(identity_kind_values)
id_kind_vals = File.read(identity_kind_values)
fail_collect(failures, "identity values.kind.yaml must use existingSecret=uno-arena-local-credentials") unless id_kind_vals.include?("existingSecret: uno-arena-local-credentials")
fail_collect(failures, "identity values.kind.yaml must map DATABASE_URL") unless id_kind_vals.include?("DATABASE_URL: identity-database-url")
fail_collect(failures, "identity values.kind.yaml must set DEPLOYMENT_ENV=local") unless id_kind_vals.match?(/DEPLOYMENT_ENV:\s*local/)
fail_collect(failures, "identity values.kind.yaml must set kind: true") unless id_kind_vals.match?(/^kind:\s*true\b/)
fail_collect(failures, "identity chart missing _helpers.tpl") unless File.file?(File.join(identity_chart, "templates/_helpers.tpl"))
fail_collect(failures, "identity chart missing helm-test.sh") unless File.file?(File.join(identity_chart, "helm-test.sh"))

# No static Identity Deployment duplicate under kind manifests (Helm-only acceptance).
each_manifest(MANIFESTS) do |path, doc|
  next unless doc.is_a?(Hash) && doc["kind"] == "Deployment"
  name = doc.dig("metadata", "name").to_s
  next unless name == "identity"
  fail_collect(failures, "static identity Deployment forbidden in #{path}; deploy via Helm script")
end

if helm_ok
  id_rendered = `#{helm_bin} template identity-local #{identity_chart} -f #{identity_kind_values} 2>&1`
  if $?.success?
    fail_collect(failures, "identity kind render missing uno-arena-local-credentials") unless id_rendered.include?("uno-arena-local-credentials")
    fail_collect(failures, "identity kind render must use repository:tag local image") unless id_rendered.match?(%r{image:\s*"?uno-arena/identity:local"?})
    fail_collect(failures, "identity kind render must not emit empty digest image ref") if id_rendered.match?(%r{image:\s*"?uno-arena/identity@"?})
    fail_collect(failures, "identity kind render missing DATABASE_URL binding") unless id_rendered.include?("DATABASE_URL") && id_rendered.include?("identity-database-url")
    fail_collect(failures, "identity kind render must not use NodePort/LoadBalancer") if id_rendered.match?(/type:\s*(NodePort|LoadBalancer)/)
  else
    fail_collect(failures, "helm template identity values.kind.yaml failed: #{id_rendered.lines.first}")
  end
  id_staging = `#{helm_bin} template identity-staging #{identity_chart} -f #{File.join(identity_chart, "values.yaml")} -f #{File.join(identity_chart, "values.staging.yaml")} 2>&1`
  fail_collect(failures, "identity staging without digest must fail helm render") if $?.success?
  id_prod = `#{helm_bin} template identity-prod #{identity_chart} -f #{File.join(identity_chart, "values.yaml")} -f #{File.join(identity_chart, "values.production.yaml")} 2>&1`
  fail_collect(failures, "identity production without digest must fail helm render") if $?.success?
end
puts "ok identity-kind-helm"

# --- Room Gameplay local durable + Redis timer (ADR-0019/0027) ---
fail_collect(failures, "local secrets missing room-database-url") unless local_secrets.include?("room-database-url")
fail_collect(failures, "local secrets missing room-timer-service-credential") unless local_secrets.include?("room-timer-service-credential")

room_chart = File.join(ROOT, "services/room-gameplay/helm/room-gameplay")
room_kind_values = File.join(room_chart, "values.kind.yaml")
fail_collect(failures, "missing values.kind.yaml for Room local binding") unless File.file?(room_kind_values)
room_kind_vals = File.read(room_kind_values)
fail_collect(failures, "room values.kind.yaml must use existingSecret=uno-arena-local-credentials") unless room_kind_vals.include?("existingSecret: uno-arena-local-credentials")
fail_collect(failures, "room values.kind.yaml must map DATABASE_URL") unless room_kind_vals.include?("DATABASE_URL: room-database-url")
fail_collect(failures, "room values.kind.yaml must set DEPLOYMENT_ENV=local") unless room_kind_vals.match?(/DEPLOYMENT_ENV:\s*local/)
fail_collect(failures, "room values.kind.yaml must set kind: true") unless room_kind_vals.match?(/^kind:\s*true\b/)
fail_collect(failures, "room values.kind.yaml must enable timerWorker") unless room_kind_vals.match?(/timerWorker:\s*\n\s*enabled:\s*true/)
fail_collect(failures, "room chart missing _helpers.tpl") unless File.file?(File.join(room_chart, "templates/_helpers.tpl"))
fail_collect(failures, "room chart missing helm-test.sh") unless File.file?(File.join(room_chart, "helm-test.sh"))

each_manifest(MANIFESTS) do |path, doc|
  next unless doc.is_a?(Hash) && doc["kind"] == "Deployment"
  name = doc.dig("metadata", "name").to_s
  next unless name == "room-gameplay"
  fail_collect(failures, "static room-gameplay Deployment forbidden in #{path}; deploy via Helm script")
end

if helm_ok
  room_rendered = `#{helm_bin} template room-local #{room_chart} -f #{room_kind_values} 2>&1`
  if $?.success?
    fail_collect(failures, "room kind render missing uno-arena-local-credentials") unless room_rendered.include?("uno-arena-local-credentials")
    fail_collect(failures, "room kind render must use repository:tag local image") unless room_rendered.match?(%r{image:\s*"?uno-arena/room-gameplay:local"?})
    fail_collect(failures, "room kind render must not emit empty digest image ref") if room_rendered.match?(%r{image:\s*"?uno-arena/room-gameplay@"?})
    fail_collect(failures, "room kind render missing DATABASE_URL binding") unless room_rendered.include?("DATABASE_URL") && room_rendered.include?("room-database-url")
    fail_collect(failures, "room kind render missing WORKER_ROLE=room-timer") unless room_rendered.include?("room-timer")
    fail_collect(failures, "room kind render must not use NodePort/LoadBalancer") if room_rendered.match?(/type:\s*(NodePort|LoadBalancer)/)
  else
    fail_collect(failures, "helm template room values.kind.yaml failed: #{room_rendered.lines.first}")
  end
  room_staging = `#{helm_bin} template room-staging #{room_chart} -f #{File.join(room_chart, "values.yaml")} -f #{File.join(room_chart, "values.staging.yaml")} 2>&1`
  fail_collect(failures, "room staging without digest must fail helm render") if $?.success?
  room_prod = `#{helm_bin} template room-prod #{room_chart} -f #{File.join(room_chart, "values.yaml")} -f #{File.join(room_chart, "values.production.yaml")} 2>&1`
  fail_collect(failures, "room production without digest must fail helm render") if $?.success?
end
puts "ok room-kind-helm"

# --- Tournament Orchestration local durable Postgres (ADR-0016/0027) ---
fail_collect(failures, "local secrets missing tournament-database-url") unless local_secrets.include?("tournament-database-url")
fail_collect(failures, "local secrets missing tournament-internal-credential") unless local_secrets.include?("tournament-internal-credential")

tournament_chart = File.join(ROOT, "services/tournament-orchestration/helm/tournament-orchestration")
tournament_kind_values = File.join(tournament_chart, "values.kind.yaml")
fail_collect(failures, "missing values.kind.yaml for Tournament local binding") unless File.file?(tournament_kind_values)
tour_kind_vals = File.read(tournament_kind_values)
fail_collect(failures, "tournament values.kind.yaml must use existingSecret=uno-arena-local-credentials") unless tour_kind_vals.include?("existingSecret: uno-arena-local-credentials")
fail_collect(failures, "tournament values.kind.yaml must map DATABASE_URL") unless tour_kind_vals.include?("DATABASE_URL: tournament-database-url")
fail_collect(failures, "tournament values.kind.yaml must set DEPLOYMENT_ENV=local") unless tour_kind_vals.match?(/DEPLOYMENT_ENV:\s*local/)
fail_collect(failures, "tournament values.kind.yaml must set kind: true") unless tour_kind_vals.match?(/^kind:\s*true\b/)
fail_collect(failures, "tournament values.kind.yaml must keep provisioningWorker enabled for kind") unless tour_kind_vals.match?(/provisioningWorker:\s*\n\s*enabled:\s*true/)
fail_collect(failures, "tournament values.kind.yaml must keep seedingWorker enabled for kind") unless tour_kind_vals.match?(/seedingWorker:\s*\n\s*enabled:\s*true/)
fail_collect(failures, "tournament values.kind.yaml must keep completionWorker enabled for kind") unless tour_kind_vals.match?(/completionWorker:\s*\n\s*enabled:\s*true/)
fail_collect(failures, "tournament chart missing _helpers.tpl") unless File.file?(File.join(tournament_chart, "templates/_helpers.tpl"))
fail_collect(failures, "tournament chart missing helm-test.sh") unless File.file?(File.join(tournament_chart, "helm-test.sh"))
fail_collect(failures, "tournament chart missing seeding-worker-deployment.yaml") unless File.file?(File.join(tournament_chart, "templates/seeding-worker-deployment.yaml"))
fail_collect(failures, "tournament chart missing completion-worker-deployment.yaml") unless File.file?(File.join(tournament_chart, "templates/completion-worker-deployment.yaml"))

each_manifest(MANIFESTS) do |path, doc|
  next unless doc.is_a?(Hash) && doc["kind"] == "Deployment"
  name = doc.dig("metadata", "name").to_s
  next unless name == "tournament-orchestration"
  fail_collect(failures, "static tournament-orchestration Deployment forbidden in #{path}; deploy via Helm script")
end

if helm_ok
  tour_rendered = `#{helm_bin} template tournament-local #{tournament_chart} -f #{tournament_kind_values} 2>&1`
  if $?.success?
    fail_collect(failures, "tournament kind render missing uno-arena-local-credentials") unless tour_rendered.include?("uno-arena-local-credentials")
    fail_collect(failures, "tournament kind render must use repository:tag local image") unless tour_rendered.match?(%r{image:\s*"?uno-arena/tournament-orchestration:local"?})
    fail_collect(failures, "tournament kind render must not emit empty digest image ref") if tour_rendered.match?(%r{image:\s*"?uno-arena/tournament-orchestration@"?})
    fail_collect(failures, "tournament kind render missing DATABASE_URL binding") unless tour_rendered.include?("DATABASE_URL") && tour_rendered.include?("tournament-database-url")
    fail_collect(failures, "tournament kind render must not use NodePort/LoadBalancer") if tour_rendered.match?(/type:\s*(NodePort|LoadBalancer)/)
    fail_collect(failures, "tournament kind render missing provisioning worker") unless tour_rendered.include?("tournament-provisioning")
    fail_collect(failures, "tournament kind render missing seeding worker") unless tour_rendered.include?("tournament-seeding")
    fail_collect(failures, "tournament kind render missing completion worker") unless tour_rendered.include?("tournament-round-completion")
  else
    fail_collect(failures, "helm template tournament values.kind.yaml failed: #{tour_rendered.lines.first}")
  end
  tour_staging = `#{helm_bin} template tournament-staging #{tournament_chart} -f #{File.join(tournament_chart, "values.yaml")} -f #{File.join(tournament_chart, "values.staging.yaml")} 2>&1`
  fail_collect(failures, "tournament staging without digest must fail helm render") if $?.success?
  if tour_staging.include?("tournament-seeding") || tour_staging.include?("seeding-worker")
    fail_collect(failures, "tournament staging must keep seeding worker disabled")
  end
  if tour_staging.include?("tournament-round-completion") || tour_staging.include?("completion-worker")
    fail_collect(failures, "tournament staging must keep completion worker disabled")
  end
  tour_prod = `#{helm_bin} template tournament-prod #{tournament_chart} -f #{File.join(tournament_chart, "values.yaml")} -f #{File.join(tournament_chart, "values.production.yaml")} 2>&1`
  fail_collect(failures, "tournament production without digest must fail helm render") if $?.success?
  if tour_prod.include?("tournament-seeding") || tour_prod.include?("seeding-worker")
    fail_collect(failures, "tournament production must keep seeding worker disabled")
  end
  if tour_prod.include?("tournament-round-completion") || tour_prod.include?("completion-worker")
    fail_collect(failures, "tournament production must keep completion worker disabled")
  end
end
puts "ok tournament-kind-helm"

# --- Ranking local durable Postgres (ADR-0016/0027) ---
fail_collect(failures, "local secrets missing ranking-database-url") unless local_secrets.include?("ranking-database-url")
fail_collect(failures, "local secrets missing ranking-internal-credential") unless local_secrets.include?("ranking-internal-credential")

ranking_chart = File.join(ROOT, "services/ranking/helm/ranking")
ranking_kind_values = File.join(ranking_chart, "values.kind.yaml")
fail_collect(failures, "missing values.kind.yaml for Ranking local binding") unless File.file?(ranking_kind_values)
rank_kind_vals = File.read(ranking_kind_values)
fail_collect(failures, "ranking values.kind.yaml must use existingSecret=uno-arena-local-credentials") unless rank_kind_vals.include?("existingSecret: uno-arena-local-credentials")
fail_collect(failures, "ranking values.kind.yaml must map DATABASE_URL") unless rank_kind_vals.include?("DATABASE_URL: ranking-database-url")
fail_collect(failures, "ranking values.kind.yaml must set DEPLOYMENT_ENV=local") unless rank_kind_vals.match?(/DEPLOYMENT_ENV:\s*local/)
fail_collect(failures, "ranking values.kind.yaml must set kind: true") unless rank_kind_vals.match?(/^kind:\s*true\b/)
fail_collect(failures, "ranking chart missing _helpers.tpl") unless File.file?(File.join(ranking_chart, "templates/_helpers.tpl"))
fail_collect(failures, "ranking chart missing helm-test.sh") unless File.file?(File.join(ranking_chart, "helm-test.sh"))

each_manifest(MANIFESTS) do |path, doc|
  next unless doc.is_a?(Hash) && doc["kind"] == "Deployment"
  name = doc.dig("metadata", "name").to_s
  next unless name == "ranking"
  fail_collect(failures, "static ranking Deployment forbidden in #{path}; deploy via Helm script")
end

if helm_ok
  rank_rendered = `#{helm_bin} template ranking-local #{ranking_chart} -f #{ranking_kind_values} 2>&1`
  if $?.success?
    fail_collect(failures, "ranking kind render missing uno-arena-local-credentials") unless rank_rendered.include?("uno-arena-local-credentials")
    fail_collect(failures, "ranking kind render must use repository:tag local image") unless rank_rendered.match?(%r{image:\s*"?uno-arena/ranking:local"?})
    fail_collect(failures, "ranking kind render must not emit empty digest image ref") if rank_rendered.match?(%r{image:\s*"?uno-arena/ranking@"?})
    fail_collect(failures, "ranking kind render missing DATABASE_URL binding") unless rank_rendered.include?("DATABASE_URL") && rank_rendered.include?("ranking-database-url")
    fail_collect(failures, "ranking kind render must not use NodePort/LoadBalancer") if rank_rendered.match?(/type:\s*(NodePort|LoadBalancer)/)
  else
    fail_collect(failures, "helm template ranking values.kind.yaml failed: #{rank_rendered.lines.first}")
  end
  rank_staging = `#{helm_bin} template ranking-staging #{ranking_chart} -f #{File.join(ranking_chart, "values.yaml")} -f #{File.join(ranking_chart, "values.staging.yaml")} 2>&1`
  fail_collect(failures, "ranking staging without digest must fail helm render") if $?.success?
  rank_prod = `#{helm_bin} template ranking-prod #{ranking_chart} -f #{File.join(ranking_chart, "values.yaml")} -f #{File.join(ranking_chart, "values.production.yaml")} 2>&1`
  fail_collect(failures, "ranking production without digest must fail helm render") if $?.success?
end
puts "ok ranking-kind-helm"

# --- Spectator View kind Helm binding ---
spectator_chart = File.join(ROOT, "services/spectator-view/helm/spectator-view")
spectator_kind_values = File.join(spectator_chart, "values.kind.yaml")
fail_collect(failures, "missing values.kind.yaml for Spectator local binding") unless File.file?(spectator_kind_values)
spec_kind_vals = File.read(spectator_kind_values)
fail_collect(failures, "spectator values.kind.yaml must use existingSecret=uno-arena-local-credentials") unless spec_kind_vals.include?("existingSecret: uno-arena-local-credentials")
fail_collect(failures, "spectator values.kind.yaml must set REDIS_URL DB 5") unless spec_kind_vals.match?(%r{REDIS_URL:.*6379/5})
fail_collect(failures, "spectator values.kind.yaml must set DEPLOYMENT_ENV=local") unless spec_kind_vals.match?(/DEPLOYMENT_ENV:\s*local/)
fail_collect(failures, "spectator values.kind.yaml must set kind: true") unless spec_kind_vals.match?(/^kind:\s*true\b/)
fail_collect(failures, "spectator values.kind.yaml must enable projectionRebuilder after live recovery proof") unless spec_kind_vals.match?(/projectionRebuilder:\s*\n\s*enabled:\s*true/)
fail_collect(failures, "spectator values.kind.yaml must set ROOM_GAMEPLAY_URL for rebuilder") unless spec_kind_vals.include?("ROOM_GAMEPLAY_URL")
fail_collect(failures, "spectator values.kind.yaml must map ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL") unless spec_kind_vals.include?("ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL")
fail_collect(failures, "spectator values.kind.yaml must set KAFKA_PROJECTION_REBUILD_TOPIC") unless spec_kind_vals.include?("KAFKA_PROJECTION_REBUILD_TOPIC")
fail_collect(failures, "spectator values.kind.yaml must set KAFKA_BROKERS") unless spec_kind_vals.include?("KAFKA_BROKERS")
fail_collect(failures, "spectator values.kind.yaml must set KAFKA_SPECTATOR_SAFE_TOPIC") unless spec_kind_vals.include?("KAFKA_SPECTATOR_SAFE_TOPIC")
fail_collect(failures, "spectator values.kind.yaml must set KAFKA_SPECTATOR_SAFE_DLQ_TOPIC") unless spec_kind_vals.include?("KAFKA_SPECTATOR_SAFE_DLQ_TOPIC")
fail_collect(failures, "missing spectator _helpers.tpl") unless File.file?(File.join(spectator_chart, "templates/_helpers.tpl"))
fail_collect(failures, "missing spectator helm-test.sh") unless File.file?(File.join(spectator_chart, "helm-test.sh"))
if helm_ok
  spec_rendered = `#{helm_bin} template spectator-kind #{spectator_chart} -f #{spectator_kind_values} 2>&1`
  unless $?.success?
    fail_collect(failures, "helm template spectator values.kind.yaml failed: #{spec_rendered.lines.first}")
  end
  fail_collect(failures, "spectator kind render must use local tag image") unless spec_rendered.include?("uno-arena/spectator-view:local")
  fail_collect(failures, "spectator kind render must be ClusterIP") unless spec_rendered.match?(/type:\s*ClusterIP/)
  fail_collect(failures, "spectator kind render must include REDIS_URL") unless spec_rendered.include?("REDIS_URL")
  fail_collect(failures, "spectator kind render must include projection-rebuilder after live recovery proof") unless spec_rendered.include?("spectator-projection-rebuilder")
  # Template privilege checks use an explicit enable flip — not a live recovery claim.
  spec_rebuilder = `#{helm_bin} template spectator-kind-rebuilder #{spectator_chart} -f #{spectator_kind_values} --set projectionRebuilder.enabled=true 2>&1`
  unless $?.success?
    fail_collect(failures, "helm template spectator rebuilder enable flip failed: #{spec_rebuilder.lines.first}")
  end
  fail_collect(failures, "spectator rebuilder template must include WORKER_ROLE") unless spec_rebuilder.include?("spectator-projection-rebuilder")
  spec_rebuilder_ports = spec_rebuilder.match(/- name: projection-rebuilder[\s\S]*?ports:\s*([\s\S]*?)\n\s*env:/)&.captures&.first.to_s
  fail_collect(failures, "spectator rebuilder must expose only the private metrics port") unless spec_rebuilder_ports.match?(/name:\s*metrics[\s\S]*?containerPort:\s*9090/)
  fail_collect(failures, "spectator rebuilder must not expose the application port") if spec_rebuilder_ports.match?(/containerPort:\s*8080/)
  fail_collect(failures, "spectator rebuilder must not use readinessProbe") if spec_rebuilder.match?(/name: projection-rebuilder[\s\S]*?readinessProbe:/)
  fail_collect(failures, "spectator rebuilder must not mount SPECTATOR_VIEW_INTERNAL_CREDENTIAL") if spec_rebuilder.match?(/name: projection-rebuilder[\s\S]*?SPECTATOR_VIEW_INTERNAL_CREDENTIAL/)
  fail_collect(failures, "spectator rebuilder must mount ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL") unless spec_rebuilder.include?("ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL")
  spec_staging = `#{helm_bin} template spectator-staging #{spectator_chart} -f #{File.join(spectator_chart, "values.yaml")} -f #{File.join(spectator_chart, "values.staging.yaml")} 2>&1`
  fail_collect(failures, "spectator staging without digest must fail helm render") if $?.success?
  spec_prod = `#{helm_bin} template spectator-prod #{spectator_chart} -f #{File.join(spectator_chart, "values.yaml")} -f #{File.join(spectator_chart, "values.production.yaml")} 2>&1`
  fail_collect(failures, "spectator production without digest must fail helm render") if $?.success?
end
puts "ok spectator-kind-helm"

# --- Gateway kind Helm binding ---
gateway_chart = File.join(ROOT, "services/gateway/helm/gateway")
gateway_kind_values = File.join(gateway_chart, "values.kind.yaml")
fail_collect(failures, "missing values.kind.yaml for Gateway local binding") unless File.file?(gateway_kind_values)
gw_kind_vals = File.read(gateway_kind_values)
fail_collect(failures, "gateway values.kind.yaml must use existingSecret=uno-arena-local-credentials") unless gw_kind_vals.include?("existingSecret: uno-arena-local-credentials")
fail_collect(failures, "gateway values.kind.yaml must map GATEWAY_IDENTITY_SERVICE_CREDENTIAL to identity-internal-credential") unless gw_kind_vals.match?(/GATEWAY_IDENTITY_SERVICE_CREDENTIAL:\s*identity-internal-credential/)
fail_collect(failures, "gateway values.kind.yaml must map GATEWAY_ROOM_SERVICE_CREDENTIAL to room-service-credential") unless gw_kind_vals.match?(/GATEWAY_ROOM_SERVICE_CREDENTIAL:\s*room-service-credential/)
fail_collect(failures, "gateway values.kind.yaml must map GATEWAY_TOURNAMENT_SERVICE_CREDENTIAL to tournament-internal-credential") unless gw_kind_vals.match?(/GATEWAY_TOURNAMENT_SERVICE_CREDENTIAL:\s*tournament-internal-credential/)
fail_collect(failures, "gateway values.kind.yaml must map GATEWAY_SPECTATOR_SERVICE_CREDENTIAL to spectator-view-internal-credential") unless gw_kind_vals.match?(/GATEWAY_SPECTATOR_SERVICE_CREDENTIAL:\s*spectator-view-internal-credential/)
fail_collect(failures, "gateway values.kind.yaml must set REDIS_URL DB 6") unless gw_kind_vals.match?(%r{REDIS_URL:.*6379/6})
fail_collect(failures, "gateway values.kind.yaml must set GATEWAY_PLAYER_FEED_REDIS_URL DB 2") unless gw_kind_vals.match?(%r{GATEWAY_PLAYER_FEED_REDIS_URL:.*6379/2})
fail_collect(failures, "gateway values.kind.yaml must set GATEWAY_SPECTATOR_REDIS_URL DB 5") unless gw_kind_vals.match?(%r{GATEWAY_SPECTATOR_REDIS_URL:.*6379/5})
fail_collect(failures, "gateway values.kind.yaml must set DEPLOYMENT_ENV=local") unless gw_kind_vals.match?(/DEPLOYMENT_ENV:\s*local/)
fail_collect(failures, "gateway values.kind.yaml must set kind: true") unless gw_kind_vals.match?(/^kind:\s*true\b/)
fail_collect(failures, "gateway values.kind.yaml must set KAFKA_BROKERS") unless gw_kind_vals.include?("KAFKA_BROKERS")
fail_collect(failures, "gateway values.kind.yaml must set KAFKA_CONSUMER_GROUP=gateway") unless gw_kind_vals.match?(/KAFKA_CONSUMER_GROUP:\s*gateway/)
fail_collect(failures, "gateway values.kind.yaml must set KAFKA_SESSION_INVALIDATED_TOPIC") unless gw_kind_vals.include?("identity.session.invalidated")
fail_collect(failures, "gateway values.kind.yaml must set gateway DLQ topic") unless gw_kind_vals.include?("identity.session.invalidated.gateway.dlq")
fail_collect(failures, "gateway values.kind.yaml must set GATEWAY_SESSION_INVALIDATION_TTL") unless gw_kind_vals.include?("GATEWAY_SESSION_INVALIDATION_TTL")
fail_collect(failures, "gateway values.kind.yaml SI TTL must be 7h (ADR-0029: kind source 30m + DLQ 6h)") unless gw_kind_vals.match?(/GATEWAY_SESSION_INVALIDATION_TTL:\s*"7h"/)
fail_collect(failures, "gateway values.kind.yaml must document ADR-0029 SI TTL rationale") unless gw_kind_vals.include?("ADR-0029")
fail_collect(failures, "gateway values.kind.yaml must not claim Debezium PENDING") if gw_kind_vals.match?(/debezium.*PENDING|PENDING.*[Dd]ebezium/i)
fail_collect(failures, "gateway values.kind.yaml must omit GATEWAY_CAPABILITY_MODE") if gw_kind_vals.include?("GATEWAY_CAPABILITY_MODE")
fail_collect(failures, "gateway values.kind.yaml must omit GATEWAY_ALLOW_FAKES") if gw_kind_vals.include?("GATEWAY_ALLOW_FAKES")
fail_collect(failures, "missing gateway _helpers.tpl") unless File.file?(File.join(gateway_chart, "templates/_helpers.tpl"))
fail_collect(failures, "missing gateway helm-test.sh") unless File.file?(File.join(gateway_chart, "helm-test.sh"))
fail_collect(failures, "missing gateway SI Redis admission live proof script") unless File.file?(File.join(KIND, "scripts/test-gateway-si-redis-admission-live.sh"))
fail_collect(failures, "missing gateway session-invalidation live alias script") unless File.file?(File.join(KIND, "scripts/test-gateway-session-invalidation-live.sh"))
if helm_ok
  gw_rendered = `#{helm_bin} template gateway-kind #{gateway_chart} -f #{gateway_kind_values} 2>&1`
  unless $?.success?
    fail_collect(failures, "helm template gateway values.kind.yaml failed: #{gw_rendered.lines.first}")
  end
  fail_collect(failures, "gateway kind render must use local tag image") unless gw_rendered.include?("uno-arena/gateway:local")
  fail_collect(failures, "gateway kind render must be ClusterIP") unless gw_rendered.match?(/type:\s*ClusterIP/)
  fail_collect(failures, "gateway kind render must include REDIS_URL") unless gw_rendered.include?("REDIS_URL")
  fail_collect(failures, "gateway kind render must include KAFKA_BROKERS") unless gw_rendered.include?("KAFKA_BROKERS")
  fail_collect(failures, "gateway kind render must include session invalidated topic") unless gw_rendered.include?("identity.session.invalidated")
  gw_staging = `#{helm_bin} template gateway-staging #{gateway_chart} -f #{File.join(gateway_chart, "values.yaml")} -f #{File.join(gateway_chart, "values.staging.yaml")} 2>&1`
  fail_collect(failures, "gateway staging without digest must fail helm render") if $?.success?
  gw_prod = `#{helm_bin} template gateway-prod #{gateway_chart} -f #{File.join(gateway_chart, "values.yaml")} -f #{File.join(gateway_chart, "values.production.yaml")} 2>&1`
  fail_collect(failures, "gateway production without digest must fail helm render") if $?.success?
end
puts "ok gateway-kind-helm"

# --- Four distinct Postgres Services/DBs + per-context admin secrets ---
pg_services = []
pg_dbs = {}
each_manifest(File.join(MANIFESTS, "10-postgres")) do |_path, doc|
  next unless doc.is_a?(Hash)
  if doc["kind"] == "Service"
    pg_services << doc.dig("metadata", "name")
  end
  if doc["kind"] == "Deployment"
    name = doc.dig("metadata", "name")
    env_list = doc.dig("spec", "template", "spec", "containers", 0, "env") || []
    db = env_list.find { |e| e["name"] == "POSTGRES_DB" }&.fetch("value", nil)
    pg_dbs[name] = db
    admin_key = env_list.find { |e| e["name"] == "POSTGRES_USER" }&.dig("valueFrom", "secretKeyRef", "key")
    fail_collect(failures, "#{name} must use context-specific admin secret key") if admin_key == "postgres-admin-user" || admin_key.nil?
  end
end

POSTGRES_SERVICES.each do |name|
  fail_collect(failures, "missing Postgres Service #{name}") unless pg_services.include?(name)
  fail_collect(failures, "missing Postgres DB for #{name}") unless pg_dbs[name] == POSTGRES_DBS[name]
end
fail_collect(failures, "Postgres Services not distinct") unless pg_services.uniq.size == 4
fail_collect(failures, "Postgres DBs not distinct") unless pg_dbs.values.uniq.size == 4
fail_collect(failures, "expected exactly 4 Postgres DB boundaries") unless pg_dbs.size == 4

# Logical replication readiness (CDC prerequisite; disposable emptyDir retained).
each_manifest(File.join(MANIFESTS, "10-postgres")) do |path, doc|
  next unless doc.is_a?(Hash) && doc["kind"] == "Deployment"
  container = doc.dig("spec", "template", "spec", "containers", 0) || {}
  args = container["args"] || []
  joined = args.join(" ")
  fail_collect(failures, "#{doc.dig('metadata', 'name')} missing wal_level=logical") unless joined.include?("wal_level=logical")
  fail_collect(failures, "#{doc.dig('metadata', 'name')} missing bounded max_replication_slots") unless joined.include?("max_replication_slots=")
  fail_collect(failures, "#{doc.dig('metadata', 'name')} missing bounded max_wal_senders") unless joined.include?("max_wal_senders=")
  vols = doc.dig("spec", "template", "spec", "volumes") || []
  data = vols.find { |v| v["name"] == "data" }
  fail_collect(failures, "#{path} must retain emptyDir data volume") unless data&.key?("emptyDir")
end
puts "ok postgres-separation"

# --- Bootstrap job count/ownership + env/action contract ---
bootstrap_jobs = {}
each_manifest(File.join(MANIFESTS, "70-bootstrap")) do |_path, doc|
  next unless doc.is_a?(Hash) && doc["kind"] == "Job"
  name = doc.dig("metadata", "name")
  ctx = doc.dig("metadata", "labels", "uno-arena.local/context")
  bootstrap_jobs[name] = ctx
  names = env_names(doc)
  if name.start_with?("bootstrap-postgres-")
    %w[ADMIN_USER ADMIN_PASSWORD BOOTSTRAP_USER BOOTSTRAP_PASSWORD RUNTIME_USER RUNTIME_PASSWORD
       ADVISORY_LOCK_KEY EXPECTED_VERSION MIGRATION_FILE CONTEXT_NAME
       CDC_USER CDC_PASSWORD CDC_PUBLICATION CDC_TABLE].each do |req|
      fail_collect(failures, "#{name} missing env #{req}") unless names.include?(req)
    end
    # Runtime vs DDL credential separation: distinct secret keys
    env_list = doc.dig("spec", "template", "spec", "containers", 0, "env") || []
    keys = env_list.to_h { |e| [e["name"], e.dig("valueFrom", "secretKeyRef", "key")] }
    vals = env_list.to_h { |e| [e["name"], e["value"]] }
    if keys["BOOTSTRAP_USER"] && keys["RUNTIME_USER"] && keys["BOOTSTRAP_USER"] == keys["RUNTIME_USER"]
      fail_collect(failures, "#{name} bootstrap/runtime user keys must differ")
    end
    if keys["ADMIN_USER"] && keys["BOOTSTRAP_USER"] && keys["ADMIN_USER"] == keys["BOOTSTRAP_USER"]
      fail_collect(failures, "#{name} admin/bootstrap user keys must differ")
    end
    if keys["CDC_USER"] && keys["RUNTIME_USER"] && keys["CDC_USER"] == keys["RUNTIME_USER"]
      fail_collect(failures, "#{name} CDC/runtime user keys must differ")
    end
    if keys["CDC_USER"] && keys["BOOTSTRAP_USER"] && keys["CDC_USER"] == keys["BOOTSTRAP_USER"]
      fail_collect(failures, "#{name} CDC/bootstrap user keys must differ")
    end
    if name == "bootstrap-postgres-room-gameplay"
      %w[CDC_REALTIME_USER CDC_REALTIME_PASSWORD CDC_REALTIME_PUBLICATION CDC_REALTIME_TABLE CDC_PEER_TABLE].each do |req|
        fail_collect(failures, "#{name} missing env #{req}") unless names.include?(req)
      end
      fail_collect(failures, "room CDC_TABLE must be integration_outbox_events") unless vals["CDC_TABLE"] == "integration_outbox_events"
      fail_collect(failures, "room CDC_PEER_TABLE must be realtime_outbox_events") unless vals["CDC_PEER_TABLE"] == "realtime_outbox_events"
      fail_collect(failures, "room CDC_REALTIME_TABLE must be realtime_outbox_events") unless vals["CDC_REALTIME_TABLE"] == "realtime_outbox_events"
      if keys["CDC_USER"] && keys["CDC_REALTIME_USER"] && keys["CDC_USER"] == keys["CDC_REALTIME_USER"]
        fail_collect(failures, "room kafka/realtime CDC user keys must differ")
      end
    end
  end
  if name == "bootstrap-clickhouse-analytics"
    %w[ADMIN_USER BOOTSTRAP_USER RUNTIME_USER EXPECTED_VERSION MIGRATION_FILE].each do |req|
      fail_collect(failures, "#{name} missing env #{req}") unless names.include?(req)
    end
  end
end
each_manifest(GENERATED) do |_path, doc|
  next unless doc.is_a?(Hash) && doc["kind"] == "Job"
  name = doc.dig("metadata", "name")
  bootstrap_jobs[name] = doc.dig("metadata", "labels", "uno-arena.local/context")
end

EXPECTED_BOOTSTRAP_JOBS.each do |name, ctx|
  fail_collect(failures, "missing bootstrap Job #{name}") unless bootstrap_jobs.key?(name)
  if ctx && bootstrap_jobs[name] != ctx
    fail_collect(failures, "bootstrap Job #{name} context=#{bootstrap_jobs[name].inspect} expected=#{ctx}")
  end
end
fail_collect(failures, "expected 6 bootstrap Jobs, found #{bootstrap_jobs.size}") unless bootstrap_jobs.size == 6
puts "ok bootstrap-jobs"

# --- Postgres atomic single-session gate (zero pre-gate mutations) ---
pg_boot = File.read(File.join(BOOTSTRAP, "bin/bootstrap-postgres.sh"))
fail_collect(failures, "postgres bootstrap must use --single-transaction") unless pg_boot.include?("--single-transaction")
fail_collect(failures, "postgres bootstrap must use pg_advisory_xact_lock") unless pg_boot.include?("pg_advisory_xact_lock")
fail_collect(failures, "postgres bootstrap must not use session pg_advisory_lock for gate") if pg_boot.match?(/pg_advisory_lock\(/) && !pg_boot.include?("pg_advisory_xact_lock")
fail_collect(failures, "postgres bootstrap must write schema_bootstrap_meta checksum") unless pg_boot.include?("schema_bootstrap_meta")
fail_collect(failures, "postgres bootstrap must \\gset boolean branch vars (no bash multi-row parse)") unless pg_boot.include?("\\gset")
fail_collect(failures, "postgres bootstrap must SET ROLE bootstrap DDL for migration ownership") unless pg_boot.include?("SET ROLE")
fail_collect(failures, "postgres bootstrap must RESET ROLE before runtime grants") unless pg_boot.include?("RESET ROLE")

# psql \\if consumes a boolean variable, not an equality expression.
fail_collect(failures, "postgres gate must not use expression-style \\if") if pg_boot.match?(/\\if\s+:'[^']+'\s*=/)
%w[do_apply do_exact do_fail].each do |var|
  fail_collect(failures, "postgres gate must SELECT AS #{var} for \\gset") unless pg_boot.match?(/AS\s+#{var}\b/i)
end
fail_collect(failures, "postgres gate must \\if :do_apply") unless pg_boot.match?(/\\if\s+:do_apply\b/)
fail_collect(failures, "postgres gate must \\elif :do_exact") unless pg_boot.match?(/\\elif\s+:do_exact\b/)
fail_collect(failures, "postgres gate must \\elif :do_fail") unless pg_boot.match?(/\\elif\s+:do_fail\b/)

lock_idx = pg_boot.index("pg_advisory_xact_lock")
ensure_idx = pg_boot.index("postgres_ensure_roles.sql")
grant_idx = pg_boot.index("postgres_grant_runtime.sql")
apply_idx = pg_boot.index("\\if :do_apply") || pg_boot.index("\\if :do_apply")
fail_collect(failures, "postgres advisory lock must appear before role ensure") unless lock_idx && ensure_idx && lock_idx < ensure_idx
fail_collect(failures, "postgres role ensure must only run on apply branch") unless apply_idx && ensure_idx && apply_idx < ensure_idx
fail_collect(failures, "postgres runtime grants must remain inside the gated apply transaction") unless grant_idx && apply_idx && apply_idx < grant_idx && ensure_idx < grant_idx

# Reject separate-session race: no admin role mutation before the single-transaction gate file.
pre_gate = pg_boot.split("--single-transaction").first
if pre_gate&.include?("postgres_ensure_roles.sql") || pre_gate&.match?(/CREATE ROLE|ALTER ROLE/)
  fail_collect(failures, "postgres must not mutate roles before the single-transaction gate (separate-session race)")
end
fail_collect(failures, "postgres must not use a separate bootstrap login for the gated transaction") if pg_boot.match?(/psql_bootstrap\s/)

# Exact-one-row: reject extra schema_migrations / schema_bootstrap_meta acceptance.
fail_collect(failures, "postgres exact gate must count schema_migrations rows") unless pg_boot.include?("INTO version_count FROM schema_migrations") || pg_boot.match?(/count\(\*\) INTO version_count FROM schema_migrations/)
fail_collect(failures, "postgres exact gate must count schema_bootstrap_meta rows (reject extras)") unless pg_boot.include?("INTO meta_count FROM schema_bootstrap_meta") || pg_boot.match?(/count\(\*\) INTO meta_count FROM schema_bootstrap_meta/)
fail_collect(failures, "postgres exact gate must require meta_count = 1") unless pg_boot.include?("meta_count <> 1") || pg_boot.include?("meta_count != 1")
fail_collect(failures, "postgres must not accept extra meta via version-filtered LIMIT 1") if pg_boot.match?(/schema_bootstrap_meta WHERE version.*LIMIT 1/m)

# Full index set equality (reject unexpected as well as missing non-constraint indexes).
fail_collect(failures, "postgres exact must compare full index set equality") unless pg_boot.include?("index_names IS NOT DISTINCT FROM expected_indexes") || pg_boot.include?("index_names IS DISTINCT FROM expected_indexes")
fail_collect(failures, "postgres must not only FOREACH missing expected indexes") if pg_boot.match?(/FOREACH missing_index IN ARRAY expected_indexes/) && !pg_boot.include?("index_names IS")

# Ownership of every public migrated/bootstrap object by DDL role (not only schema_migrations).
# Must cover ordinary/partitioned tables, views, matviews, sequences, ordinary/partitioned indexes — on exact AND post-apply.
fail_collect(failures, "postgres must verify ownership of all public relations (r,p,v,m,S,i,I)") unless pg_boot.match?(/relkind\s+IN\s*\(\s*'r'\s*,\s*'p'\s*,\s*'v'\s*,\s*'m'\s*,\s*'S'\s*,\s*'i'\s*,\s*'I'\s*\)/)
fail_collect(failures, "postgres must not only check schema_migrations ownership") if pg_boot.include?("relname = 'schema_migrations'") && !pg_boot.match?(/relkind\s+IN/)
fail_collect(failures, "postgres index catalog must include partitioned relkind I") unless pg_boot.match?(/relkind\s+IN\s*\(\s*'i'\s*,\s*'I'\s*\)/)
gate_do = pg_boot[/\bDO \\\$\\\$.*?INSERT INTO _bootstrap_gate/m]
if gate_do
  noop_pos = gate_do.index("action := 'noop'")
  owner_pos = gate_do.index("relkind IN ('r', 'p', 'v', 'm', 'S', 'i', 'I')")
  fail_collect(failures, "postgres exact/no-op must validate ownership before accepting noop") unless owner_pos && noop_pos && owner_pos < noop_pos
  %w[
    table_names IS NOT DISTINCT FROM expected_tables
    view_names IS NOT DISTINCT FROM expected_views
    matview_names IS NOT DISTINCT FROM expected_matviews
    sequence_names IS NOT DISTINCT FROM expected_sequences
    index_names IS NOT DISTINCT FROM expected_indexes
  ].each do |eq|
    fail_collect(failures, "postgres exact/no-op gate must itself contain #{eq}") unless gate_do.include?(eq)
  end
  fail_collect(failures, "postgres exact/no-op gate must OID-exclude constraint indexes") unless gate_do.include?("conindid")
  fail_collect(failures, "postgres exact/no-op gate must include partitioned indexes I") unless gate_do.match?(/relkind\s+IN\s*\(\s*'i'\s*,\s*'I'\s*\)/)
  fail_collect(failures, "postgres empty/drift must count index_names") unless gate_do.include?("array_length(index_names")
else
  fail_collect(failures, "postgres gate DO block before _bootstrap_gate missing")
end

# Index equality excludes constraint-backed indexes by OID (pg_constraint.conindid), never name suffix.
fail_collect(failures, "postgres must exclude constraint-backed indexes via pg_constraint.conindid") unless pg_boot.include?("pg_constraint") && pg_boot.include?("conindid")
fail_collect(failures, "postgres must not exclude indexes by _pkey|_key name suffix") if pg_boot.match?(/indexname\s+!~/) || pg_boot.include?("_(pkey|key)$")

# Materialized views in empty/drift/exact/fingerprint path.
fail_collect(failures, "postgres must query pg_matviews") unless pg_boot.include?("pg_matviews")
fail_collect(failures, "postgres must compare matview sets") unless pg_boot.include?("matview_names")
fail_collect(failures, "postgres must load EXPECTED_MATERIALIZED_VIEWS") unless pg_boot.include?("EXPECTED_MATERIALIZED_VIEWS")
puts "ok postgres-atomic-gate"

# --- Exact-state / fingerprint strategy ---
ch_boot = File.read(File.join(BOOTSTRAP, "bin/bootstrap-clickhouse.sh"))
fail_collect(failures, "clickhouse must gate before user creation") unless ch_boot.index("decide=") && ch_boot.index("CREATE USER") && ch_boot.index("decide=") < ch_boot.index("CREATE USER")
fail_collect(failures, "clickhouse must document non-transactional DDL") unless ch_boot.include?("cross-DDL") || ch_boot.include?("NOT cross-DDL") || ch_boot.include?("nontransactional")
fail_collect(failures, "clickhouse must post-apply verify") unless ch_boot.include?("post-apply")
fail_collect(failures, "clickhouse exact noop must exit without mutations") unless ch_boot.match?(/decide="noop"[\s\S]*exit 0/)
fail_collect(failures, "clickhouse exact gate must count schema_bootstrap_meta rows") unless ch_boot.include?("schema_bootstrap_meta FINAL") && ch_boot.include?("meta_count")
fail_collect(failures, "clickhouse exact/noop must require marker meta_count == 1") unless ch_boot.include?('"$meta_count" == "1"')
fail_collect(failures, "clickhouse must not accept extra meta via version-filtered LIMIT 1") if ch_boot.match?(/schema_bootstrap_meta FINAL WHERE version.*LIMIT 1/m)

# Fail closed: no error suppression on catalog reads or grant cleanup.
ch_suppresses_errors = ch_boot.lines.any? { |l| !l.lstrip.start_with?("#") && l.match?(/\|\|\s*true/) }
fail_collect(failures, "clickhouse must not suppress catalog/grant errors with || true") if ch_suppresses_errors

# Catalog reads must capture via assignment (not mapfile+process-substitution).
if ch_boot.match?(/mapfile\s+-t\s+\w+\s+<\s+<\(list_analytics_/)
  fail_collect(failures, "clickhouse must not use mapfile+process-substitution for catalog lists (loses exit status)")
end
fail_collect(failures, "clickhouse must assign list_analytics_tables output under set -e") unless ch_boot.match?(/live_tables_raw="\$\(list_analytics_tables\)"\s*\|\|\s*exit/)
fail_collect(failures, "clickhouse must assign list_analytics_views output under set -e") unless ch_boot.match?(/live_views_raw="\$\(list_analytics_views\)"\s*\|\|\s*exit/)
fail_collect(failures, "clickhouse must enable inherit_errexit for catalog captures") unless ch_boot.include?("inherit_errexit")
fail_collect(failures, "clickhouse must retain set -euo pipefail") unless ch_boot.match?(/^set -euo pipefail$/)
fail_collect(failures, "postgres bootstrap must retain standalone set -euo pipefail") unless pg_boot.match?(/^set -euo pipefail$/)
fail_collect(failures, "postgres bootstrap must not append set options onto slots comment") if pg_boot.include?("slots.set")

# Default-DB sentinel-first mutation; preflight rejects existing sentinel; drop before final marker.
fail_collect(failures, "clickhouse must define bootstrap-in-progress sentinel") unless ch_boot.include?("_bootstrap_in_progress")
sentinel_create = ch_boot.index("CREATE TABLE IF NOT EXISTS default.${SENTINEL_TABLE}")
create_db = ch_boot.index("CREATE DATABASE IF NOT EXISTS analytics")
create_user = ch_boot.index("CREATE USER IF NOT EXISTS")
fail_collect(failures, "clickhouse first mutation must be default-DB sentinel") unless sentinel_create
fail_collect(failures, "clickhouse default sentinel must precede CREATE DATABASE analytics") unless sentinel_create && create_db && sentinel_create < create_db
fail_collect(failures, "clickhouse sentinel must be first mutation before CREATE USER") unless sentinel_create && create_user && sentinel_create < create_user
fail_collect(failures, "clickhouse must not place sentinel in analytics DB") if ch_boot.match?(/CREATE TABLE IF NOT EXISTS analytics\.\$\{SENTINEL_TABLE\}/) || ch_boot.include?("CREATE TABLE IF NOT EXISTS analytics._bootstrap_in_progress")
fail_collect(failures, "clickhouse preflight must reject default in-progress sentinel") unless ch_boot.include?("default bootstrap-in-progress sentinel present")
fail_collect(failures, "clickhouse must DROP default sentinel before completion marker") unless ch_boot.include?("DROP TABLE IF EXISTS default.${SENTINEL_TABLE}")

# Completion marker must be the final mutation after table+view verify, runtime grants, and sentinel drop.
marker_insert = ch_boot.index("INSERT INTO analytics.schema_bootstrap_meta")
runtime_grant = ch_boot.index("GRANT SELECT, INSERT ON analytics")
drop_sentinel = ch_boot.index("DROP TABLE IF EXISTS default.${SENTINEL_TABLE}")
# Migration apply: one HTTP POST per split statement (not full-file @MIGRATION_FILE).
migration_apply = ch_boot.index("split-clickhouse-sql") ||
                  ch_boot.index('--data-binary @"${MIGRATION_FILE}"') ||
                  ch_boot.index("MIGRATION_FILE}")
fail_collect(failures, "clickhouse must split migration via split-clickhouse-sql") unless ch_boot.include?("split-clickhouse-sql")
fail_collect(failures, "clickhouse must not POST full migration via --data-binary @MIGRATION_FILE") if ch_boot.match?(/--data-binary\s+@"\$\{MIGRATION_FILE\}"/)
if ch_boot.lines.any? { |l| !l.lstrip.start_with?("#") && l.include?("multiquery=1") }
  fail_collect(failures, "clickhouse must not use unsupported multiquery=1")
end
ch_boot_active = ch_boot.lines.reject { |l| l.lstrip.start_with?("#") }.join
if ch_boot_active.match?(/ch_query_bootstrap\s+"\$\(cat\b/)
  fail_collect(failures, "clickhouse must not nest $(cat …) inside ch_query_bootstrap/curl argument")
end
unless ch_boot_active.match?(/stmt="\$\(cat\s+"\$\{stmt_file\}"\)"\s*\|\|\s*/)
  fail_collect(failures, "clickhouse must read each split statement via guarded assignment || exit")
end
unless ch_boot_active.match?(/\[\[\s+-z\s+"\$\{stmt\}"\s*\]\]/)
  fail_collect(failures, "clickhouse must reject empty statement after guarded read")
end
unless ch_boot_active.match?(/ch_query_bootstrap\s+"\$\{stmt\}"/)
  fail_collect(failures, "clickhouse must pass stmt variable to ch_query_bootstrap")
end
split_lib = File.join(BOOTSTRAP, "lib/clickhouse_sql_split.rb")
split_bin = File.join(BOOTSTRAP, "bin/split-clickhouse-sql.rb")
fail_collect(failures, "missing clickhouse_sql_split.rb") unless File.file?(split_lib)
fail_collect(failures, "missing split-clickhouse-sql.rb") unless File.file?(split_bin)
if marker_insert && runtime_grant
  fail_collect(failures, "clickhouse completion marker must come after runtime grants") unless marker_insert > runtime_grant
end
if marker_insert && drop_sentinel
  fail_collect(failures, "clickhouse completion marker must come after DROP sentinel") unless marker_insert > drop_sentinel
end
if drop_sentinel && runtime_grant
  fail_collect(failures, "clickhouse DROP sentinel must come after runtime grants") unless drop_sentinel > runtime_grant
end
if sentinel_create && create_db && drop_sentinel && marker_insert
  fail_collect(failures, "clickhouse apply order: default sentinel < CREATE DATABASE < DROP sentinel < marker") unless sentinel_create < create_db && create_db < drop_sentinel && drop_sentinel < marker_insert
end
if marker_insert && migration_apply
  post_apply = ch_boot[migration_apply...marker_insert]
  unless post_apply&.include?("live_views_sorted") &&
         (post_apply.include?('"$live_views_sorted" != "$expected_views_sorted"') ||
          post_apply.include?('"$live_views_sorted" == "$expected_views_sorted"'))
    fail_collect(failures, "clickhouse post-apply must compare exact views before final marker")
  end
else
  fail_collect(failures, "clickhouse post-apply must compare exact views before final marker")
end
fail_collect(failures, "clickhouse must document nontransactional recovery / kind-reset") unless ch_boot.include?("kind-reset") && (ch_boot.include?("nontransactional") || ch_boot.include?("non-transactional") || ch_boot.include?("NOT cross-DDL") || ch_boot.include?("cross-DDL"))

fp_lib = File.read(File.join(BOOTSTRAP, "lib/fingerprint.rb"))
fail_collect(failures, "fingerprint lib must document strategy") unless fp_lib.include?("checksum") && fp_lib.include?("schema_bootstrap_meta")
fail_collect(failures, "fingerprint lib must parse materialized views") unless fp_lib.include?("MATERIALIZED") && fp_lib.include?("materialized_views")
fail_collect(failures, "fingerprint lib must document OID constraint-index exclusion") unless fp_lib.include?("pg_constraint") || fp_lib.include?("conindid")
puts "ok exact-state-fingerprint"

# --- Kafka RF1 / local partition policy + AsyncAPI exact set ---
plan_path = File.join(GENERATED, "kafka-topic-plan.yaml")
script_path = File.join(GENERATED, "kafka-create-topics.sh")
fail_collect(failures, "missing generated kafka-topic-plan.yaml; run make kind-render") unless File.file?(plan_path)
if File.file?(plan_path)
  plan = YAML.load_file(plan_path)
  spec = plan.fetch("spec")
  fail_collect(failures, "kafka plan RF != 1") unless spec["replicationFactor"] == 1
  fail_collect(failures, "kafka plan minISR != 1") unless spec["minInSyncReplicas"] == 1
  fail_collect(failures, "kafka high partitions != 2") unless spec.dig("partitionPolicy", "high") == 2
  fail_collect(failures, "kafka business partitions != 1") unless spec.dig("partitionPolicy", "business") == 1

  async = YAML.load_file(ASYNCAPI)
  channels = async.fetch("channels").keys.sort
  high = async.fetch("x-partitionPlanning").fetch("highVolumeTopics").sort
  rebuild_requests = %w[
    analytics.projection.rebuild_requested
    spectator.projection.rebuild_requested
  ].freeze
  domain = spec.fetch("domainTopics")
  domain_names = domain.map { |t| t["name"] }.sort
  fail_collect(failures, "domain topic set != AsyncAPI channels") unless domain_names == channels
  high.each do |name|
    t = domain.find { |x| x["name"] == name }
    fail_collect(failures, "high-volume topic #{name} missing or wrong class") unless t && t["class"] == "high" && t["partitions"] == 2
  end
  domain.each do |t|
    fail_collect(failures, "topic #{t['name']} RF != 1") unless t["replicationFactor"] == 1
    expected = if rebuild_requests.include?(t["name"])
                 2
               elsif t["class"] == "high"
                 2
               else
                 1
               end
    fail_collect(failures, "topic #{t['name']} partitions=#{t['partitions']} expected=#{expected}") unless t["partitions"] == expected
    fail_collect(failures, "topic #{t['name']} missing retentionMs") if t["retentionMs"].nil? || t["retentionMs"].to_i <= 0
  end
  dlq = spec.fetch("dlqTopics")
  fail_collect(failures, "dlqTopics must be non-empty scaffolding") if dlq.empty?
  dlq.each do |t|
    fail_collect(failures, "DLQ #{t['name']} must end with .dlq") unless t["name"].end_with?(".dlq")
    fail_collect(failures, "DLQ #{t['name']} RF != 1") unless t["replicationFactor"] == 1
    fail_collect(failures, "DLQ #{t['name']} missing retentionMs") if t["retentionMs"].nil? || t["retentionMs"].to_i <= 0
    fail_collect(failures, "DLQ #{t['name']} retentionClass") unless t["retentionClass"] == "dlq"
  end
  fail_collect(failures, "missing ranking DLQ scaffolding") unless dlq.any? { |t| t["name"] == "room.game.completed.ranking.dlq" }
  fail_collect(failures, "missing ranking players.advanced DLQ") unless dlq.any? { |t| t["name"] == "tournament.players.advanced.ranking.dlq" }
  fail_collect(failures, "missing ranking tournament.completed DLQ") unless dlq.any? { |t| t["name"] == "tournament.completed.ranking.dlq" }
  connect = spec.fetch("connectInternalTopics")
  %w[connect-configs connect-offsets connect-status].each do |name|
    fail_collect(failures, "missing connect topic #{name}") unless connect.any? { |t| t["name"] == name && t["partitions"] == 1 && t["replicationFactor"] == 1 && t["cleanupPolicy"] == "compact" }
  end

  if File.file?(script_path)
    script = File.read(script_path)
    (domain_names + dlq.map { |t| t["name"] } + %w[connect-configs connect-offsets connect-status]).each do |name|
      fail_collect(failures, "topic script missing #{name}") unless script.include?(name)
    end
    fail_collect(failures, "topic script must assert partition drift") unless script.include?("PartitionCount")
    fail_collect(failures, "topic script must assert RF drift") unless script.include?("ReplicationFactor")
    fail_collect(failures, "topic script must assert cleanup.policy drift") unless script.include?("cleanup.policy")
    fail_collect(failures, "topic script must assert min.insync.replicas drift") unless script.include?("min.insync.replicas")
    fail_collect(failures, "topic script must assert retention.ms drift") unless script.include?("retention.ms")
    fail_collect(failures, "topic script must fail closed on empty cleanup.policy") unless script.include?('-z "$cleanup_got"') && script.include?("cleanup.policy missing")
    fail_collect(failures, "topic script must fail closed on empty min.insync.replicas") unless script.include?('-z "$min_isr_got"') && script.include?("min.insync.replicas missing")
    # Reject fail-open pattern that only checks mismatch when nonempty.
    if script.match?(/\[\[\s+-n\s+"\$cleanup_got"\s+&&/) || script.match?(/\[\[\s+-n\s+"\$min_isr_got"\s+&&/)
      fail_collect(failures, "topic script must not accept empty Kafka config values (fail-open -n pattern)")
    end
    fail_collect(failures, "topic script must harden apache/kafka describe/config parsing") unless script.include?("kafka_config_value") && script.include?("kafka-configs.sh")
    render_src = File.read(File.join(KIND, "scripts/render-kafka-topics.rb"))
    fail_collect(failures, "render-kafka-topics must emit nonempty cleanup/minISR assertions") unless render_src.include?('-z "$cleanup_got"') && render_src.include?('-z "$min_isr_got"')
    fail_collect(failures, "render-kafka-topics must emit retention.ms for domain/DLQ") unless render_src.include?("KIND_RETENTION_MS")
  end

  kafka_dep = File.read(File.join(MANIFESTS, "30-kafka/kafka.yaml"))
  %w[
    KAFKA_NODE_ID
    KAFKA_PROCESS_ROLES
    KAFKA_LISTENERS
    KAFKA_ADVERTISED_LISTENERS
    KAFKA_CONTROLLER_LISTENER_NAMES
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP
    KAFKA_CONTROLLER_QUORUM_VOTERS
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR
    CLUSTER_ID
  ].each do |k|
    fail_collect(failures, "kafka missing KRaft env #{k}") unless kafka_dep.include?(k)
  end
  fail_collect(failures, "kafka controller quorum should use loopback self-discovery") unless kafka_dep.include?("1@127.0.0.1:9093")
  fail_collect(failures, "kafka deployment missing non-HA label") unless kafka_dep.include?('uno-arena.local/kafka-ha: "false"')

  # Heavy kafka-topics.sh exec probes can hang while the broker is already listening,
  # leaving the Service with no ready endpoints (advertised DNS works; readiness does not).
  kafka_deployment = load_all_yaml(File.join(MANIFESTS, "30-kafka/kafka.yaml")).find do |doc|
    doc.is_a?(Hash) && doc["kind"] == "Deployment"
  end
  kafka_container = kafka_deployment&.dig("spec", "template", "spec", "containers", 0) || {}
  %w[readinessProbe livenessProbe].each do |probe_name|
    probe = kafka_container[probe_name] || {}
    if probe.key?("exec") || probe.to_s.include?("kafka-topics")
      fail_collect(
        failures,
        "kafka #{probe_name} must not use kafka-topics.sh exec " \
        "(use lightweight tcpSocket:9092; topic drift stays in bootstrap Job)"
      )
    end
    port = probe.dig("tcpSocket", "port")
    unless port == 9092
      fail_collect(failures, "kafka #{probe_name} must use tcpSocket port 9092, got #{port.inspect}")
    end
  end
  readiness = kafka_container["readinessProbe"] || {}
  liveness = kafka_container["livenessProbe"] || {}
  unless (readiness["initialDelaySeconds"] || 0) >= 10
    fail_collect(failures, "kafka readinessProbe must retain a reasonable initialDelaySeconds (>= 10)")
  end
  unless (liveness["initialDelaySeconds"] || 0) >= 20
    fail_collect(failures, "kafka livenessProbe must retain a reasonable initialDelaySeconds (>= 20)")
  end
end
puts "ok kafka-local-policy"

# --- emptyDir / disposable policy ---
each_manifest(MANIFESTS) do |path, doc|
  next unless doc.is_a?(Hash) && doc["kind"] == "Deployment"
  # Application datastores remain disposable emptyDir. The sole reviewed local
  # persistence exception is MinIO's observability object store, whose 5 GiB PVC
  # survives pod replacement but is deleted with the kind cluster.
  minio_observability = path.end_with?("/90-observability-storage/minio.yaml") && doc.dig("metadata", "name") == "minio"
  vols = doc.dig("spec", "template", "spec", "volumes") || []
  vols.each do |v|
    if v.key?("persistentVolumeClaim") && !(minio_observability && v["name"] == "data" && v.dig("persistentVolumeClaim", "claimName") == "minio-data")
      fail_collect(failures, "PVC not allowed in #{path} volume #{v['name']}")
    end
    fail_collect(failures, "hostPath not allowed in #{path} volume #{v['name']}") if v.key?("hostPath")
  end
  vols.each do |v|
    next unless %w[data].include?(v["name"])
    fail_collect(failures, "data volume must be emptyDir in #{path}") unless v.key?("emptyDir") || minio_observability
  end
end
puts "ok emptydir-policy"

# --- No LoadBalancer / public backend exposure ---
[MANIFESTS, GENERATED].each do |dir|
  next unless File.directory?(dir)
  each_manifest(dir) do |path, doc|
    next unless doc.is_a?(Hash) && doc["kind"] == "Service"
    type = doc.dig("spec", "type") || "ClusterIP"
    name = doc.dig("metadata", "name")
    fail_collect(failures, "Service #{name} type=#{type} in #{path}; expected ClusterIP") unless type == "ClusterIP"
  end
end
each_manifest(MANIFESTS) do |path, doc|
  next unless doc.is_a?(Hash) && doc["kind"] == "Deployment"
  containers = doc.dig("spec", "template", "spec", "containers") || []
  containers.each do |c|
    (c["ports"] || []).each do |p|
      fail_collect(failures, "hostPort on #{doc.dig('metadata', 'name')} in #{path}") if p.key?("hostPort")
    end
  end
end
puts "ok no-public-exposure"

# --- Namespace + Keycloak realm import ---
ns_ok = false
load_all_yaml(File.join(MANIFESTS, "00-namespace.yaml")).each do |doc|
  ns_ok = true if doc.is_a?(Hash) && doc["kind"] == "Namespace" && doc.dig("metadata", "name") == "uno-arena"
end
fail_collect(failures, "missing uno-arena namespace") unless ns_ok

kc = File.read(File.join(MANIFESTS, "60-keycloak/keycloak.yaml"))
fail_collect(failures, "keycloak missing realm import mount") unless kc.include?("keycloak-realm") || kc.include?("unoarena-realm")
fail_collect(failures, "keycloak missing start-dev") unless kc.include?("start-dev")
fail_collect(failures, "keycloak realm missing unoarena") unless kc.include?('"realm": "unoarena"')
puts "ok namespace-keycloak"

# --- Bootstrap image embeds migrations; dockerignore must not exclude them ---
dockerfile = File.read(File.join(BOOTSTRAP, "Dockerfile"))
MIGRATION_COPY_PATHS.each do |rel|
  fail_collect(failures, "bootstrap Dockerfile missing COPY #{rel}") unless dockerfile.include?(rel)
  fail_collect(failures, "migration source missing #{rel}") unless File.file?(File.join(ROOT, rel))
end
di = File.read(DOCKERIGNORE)
if di.match?(%r{^\*\*/migrations/}) && !di.include?("!services/identity/migrations/")
  fail_collect(failures, ".dockerignore excludes migrations without service exceptions")
end
MIGRATION_COPY_PATHS.each do |rel|
  ctx = rel.split("/")[1]
  fail_collect(failures, ".dockerignore missing negation for #{ctx} migrations") unless di.include?("!services/#{ctx}/migrations/")
end
puts "ok bootstrap-image-source"

# --- apply ordering / waits + literal reset pin ---
apply = File.read(File.join(KIND, "scripts/apply.sh"))
fail_collect(failures, "apply.sh must wait for datastore rollouts before bootstrap Jobs") unless apply.index("70-bootstrap") && apply.index("rollout status") && apply.index("rollout status") < apply.index("70-bootstrap")
fail_collect(failures, "apply.sh must wait for Job completion") unless apply.include?("wait --for=condition=complete")
fail_collect(failures, "apply.sh must apply Debezium Server after bootstrap") unless apply.include?("80-debezium-server")
fail_collect(failures, "apply.sh must wait for debezium-server-room-realtime rollout") unless apply.include?("debezium-server-room-realtime")
fail_collect(failures, "apply.sh must rollout restart debezium-server-room-realtime after apply") unless apply.include?('rollout restart "deployment/debezium-server-room-realtime"')
server_apply_i = apply.index('kubectl apply -f "${MANIFESTS_DIR}/80-debezium-server"')
server_restart_i = apply.index('rollout restart "deployment/debezium-server-room-realtime"')
server_wait_i = apply.index('rollout status "deployment/debezium-server-room-realtime"')
fail_collect(failures, "apply.sh must order Server apply → restart → wait") unless server_apply_i && server_restart_i && server_wait_i && server_apply_i < server_restart_i && server_restart_i < server_wait_i
fail_collect(failures, "apply.sh must apply Debezium Connect after bootstrap") unless apply.match?(%r{80-debezium"})
fail_collect(failures, "apply.sh must wait for debezium-connect rollout") unless apply.include?("debezium-connect")
fail_collect(failures, "apply.sh must wait for register-debezium-connectors") unless apply.include?("register-debezium-connectors")
fail_collect(failures, "apply.sh must delete register-debezium-connectors before recreate") unless apply.include?("delete job/register-debezium-connectors") && apply.include?("--ignore-not-found")
wait_src = File.read(File.join(KIND, "scripts/wait.sh"))
fail_collect(failures, "wait.sh must include debezium-server-room-realtime") unless wait_src.include?("debezium-server-room-realtime")
fail_collect(failures, "wait.sh must include debezium-connect") unless wait_src.include?("debezium-connect")
fail_collect(failures, "wait.sh must include register-debezium-connectors") unless wait_src.include?("register-debezium-connectors")

# kubectl apply -f <dir> loads every file; realm/auxiliary JSON is not a Kubernetes object.
# ConfigMap already embeds the realm — apply only keycloak.yaml (or any dir that is YAML-only).
def kubernetes_object_json?(path)
  data = JSON.parse(File.read(path))
  data.is_a?(Hash) && data.key?("apiVersion") && data.key?("kind")
rescue StandardError
  false
end

def apply_dir_with_non_k8s_json?(apply_src, manifests_dir)
  apply_src.scan(%r{kubectl apply -f "\$\{MANIFESTS_DIR\}/([^"]+)"}).flatten.any? do |rel|
    dir = File.join(manifests_dir, rel)
    next false unless File.directory?(dir)

    Dir.children(dir).any? do |name|
      path = File.join(dir, name)
      File.file?(path) && name.end_with?(".json") && !kubernetes_object_json?(path)
    end
  end
end

fail_collect(
  failures,
  "apply.sh must not kubectl-apply a manifests directory that contains non-Kubernetes JSON " \
  "(e.g. realm-unoarena.json); apply explicit keycloak.yaml instead"
) if apply_dir_with_non_k8s_json?(apply, MANIFESTS)

reset = File.read(File.join(KIND, "scripts/reset.sh"))
fail_collect(failures, "reset.sh must reject KIND_CLUSTER_NAME overrides") unless reset.include?("rejects KIND_CLUSTER_NAME")
fail_collect(failures, "reset.sh must pin literal uno-arena") unless reset.include?('kind delete cluster --name "uno-arena"')
fail_collect(failures, "reset.sh must pin kind-uno-arena context") unless reset.include?("kind-uno-arena")
puts "ok apply-reset-guards"

# --- Credential placeholders obvious / not broadly shared ---
secrets = File.read(File.join(MANIFESTS, "01-local-secrets.yaml"))
%w[identity-admin-password room-admin-password tournament-admin-password ranking-admin-password].each do |k|
  fail_collect(failures, "missing context admin secret #{k}") unless secrets.include?(k)
end
%w[
  identity-cdc-user identity-cdc-password
  room-cdc-kafka-user room-cdc-kafka-password
  room-cdc-realtime-user room-cdc-realtime-password
  tournament-cdc-user tournament-cdc-password
  ranking-cdc-user ranking-cdc-password
].each do |k|
  fail_collect(failures, "missing CDC secret #{k}") unless secrets.include?(k)
end
fail_collect(failures, "shared postgres-admin-password must not remain") if secrets.include?("postgres-admin-password")
fail_collect(failures, "credentials must be marked local-only") unless secrets.include?("local-only")
puts "ok credentials-scoping"

# --- Regression fixtures: predicates that must reject known-bad Slice 0 shapes ---
def fixture_rejects?(predicate, bad_sample)
  predicate.call(bad_sample)
end

role_before_gate = lambda do |src|
  lock = src.index("pg_advisory_xact_lock")
  ensure_roles = src.index("postgres_ensure_roles.sql")
  return true if lock.nil? || ensure_roles.nil?
  ensure_roles < lock || src.split("--single-transaction").first.include?("postgres_ensure_roles.sql")
end
fail_collect(failures, "fixture: role-mutation-before-gate detector inert") unless fixture_rejects?(
  role_before_gate,
  <<~BAD
    psql_admin -f /bootstrap/sql/postgres_ensure_roles.sql
    SELECT pg_advisory_xact_lock(1);
    psql_bootstrap --single-transaction -f "$final"
  BAD
)

separate_session = lambda do |src|
  src.match?(/psql_bootstrap\s/) || (
    src.include?("postgres_ensure_roles.sql") &&
      src.split("--single-transaction").first.include?("postgres_ensure_roles.sql")
  )
end
fail_collect(failures, "fixture: separate-session-race detector inert") unless fixture_rejects?(
  separate_session,
  <<~BAD
    psql_admin -f postgres_ensure_roles.sql
    psql_bootstrap --single-transaction -f gate.sql
  BAD
)

extra_meta_ok = lambda do |src|
  src.include?("schema_bootstrap_meta") &&
    !src.include?("meta_count") &&
    src.match?(/WHERE version = .*LIMIT 1/m)
end
fail_collect(failures, "fixture: extra-metadata-acceptance detector inert") unless fixture_rejects?(
  extra_meta_ok,
  <<~BAD
    SELECT checksum INTO found_checksum FROM schema_bootstrap_meta WHERE version = 'v1' LIMIT 1;
    IF version_count = 1 AND found_checksum IS NOT DISTINCT FROM 'abc' THEN
      action := 'noop';
    END IF;
  BAD
)

empty_kafka_ok = lambda do |src|
  src.match?(/\[\[\s+-n\s+"\$cleanup_got"\s+&&/) || src.match?(/\[\[\s+-n\s+"\$min_isr_got"\s+&&/)
end
fail_collect(failures, "fixture: empty-kafka-config-acceptance detector inert") unless fixture_rejects?(
  empty_kafka_ok,
  <<~BAD
    if [[ -n "$cleanup_got" && "$cleanup_got" != "$cleanup" ]]; then exit 1; fi
    if [[ -n "$min_isr_got" && "$min_isr_got" != "$min_isr" ]]; then exit 1; fi
  BAD
)

# Invalid psql branch syntax: expression-style \\if (not boolean \\gset).
expression_if = lambda { |src| src.match?(/\\if\s+:'[^']+'\s*=/) }
fail_collect(failures, "fixture: expression-style \\if detector inert") unless fixture_rejects?(
  expression_if,
  <<~BAD
    SELECT action AS bootstrap_action FROM _bootstrap_gate \\gset
    \\if :'bootstrap_action' = 'apply'
    \\echo apply
    \\endif
  BAD
)

# ClickHouse catalog/grant error suppression (ignore comment-only mentions).
ch_error_suppress = lambda do |src|
  src.lines.any? { |l| !l.lstrip.start_with?("#") && l.match?(/\|\|\s*true/) }
end
fail_collect(failures, "fixture: clickhouse || true suppression detector inert") unless fixture_rejects?(
  ch_error_suppress,
  <<~BAD
    list_analytics_tables() {
      ch_query_admin "SELECT name FROM system.tables" | sed '/^$/d' || true
    }
    ch_query_admin "REVOKE DROP ON *.* FROM runtime" || true
  BAD
)

# Early completion marker before grants / view verify.
early_marker = lambda do |src|
  insert = src.index("INSERT INTO analytics.schema_bootstrap_meta")
  grant = src.index("GRANT SELECT, INSERT ON analytics")
  return false if insert.nil? || grant.nil?
  insert < grant
end
fail_collect(failures, "fixture: early completion marker detector inert") unless fixture_rejects?(
  early_marker,
  <<~BAD
    INSERT INTO analytics.schema_bootstrap_meta (version, checksum) VALUES ('v', 'c')
    # post-apply
    live_views_sorted=x
    if [[ "$live_views_sorted" != "$expected_views_sorted" ]]; then exit 1; fi
    GRANT SELECT, INSERT ON analytics.* TO runtime
  BAD
)

# Missing post-apply view comparison before marker (after migration apply).
missing_view_compare = lambda do |src|
  insert = src.index("INSERT INTO analytics.schema_bootstrap_meta")
  mig = src.index("split-clickhouse-sql") ||
        src.index('--data-binary @"${MIGRATION_FILE}"') ||
        src.index("MIGRATION_FILE}")
  return false if insert.nil? || mig.nil? || mig > insert
  between = src[mig...insert]
  !between.include?("live_views_sorted") || (
    !between.include?('"$live_views_sorted" != "$expected_views_sorted"') &&
      !between.include?('"$live_views_sorted" == "$expected_views_sorted"')
  )
end
fail_collect(failures, "fixture: missing view comparison detector inert") unless fixture_rejects?(
  missing_view_compare,
  <<~BAD
    split-clickhouse-sql.rb "${MIGRATION_FILE}" "${stmt_dir}"
    # post-apply tables only
    if [[ "$live_sorted" != "$expected_sorted" ]]; then exit 1; fi
    GRANT SELECT, INSERT ON analytics.* TO runtime
    INSERT INTO analytics.schema_bootstrap_meta (version, checksum) VALUES ('v', 'c')
  BAD
)
# Unexpected-index acceptance: FOREACH expected only, no full set equality.
unexpected_index_ok = lambda do |src|
  src.match?(/FOREACH missing_index IN ARRAY expected_indexes/) &&
    !src.include?("index_names IS NOT DISTINCT FROM expected_indexes") &&
    !src.include?("index_names IS DISTINCT FROM expected_indexes")
end
fail_collect(failures, "fixture: unexpected-index acceptance detector inert") unless fixture_rejects?(
  unexpected_index_ok,
  <<~BAD
    FOREACH missing_index IN ARRAY expected_indexes LOOP
      IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = missing_index) THEN
        RAISE EXCEPTION 'missing';
      END IF;
    END LOOP;
  BAD
)

# Partial ownership: schema_migrations only.
partial_ownership = lambda do |src|
  src.include?("relname = 'schema_migrations'") && !src.match?(/relkind\s+IN/)
end
fail_collect(failures, "fixture: partial ownership detector inert") unless fixture_rejects?(
  partial_ownership,
  <<~BAD
    SELECT pg_get_userbyid(c.relowner) INTO ddl_owner
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public' AND c.relname = 'schema_migrations' AND c.relkind = 'r';
  BAD
)

# Exact without ownership: catalog match alone accepts noop.
exact_without_ownership = lambda do |src|
  gate = src[/\bDO \\\$\\\$.*?INSERT INTO _bootstrap_gate/m] || src[/\bDO \$\$.*?INSERT INTO _bootstrap_gate/m] || src
  noop = gate.index("action := 'noop'")
  return false if noop.nil?
  before = gate[0...noop]
  !before.match?(/relkind\s+IN\s*\([^)]*'p'[^)]*'m'/) && !before.include?("ownership")
end
fail_collect(failures, "fixture: exact-without-ownership detector inert") unless fixture_rejects?(
  exact_without_ownership,
  <<~BAD
    DO $$
    BEGIN
      IF table_names IS NOT DISTINCT FROM expected_tables THEN
        action := 'noop';
      END IF;
      INSERT INTO _bootstrap_gate(action) VALUES (action);
    END
    $$;
  BAD
)

# Exact equality only in post-apply (gate accepts noop without full catalog).
exact_equality_only_post_apply = lambda do |src|
  gate = src[/\bDO \\\$\\\$.*?INSERT INTO _bootstrap_gate/m] || src[/\bDO \$\$.*?INSERT INTO _bootstrap_gate/m]
  return true if gate.nil?
  required = [
    "table_names IS NOT DISTINCT FROM expected_tables",
    "view_names IS NOT DISTINCT FROM expected_views",
    "matview_names IS NOT DISTINCT FROM expected_matviews",
    "sequence_names IS NOT DISTINCT FROM expected_sequences",
    "index_names IS NOT DISTINCT FROM expected_indexes"
  ]
  !required.all? { |s| gate.include?(s) } || !gate.match?(/relkind\s+IN\s*\(\s*'i'\s*,\s*'I'\s*\)/) || !gate.include?("conindid")
end
fail_collect(failures, "fixture: post-apply-only exact equality detector inert") unless fixture_rejects?(
  exact_equality_only_post_apply,
  <<~BAD
    DO $$
    BEGIN
      IF found_version IS NOT DISTINCT FROM 'x' THEN
        action := 'noop';
      END IF;
      INSERT INTO _bootstrap_gate(action) VALUES (action);
    END
    $$;
    -- post-apply only
    IF table_names IS NOT DISTINCT FROM expected_tables
       AND index_names IS NOT DISTINCT FROM expected_indexes THEN
      NULL;
    END IF;
  BAD
)

# Index catalog missing partitioned relkind I.
missing_partitioned_index_relkind = lambda do |src|
  src.match?(/i\.relkind\s*=\s*'i'/) && !src.match?(/relkind\s+IN\s*\(\s*'i'\s*,\s*'I'\s*\)/)
end
fail_collect(failures, "fixture: missing partitioned-index I detector inert") unless fixture_rejects?(
  missing_partitioned_index_relkind,
  <<~BAD
    SELECT coalesce(array_agg(i.relname ORDER BY i.relname), ARRAY[]::text[])
      INTO index_names
      FROM pg_catalog.pg_class i
      JOIN pg_catalog.pg_namespace n ON n.oid = i.relnamespace
      JOIN pg_catalog.pg_index ix ON ix.indexrelid = i.oid
     WHERE n.nspname = 'public'
       AND i.relkind = 'i'
       AND NOT EXISTS (
         SELECT 1 FROM pg_catalog.pg_constraint c WHERE c.conindid = ix.indexrelid
       );
  BAD
)

# Name-suffix index exclusion (unsound) vs OID join.
name_suffix_index = lambda do |src|
  src.match?(/indexname\s+!~/) || src.include?("_(pkey|key)$")
end
fail_collect(failures, "fixture: name-suffix index exclusion detector inert") unless fixture_rejects?(
  name_suffix_index,
  <<~BAD
    SELECT coalesce(array_agg(indexname ORDER BY indexname), ARRAY[]::text[])
      INTO index_names FROM pg_indexes
     WHERE schemaname = 'public' AND indexname !~ '_(pkey|key)$';
  BAD
)

# mapfile+process-substitution catalog read (loses exit status).
ch_mapfile_lost_status = lambda do |src|
  src.match?(/mapfile\s+-t\s+\w+\s+<\s+<\(list_analytics_/)
end
fail_collect(failures, "fixture: clickhouse mapfile status-loss detector inert") unless fixture_rejects?(
  ch_mapfile_lost_status,
  <<~BAD
    mapfile -t LIVE_TABLES < <(list_analytics_tables)
    mapfile -t LIVE_VIEWS < <(list_analytics_views)
    if [[ "${#LIVE_TABLES[@]}" -eq 0 ]]; then decide="apply"; fi
  BAD
)

# Missing default-sentinel-before-CREATE DATABASE / users ordering.
ch_users_before_sentinel = lambda do |src|
  sentinel = src.index("CREATE TABLE IF NOT EXISTS default.${SENTINEL_TABLE}") ||
             src.index("CREATE TABLE IF NOT EXISTS default._bootstrap_in_progress")
  user = src.index("CREATE USER IF NOT EXISTS")
  create_db = src.index("CREATE DATABASE IF NOT EXISTS analytics")
  return true if user && sentinel.nil?
  return true if create_db && sentinel && create_db < sentinel
  return false if user.nil? || sentinel.nil?
  user < sentinel
end
fail_collect(failures, "fixture: users-before-sentinel detector inert") unless fixture_rejects?(
  ch_users_before_sentinel,
  <<~BAD
    ch_query_admin "CREATE DATABASE IF NOT EXISTS analytics"
    ch_query_admin "CREATE USER IF NOT EXISTS bootstrap"
    ch_query_admin "CREATE TABLE IF NOT EXISTS default._bootstrap_in_progress (x UInt8) ENGINE = Memory"
  BAD
)

# Marker before DROP sentinel / grants.
ch_marker_before_drop = lambda do |src|
  insert = src.index("INSERT INTO analytics.schema_bootstrap_meta")
  drop = src.index("DROP TABLE IF EXISTS default.${SENTINEL_TABLE}") || src.index("DROP TABLE IF EXISTS default._bootstrap_in_progress")
  return false if insert.nil? || drop.nil?
  insert < drop
end
fail_collect(failures, "fixture: marker-before-sentinel-drop detector inert") unless fixture_rejects?(
  ch_marker_before_drop,
  <<~BAD
    INSERT INTO analytics.schema_bootstrap_meta (version, checksum) VALUES ('v', 'c')
    DROP TABLE IF EXISTS default._bootstrap_in_progress
  BAD
)

# Missing pipefail on production clickhouse script.
ch_missing_pipefail = lambda do |src|
  !src.match?(/^set -euo pipefail$/) && !src.match?(/^set -o pipefail$/)
end
fail_collect(failures, "fixture: clickhouse missing-pipefail detector inert") unless fixture_rejects?(
  ch_missing_pipefail,
  <<~BAD
    set -eu
    live_tables_raw="$(list_analytics_tables)" || exit 1
  BAD
)
puts "ok regression-fixtures"

# Offline psql gate structure test (boolean \\if / \\gset + ownership/indexes/matviews).
structure_test = File.join(BOOTSTRAP, "tests/psql_gate_structure_test.rb")
fail_collect(failures, "missing psql_gate_structure_test.rb") unless File.file?(structure_test)
if File.file?(structure_test)
  unless system(RbConfig.ruby, structure_test)
    fail_collect(failures, "psql_gate_structure_test.rb failed")
  end
end

# Offline Postgres bootstrap fail-closed (set -euo) shell fixture.
pg_fail_closed = File.join(BOOTSTRAP, "tests/postgres_bootstrap_fail_closed_test.sh")
fail_collect(failures, "missing postgres_bootstrap_fail_closed_test.sh") unless File.file?(pg_fail_closed)
if File.file?(pg_fail_closed)
  unless system("bash", "-n", pg_fail_closed)
    fail_collect(failures, "postgres_bootstrap_fail_closed_test.sh bash -n failed")
  end
  unless system("bash", pg_fail_closed)
    fail_collect(failures, "postgres_bootstrap_fail_closed_test.sh failed")
  end
end

# Disposable-DB CDC SQL integration harness (executed from validate.sh; syntax here).
cdc_it = File.join(BOOTSTRAP, "tests/cdc_sql_integration_test.sh")
fail_collect(failures, "missing cdc_sql_integration_test.sh") unless File.file?(cdc_it)
if File.file?(cdc_it)
  unless system("bash", "-n", cdc_it)
    fail_collect(failures, "cdc_sql_integration_test.sh bash -n failed")
  end
end

# Offline ClickHouse catalog fail-closed + marker ordering shell fixture.
ch_fail_closed = File.join(BOOTSTRAP, "tests/clickhouse_catalog_fail_closed_test.sh")
fail_collect(failures, "missing clickhouse_catalog_fail_closed_test.sh") unless File.file?(ch_fail_closed)
if File.file?(ch_fail_closed)
  unless system("bash", "-n", ch_fail_closed)
    fail_collect(failures, "clickhouse_catalog_fail_closed_test.sh bash -n failed")
  end
  unless system("bash", ch_fail_closed)
    fail_collect(failures, "clickhouse_catalog_fail_closed_test.sh failed")
  end
end

# Offline ClickHouse SQL splitter (multi-statement → one HTTP POST each).
ch_split_test = File.join(BOOTSTRAP, "tests/clickhouse_sql_split_test.rb")
fail_collect(failures, "missing clickhouse_sql_split_test.rb") unless File.file?(ch_split_test)
if File.file?(ch_split_test)
  unless system(RbConfig.ruby, "-c", File.join(BOOTSTRAP, "lib/clickhouse_sql_split.rb"))
    fail_collect(failures, "clickhouse_sql_split.rb ruby -c failed")
  end
  unless system(RbConfig.ruby, "-c", File.join(BOOTSTRAP, "bin/split-clickhouse-sql.rb"))
    fail_collect(failures, "split-clickhouse-sql.rb ruby -c failed")
  end
  unless system(RbConfig.ruby, "-c", ch_split_test)
    fail_collect(failures, "clickhouse_sql_split_test.rb ruby -c failed")
  end
  unless system(RbConfig.ruby, ch_split_test)
    fail_collect(failures, "clickhouse_sql_split_test.rb failed")
  end
end

if failures.empty?
  puts "ok validate_kind all checks passed"
  exit 0
end

warn "\n#{failures.size} validation failure(s)"
exit 1
