#!/usr/bin/env ruby
# frozen_string_literal: true

# Deterministically render kind Kafka topic init artifacts from AsyncAPI.
# Ruby stdlib only. No network. No hand-maintained domain topic list.
# Retention classes follow ADR-0032 with explicit kind-short retention.ms.
# Consumer-owned DLQ scaffolding follows ADR-0017 naming; does not claim consumers exist.

require "yaml"
require "fileutils"
require "set"

ROOT = File.expand_path("../../..", __dir__)
ASYNCAPI = File.join(ROOT, "contracts/asyncapi/kafka-v1.yaml")
OUT_DIR = File.expand_path("../generated", __dir__)
PLAN_PATH = File.join(OUT_DIR, "kafka-topic-plan.yaml")
SCRIPT_PATH = File.join(OUT_DIR, "kafka-create-topics.sh")
JOB_PATH = File.join(OUT_DIR, "job-kafka-topics.yaml")

KIND_HIGH_PARTITIONS = 2
KIND_BUSINESS_PARTITIONS = 1
# Rebuild behavior is proven with keyed ordering, not production-scale concurrency.
KIND_REBUILD_REQUEST_PARTITIONS = 2
KIND_REPLICATION_FACTOR = 1

# Kind-short retention.ms by ADR-0032 class (no production recovery-window claim).
KIND_RETENTION_MS = {
  "spectator-safe" => 600_000,      # 10m (prod 6h)
  "metrics-control" => 1_800_000,   # 30m (prod 24h)
  "business" => 7_200_000,          # 2h  (prod 7d)
  "dlq" => 21_600_000               # 6h  (prod 30d)
}.freeze

CONNECT_TOPICS = [
  { "name" => "connect-configs", "partitions" => 1, "replicationFactor" => 1, "cleanupPolicy" => "compact", "class" => "connect-internal" },
  { "name" => "connect-offsets", "partitions" => 1, "replicationFactor" => 1, "cleanupPolicy" => "compact", "class" => "connect-internal" },
  { "name" => "connect-status", "partitions" => 1, "replicationFactor" => 1, "cleanupPolicy" => "compact", "class" => "connect-internal" }
].freeze

REBUILD_REQUEST_TOPICS = [
  "spectator.projection.rebuild_requested",
  "analytics.projection.rebuild_requested"
].freeze

# Documented consumer-owned DLQ declarations only (architecture integration table).
# Topic scaffolding does not itself prove consumer deployment; do not invent undeclared groups.
DOCUMENTED_DLQ_CONSUMERS = [
  { "source" => "identity.session.invalidated", "consumer" => "gateway" },
  { "source" => "room.game.completed", "consumer" => "ranking" },
  { "source" => "room.match.completed", "consumer" => "tournament-orchestration" },
  { "source" => "room.runtime.ready", "consumer" => "tournament-orchestration" },
  { "source" => "room.match.completed", "consumer" => "analytics" },
  { "source" => "room.spectator-safe.events", "consumer" => "spectator-view" },
  { "source" => "spectator.projection.rebuild_requested", "consumer" => "spectator-view" },
  { "source" => "analytics.projection.rebuild_requested", "consumer" => "analytics" },
  { "source" => "room.gameplay.metrics", "consumer" => "analytics" },
  { "source" => "tournament.match.assigned", "consumer" => "analytics" },
  { "source" => "tournament.match.result_recorded", "consumer" => "analytics" },
  { "source" => "tournament.players.advanced", "consumer" => "analytics" },
  { "source" => "tournament.players.advanced", "consumer" => "ranking" },
  { "source" => "tournament.round.completed", "consumer" => "analytics" },
  { "source" => "tournament.completed", "consumer" => "analytics" },
  { "source" => "tournament.completed", "consumer" => "ranking" },
  { "source" => "ranking.player_rating_updated", "consumer" => "analytics" },
  { "source" => "ranking.leaderboard_snapshot_published", "consumer" => "analytics" }
].freeze

def fail!(msg)
  warn "render-kafka-topics: #{msg}"
  exit 1
end

def retention_class_for(name, high_topics)
  return "spectator-safe" if name == "room.spectator-safe.events"
  return "metrics-control" if name == "room.gameplay.metrics" || name == "identity.session.invalidated"

  "business"
end

def dlq_topic_name(source, consumer)
  "#{source}.#{consumer}.dlq"
end

fail!("missing AsyncAPI at #{ASYNCAPI}") unless File.file?(ASYNCAPI)

doc = YAML.load_file(ASYNCAPI)
channels = doc.fetch("channels")
planning = doc.fetch("x-partitionPlanning")
high_topics = planning.fetch("highVolumeTopics")
retention_plan = doc.fetch("x-retentionPlanning")
fail!("AsyncAPI must allow local shorter retention") unless retention_plan["localKindMayUseShorterRetention"]

domain_topics = channels.keys.sort.map do |name|
  klass = high_topics.include?(name) ? "high" : "business"
  partitions = if REBUILD_REQUEST_TOPICS.include?(name)
                 KIND_REBUILD_REQUEST_PARTITIONS
               elsif klass == "high"
                 KIND_HIGH_PARTITIONS
               else
                 KIND_BUSINESS_PARTITIONS
               end
  prod_partitions = channels.dig(name, "bindings", "kafka", "partitions")
  ret_class = retention_class_for(name, high_topics)
  {
    "name" => name,
    "class" => klass,
    "retentionClass" => ret_class,
    "retentionMs" => KIND_RETENTION_MS.fetch(ret_class),
    "partitions" => partitions,
    "replicationFactor" => KIND_REPLICATION_FACTOR,
    "productionPlanningPartitions" => prod_partitions,
    "cleanupPolicy" => "delete"
  }
end

channel_set = channels.keys.to_set
DOCUMENTED_DLQ_CONSUMERS.each do |row|
  fail!("DLQ source #{row['source']} missing from AsyncAPI") unless channel_set.include?(row["source"])
end

dlq_topics = DOCUMENTED_DLQ_CONSUMERS.map do |row|
  name = dlq_topic_name(row["source"], row["consumer"])
  partitions = if REBUILD_REQUEST_TOPICS.include?(row["source"])
                 KIND_REBUILD_REQUEST_PARTITIONS
               else
                 KIND_BUSINESS_PARTITIONS
               end
  {
    "name" => name,
    "class" => "dlq",
    "retentionClass" => "dlq",
    "retentionMs" => KIND_RETENTION_MS.fetch("dlq"),
    "partitions" => partitions,
    "replicationFactor" => KIND_REPLICATION_FACTOR,
    "cleanupPolicy" => "delete",
    "sourceTopic" => row["source"],
    "consumer" => row["consumer"],
    "note" => "ADR-0017 scaffolding; consumer process not claimed"
  }
end.sort_by { |t| t["name"] }

plan = {
  "apiVersion" => "uno-arena.local/v1",
  "kind" => "KafkaTopicPlan",
  "metadata" => {
    "name" => "kind-local",
    "annotations" => {
      "uno-arena.local/non-production" => "true",
      "uno-arena.local/source" => "contracts/asyncapi/kafka-v1.yaml",
      "uno-arena.local/retention" => "ADR-0032 kind-short; no production recovery claim"
    }
  },
  "spec" => {
    "replicationFactor" => KIND_REPLICATION_FACTOR,
    "minInSyncReplicas" => 1,
    "partitionPolicy" => {
      "high" => KIND_HIGH_PARTITIONS,
      "business" => KIND_BUSINESS_PARTITIONS,
      "note" => "Reduced kind counts preserving high-vs-low classes; RF1 non-HA"
    },
    "retentionPolicy" => {
      "kindShortMs" => KIND_RETENTION_MS,
      "note" => "Explicit kind-short retention.ms by ADR-0032 class"
    },
    "domainTopics" => domain_topics,
    "dlqTopics" => dlq_topics,
    "connectInternalTopics" => CONNECT_TOPICS
  }
}

script = +<<~'SH'
#!/usr/bin/env bash
# Generated by infrastructure/kind/scripts/render-kafka-topics.rb — do not hand-edit.
# Creates AsyncAPI domain topics + consumer-owned DLQ scaffolding + Connect internal topics.
# Existing topics must match partitions, RF, cleanup.policy, min.insync.replicas,
# and retention.ms (delete topics) (ADR-0030 / ADR-0032).
# Config checks fail closed on empty/unparseable values (apache/kafka:4.3.1 describe + configs).
set -euo pipefail
BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka.uno-arena.svc.cluster.local:9092}"
TOPICS_BIN="${KAFKA_TOPICS_BIN:-/opt/kafka/bin/kafka-topics.sh}"
CONFIGS_BIN="${KAFKA_CONFIGS_BIN:-/opt/kafka/bin/kafka-configs.sh}"

# Parse apache/kafka:4.3.1 topic describe Configs: k=v,k=v and kafka-configs --describe --all lines.
kafka_config_value() {
  local blob="$1" key="$2" val=""
  val="$(printf '%s\n' "$blob" | awk -v key="$key" '
    function emit(v) { gsub(/[[:space:]]/, "", v); if (v != "") { print v; exit } }
    /Configs:/ {
      line = $0
      sub(/.*Configs:[[:space:]]*/, "", line)
      n = split(line, parts, /,/)
      for (i = 1; i <= n; i++) {
        split(parts[i], pair, /=/)
        if (pair[1] == key) emit(pair[2])
      }
    }
    {
      line = $0
      sub(/^[[:space:]]+/, "", line)
      if (index(line, key "=") == 1) {
        rest = substr(line, length(key) + 2)
        sub(/[[:space:]].*/, "", rest)
        emit(rest)
      }
    }
  ')"
  printf '%s' "$val"
}

create_or_assert_topic() {
  local name="$1" partitions="$2" rf="$3" cleanup="$4" min_isr="$5" retention_ms="${6:-}"
  local desc cfg part_count rf_count cleanup_got min_isr_got retention_got
  if desc="$("$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --describe --topic "$name" 2>/dev/null)"; then
    part_count="$(printf '%s\n' "$desc" | awk '/PartitionCount:/ {for (i=1;i<=NF;i++) if ($i=="PartitionCount:") { print $(i+1); exit }}')"
    rf_count="$(printf '%s\n' "$desc" | awk '/ReplicationFactor:/ {for (i=1;i<=NF;i++) if ($i=="ReplicationFactor:") { print $(i+1); exit }}')"
    cleanup_got="$(kafka_config_value "$desc" "cleanup.policy")"
    min_isr_got="$(kafka_config_value "$desc" "min.insync.replicas")"
    retention_got="$(kafka_config_value "$desc" "retention.ms")"
    if [[ -z "$cleanup_got" || -z "$min_isr_got" || ( -n "$retention_ms" && -z "$retention_got" ) ]]; then
      cfg="$("$CONFIGS_BIN" --bootstrap-server "$BOOTSTRAP" --entity-type topics --entity-name "$name" --describe --all 2>/dev/null || true)"
      [[ -n "$cleanup_got" ]] || cleanup_got="$(kafka_config_value "$cfg" "cleanup.policy")"
      [[ -n "$min_isr_got" ]] || min_isr_got="$(kafka_config_value "$cfg" "min.insync.replicas")"
      if [[ -n "$retention_ms" ]]; then
        [[ -n "$retention_got" ]] || retention_got="$(kafka_config_value "$cfg" "retention.ms")"
      fi
    fi
    if [[ -z "$part_count" ]]; then
      echo "topic drift: $name PartitionCount missing/unparseable from describe" >&2
      exit 1
    fi
    if [[ -z "$rf_count" ]]; then
      echo "topic drift: $name ReplicationFactor missing/unparseable from describe" >&2
      exit 1
    fi
    if [[ "$part_count" != "$partitions" ]]; then
      echo "topic drift: $name PartitionCount=$part_count expected=$partitions (immutable; refusing)" >&2
      exit 1
    fi
    if [[ "$rf_count" != "$rf" ]]; then
      echo "topic drift: $name ReplicationFactor=$rf_count expected=$rf" >&2
      exit 1
    fi
    if [[ -z "$cleanup_got" ]]; then
      echo "topic drift: $name cleanup.policy missing/unparseable (fail closed)" >&2
      exit 1
    fi
    if [[ "$cleanup_got" != "$cleanup" ]]; then
      echo "topic drift: $name cleanup.policy=$cleanup_got expected=$cleanup" >&2
      exit 1
    fi
    if [[ -z "$min_isr_got" ]]; then
      echo "topic drift: $name min.insync.replicas missing/unparseable (fail closed)" >&2
      exit 1
    fi
    if [[ "$min_isr_got" != "$min_isr" ]]; then
      echo "topic drift: $name min.insync.replicas=$min_isr_got expected=$min_isr" >&2
      exit 1
    fi
    if [[ -n "$retention_ms" ]]; then
      if [[ -z "$retention_got" ]]; then
        echo "topic drift: $name retention.ms missing/unparseable (fail closed)" >&2
        exit 1
      fi
      if [[ "$retention_got" != "$retention_ms" ]]; then
        echo "topic drift: $name retention.ms=$retention_got expected=$retention_ms" >&2
        exit 1
      fi
    fi
    if [[ -n "$retention_ms" ]]; then
      echo "topic ok (exact): $name partitions=$partitions rf=$rf cleanup=$cleanup minISR=$min_isr retention.ms=$retention_ms"
    else
      echo "topic ok (exact): $name partitions=$partitions rf=$rf cleanup=$cleanup minISR=$min_isr"
    fi
    return 0
  fi
  if [[ -n "$retention_ms" ]]; then
    "$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --create \
      --topic "$name" \
      --partitions "$partitions" \
      --replication-factor "$rf" \
      --config "cleanup.policy=$cleanup" \
      --config "min.insync.replicas=$min_isr" \
      --config "retention.ms=$retention_ms"
    echo "created topic: $name partitions=$partitions rf=$rf cleanup=$cleanup minISR=$min_isr retention.ms=$retention_ms"
  else
    "$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --create \
      --topic "$name" \
      --partitions "$partitions" \
      --replication-factor "$rf" \
      --config "cleanup.policy=$cleanup" \
      --config "min.insync.replicas=$min_isr"
    echo "created topic: $name partitions=$partitions rf=$rf cleanup=$cleanup minISR=$min_isr"
  fi
}

SH

(domain_topics + dlq_topics).each do |t|
  script << format(
    "create_or_assert_topic %s %d %d %s 1 %d\n",
    t["name"].inspect,
    t["partitions"],
    t["replicationFactor"],
    t["cleanupPolicy"].inspect,
    t["retentionMs"]
  )
end

CONNECT_TOPICS.each do |t|
  script << format(
    "create_or_assert_topic %s %d %d %s 1\n",
    t["name"].inspect,
    t["partitions"],
    t["replicationFactor"],
    t["cleanupPolicy"].inspect
  )
end
script << "\necho \"kafka topic bootstrap complete\"\n"

configmap = {
  "apiVersion" => "v1",
  "kind" => "ConfigMap",
  "metadata" => {
    "name" => "kafka-topic-bootstrap-script",
    "namespace" => "uno-arena",
    "labels" => {
      "app.kubernetes.io/name" => "bootstrap-kafka-topics",
      "app.kubernetes.io/part-of" => "uno-arena",
      "uno-arena.local/bootstrap" => "kafka",
      "uno-arena.local/non-production" => "true"
    }
  },
  "data" => {
    "create-topics.sh" => script
  }
}

job = {
  "apiVersion" => "batch/v1",
  "kind" => "Job",
  "metadata" => {
    "name" => "bootstrap-kafka-topics",
    "namespace" => "uno-arena",
    "labels" => {
      "app.kubernetes.io/name" => "bootstrap-kafka-topics",
      "app.kubernetes.io/part-of" => "uno-arena",
      "uno-arena.local/bootstrap" => "kafka",
      "uno-arena.local/non-production" => "true"
    }
  },
  "spec" => {
    "backoffLimit" => 3,
    "ttlSecondsAfterFinished" => 86_400,
    "template" => {
      "metadata" => {
        "labels" => {
          "app.kubernetes.io/name" => "bootstrap-kafka-topics",
          "uno-arena.local/bootstrap" => "kafka"
        }
      },
      "spec" => {
        "restartPolicy" => "Never",
        "containers" => [
          {
            "name" => "kafka-topics",
            "image" => "apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837",
            "imagePullPolicy" => "IfNotPresent",
            "command" => ["/bin/bash", "/scripts/create-topics.sh"],
            "env" => [
              {
                "name" => "KAFKA_BOOTSTRAP",
                "value" => "kafka.uno-arena.svc.cluster.local:9092"
              }
            ],
            "volumeMounts" => [
              { "name" => "scripts", "mountPath" => "/scripts" }
            ]
          }
        ],
        "volumes" => [
          {
            "name" => "scripts",
            "configMap" => {
              "name" => "kafka-topic-bootstrap-script",
              "defaultMode" => 0o755
            }
          }
        ]
      }
    }
  }
}

FileUtils.mkdir_p(OUT_DIR)
File.write(PLAN_PATH, plan.to_yaml)
File.write(SCRIPT_PATH, script)
FileUtils.chmod(0o755, SCRIPT_PATH)

header = <<~HDR
  # Generated by infrastructure/kind/scripts/render-kafka-topics.rb — do not hand-edit.
HDR
File.write(JOB_PATH, header + [configmap, job].map { |doc| doc.to_yaml }.join)

puts "ok render: #{PLAN_PATH}"
puts "ok render: #{SCRIPT_PATH}"
puts "ok render: #{JOB_PATH}"
puts "ok topics: domain=#{domain_topics.size} dlq=#{dlq_topics.size} connect=#{CONNECT_TOPICS.size}"
