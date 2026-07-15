#!/usr/bin/env ruby
# frozen_string_literal: true

# Offline checks for kind-short retention classes and DLQ scaffolding.

require "yaml"
require "set"

ROOT = File.expand_path("../../..", __dir__)
RENDER = File.join(ROOT, "infrastructure/kind/scripts/render-kafka-topics.rb")
PLAN = File.join(ROOT, "infrastructure/kind/generated/kafka-topic-plan.yaml")
SCRIPT = File.join(ROOT, "infrastructure/kind/generated/kafka-create-topics.sh")
ASYNCAPI = File.join(ROOT, "contracts/asyncapi/kafka-v1.yaml")

failures = []

def fail_collect(failures, msg)
  failures << msg
  warn "FAIL: #{msg}"
end

fail_collect(failures, "missing render-kafka-topics.rb") unless File.file?(RENDER)
src = File.read(RENDER)

fail_collect(failures, "render must define kind-short retention ms") unless src.include?("KIND_RETENTION_MS")
fail_collect(failures, "render must include spectator-safe class") unless src.include?("spectator-safe")
fail_collect(failures, "render must include metrics-control class") unless src.include?("metrics-control")
fail_collect(failures, "render must include dlq class") unless src.include?('"dlq"')
fail_collect(failures, "render must scaffold ADR-0017 DLQ names") unless src.include?("DOCUMENTED_DLQ_CONSUMERS") && src.include?(".dlq")
fail_collect(failures, "render must not claim consumers exist") unless src.include?("not claimed") || src.include?("scaffolding")
fail_collect(failures, "connect topics must stay compacted") unless src.include?('"cleanupPolicy" => "compact"')
fail_collect(failures, "create_or_assert must check retention.ms") unless src.include?("retention.ms")

# Re-render into a temp sense: load generated plan if present after caller ran kind-render.
if File.file?(PLAN) && File.file?(SCRIPT)
  plan = YAML.load_file(PLAN)
  async = YAML.load_file(ASYNCAPI)
  channels = async.fetch("channels").keys.sort
  domain = plan.dig("spec", "domainTopics") || []
  dlq = plan.dig("spec", "dlqTopics") || []
  connect = plan.dig("spec", "connectInternalTopics") || []
  total_partitions = (domain + dlq + connect).sum { |topic| topic["partitions"].to_i }

  fail_collect(failures, "domain topic set != AsyncAPI") unless domain.map { |t| t["name"] }.sort == channels
  fail_collect(failures, "kind plan partitions=#{total_partitions} expected=44") unless total_partitions == 44

  spectator = domain.find { |t| t["name"] == "room.spectator-safe.events" }
  metrics = domain.find { |t| t["name"] == "room.gameplay.metrics" }
  identity = domain.find { |t| t["name"] == "identity.session.invalidated" }
  business = domain.find { |t| t["name"] == "room.game.completed" }

  fail_collect(failures, "spectator retention class") unless spectator && spectator["retentionClass"] == "spectator-safe"
  fail_collect(failures, "metrics retention class") unless metrics && metrics["retentionClass"] == "metrics-control"
  fail_collect(failures, "identity retention class") unless identity && identity["retentionClass"] == "metrics-control"
  fail_collect(failures, "business retention class") unless business && business["retentionClass"] == "business"

  [spectator, metrics, identity, business].compact.each do |t|
    fail_collect(failures, "#{t['name']} missing retentionMs") if t["retentionMs"].nil? || t["retentionMs"].to_i <= 0
  end

  want_dlq = "room.game.completed.ranking.dlq"
  fail_collect(failures, "missing DLQ #{want_dlq}") unless dlq.any? { |t| t["name"] == want_dlq }
  fail_collect(failures, "DLQ retention class") unless dlq.all? { |t| t["retentionClass"] == "dlq" && t["retentionMs"].to_i > 0 }

  rebuild = domain.find { |t| t["name"] == "spectator.projection.rebuild_requested" }
  rebuild_dlq = dlq.find { |t| t["name"] == "spectator.projection.rebuild_requested.spectator-view.dlq" }
  fail_collect(failures, "missing spectator.projection.rebuild_requested") unless rebuild
  fail_collect(failures, "spectator rebuild_requested must use 2 kind partitions") unless rebuild && rebuild["partitions"].to_i == 2
  fail_collect(failures, "spectator rebuild_requested must use local RF=1") unless rebuild && rebuild["replicationFactor"].to_i == 1
  fail_collect(failures, "missing spectator rebuild DLQ") unless rebuild_dlq
  fail_collect(failures, "spectator rebuild DLQ must use 2 kind partitions") unless rebuild_dlq && rebuild_dlq["partitions"].to_i == 2

  analytics_rebuild = domain.find { |t| t["name"] == "analytics.projection.rebuild_requested" }
  analytics_rebuild_dlq = dlq.find { |t| t["name"] == "analytics.projection.rebuild_requested.analytics.dlq" }
  fail_collect(failures, "missing analytics.projection.rebuild_requested") unless analytics_rebuild
  fail_collect(failures, "analytics rebuild_requested must use 2 kind partitions") unless analytics_rebuild && analytics_rebuild["partitions"].to_i == 2
  fail_collect(failures, "analytics rebuild_requested must use local RF=1") unless analytics_rebuild && analytics_rebuild["replicationFactor"].to_i == 1
  fail_collect(failures, "missing analytics rebuild DLQ") unless analytics_rebuild_dlq
  fail_collect(failures, "analytics rebuild DLQ must use 2 kind partitions") unless analytics_rebuild_dlq && analytics_rebuild_dlq["partitions"].to_i == 2

  connect.each do |t|
    fail_collect(failures, "connect topic #{t['name']} must be compact") unless t["cleanupPolicy"] == "compact"
    fail_collect(failures, "connect topic #{t['name']} must use 1 kind partition") unless t["partitions"].to_i == 1
    fail_collect(failures, "connect topic must not set retentionMs") if t.key?("retentionMs") && !t["retentionMs"].nil?
  end

  script = File.read(SCRIPT)
  fail_collect(failures, "topic script must assert retention.ms drift") unless script.include?("retention.ms")
  fail_collect(failures, "topic script must create DLQ topics") unless script.include?(want_dlq)
  fail_collect(failures, "topic script must create spectator rebuild_requested") unless script.include?("spectator.projection.rebuild_requested")
  fail_collect(failures, "topic script must create spectator rebuild DLQ") unless script.include?("spectator.projection.rebuild_requested.spectator-view.dlq")
  fail_collect(failures, "topic script must create analytics rebuild_requested") unless script.include?("analytics.projection.rebuild_requested")
  fail_collect(failures, "topic script must create analytics rebuild DLQ") unless script.include?("analytics.projection.rebuild_requested.analytics.dlq")
  fail_collect(failures, "connect create must omit retention arg") unless script.match?(/create_or_assert_topic "connect-configs".*\n/)
  # Compact connect lines should be the 5-arg form (no retention).
  connect_lines = script.lines.select { |l| l.include?("create_or_assert_topic \"connect-") }
  connect_lines.each do |line|
    # name partitions rf cleanup min_isr [retention]
    args = line.strip.split(/\s+/)
    fail_collect(failures, "connect line must not pass retention.ms: #{line.strip}") if args.size > 6
  end
end

if failures.empty?
  puts "ok kafka_retention_dlq_test"
  exit 0
end

warn "\n#{failures.size} kafka retention/DLQ failure(s)"
exit 1
