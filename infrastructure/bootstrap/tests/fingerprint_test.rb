#!/usr/bin/env ruby
# frozen_string_literal: true

# Unit-style tests for bootstrap fingerprint / state-decision generation (no containers).
# Ruby stdlib only. Models exact metadata cardinality, indexes, ownership, and matviews.

require "json"

ROOT = File.expand_path("../../..", __dir__)
require_relative "../lib/fingerprint"

failures = []

def check(failures, name)
  yield
  puts "ok #{name}"
rescue StandardError => e
  failures << "#{name}: #{e.message}"
  warn "FAIL #{name}: #{e.message}"
end

# Decision model mirroring bootstrap-postgres gate semantics (offline).
def decide(tables:, views:, matviews: [], sequences:, indexes: [],
           version_count:, meta_count:, version:, meta_version:, checksum:,
           owners:, expected:, bootstrap_role:)
  user_objects = tables.size + views.size + matviews.size + sequences.size + indexes.size
  return "apply" if user_objects.zero?

  catalog_exact =
    version_count == 1 &&
    meta_count == 1 &&
    version == expected["version"] &&
    meta_version == expected["version"] &&
    checksum == expected["checksum"] &&
    tables.sort == expected["tables"].sort &&
    views.sort == expected["views"].sort &&
    matviews.sort == Array(expected["materialized_views"]).sort &&
    sequences.sort == expected["sequences"].sort &&
    indexes.sort == Array(expected["indexes"]).sort

  return "fail" unless catalog_exact

  ownership_ok = owners.all? { |_name, owner| owner == bootstrap_role }
  ownership_ok ? "noop" : "fail"
end

# Explicit indexes only: constraint-backed OIDs are excluded (not name suffix).
# Includes ordinary (i) and partitioned (I) indexes at runtime.
def explicit_indexes(index_rows)
  # index_rows: [{name:, constraint_backed: bool, relkind: 'i'|'I'}]
  index_rows.reject { |r| r[:constraint_backed] }.map { |r| r[:name] }.sort
end

check(failures, "postgres-parser") do
  sql = <<~SQL
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version TEXT PRIMARY KEY
    );
    CREATE TABLE IF NOT EXISTS outbox_events (
        outbox_id BIGSERIAL PRIMARY KEY,
        event_id TEXT NOT NULL UNIQUE
    );
    CREATE INDEX IF NOT EXISTS outbox_events_unpublished_idx
        ON outbox_events (created_at);
    CREATE VIEW IF NOT EXISTS unused_view AS SELECT 1;
    CREATE MATERIALIZED VIEW IF NOT EXISTS unused_matview AS SELECT 1;
  SQL
  doc = BootstrapFingerprint.parse_postgres_migration(sql, context: "fixture")
  raise "tables=#{doc['tables'].inspect}" unless doc["tables"] == %w[outbox_events schema_bootstrap_meta schema_migrations]
  raise "views=#{doc['views'].inspect}" unless doc["views"] == %w[unused_view]
  raise "matviews=#{doc['materialized_views'].inspect}" unless doc["materialized_views"] == %w[unused_matview]
  raise "sequences=#{doc['sequences'].inspect}" unless doc["sequences"] == %w[outbox_events_outbox_id_seq]
  raise "indexes=#{doc['indexes'].inspect}" unless doc["indexes"] == %w[outbox_events_unpublished_idx]
end

check(failures, "state-decision-empty-apply") do
  expected = {
    "version" => "001_init",
    "checksum" => "abc",
    "tables" => %w[schema_bootstrap_meta schema_migrations players],
    "views" => [],
    "materialized_views" => [],
    "sequences" => [],
    "indexes" => []
  }
  action = decide(
    tables: [], views: [], sequences: [], indexes: [],
    version_count: 0, meta_count: 0, version: nil, meta_version: nil, checksum: nil,
    owners: {}, expected: expected, bootstrap_role: "bootstrap"
  )
  raise "expected apply" unless action == "apply"
end

check(failures, "state-decision-matview-is-nonempty-fail") do
  expected = {
    "version" => "001_init",
    "checksum" => "abc",
    "tables" => %w[schema_bootstrap_meta schema_migrations players],
    "views" => [],
    "materialized_views" => [],
    "sequences" => [],
    "indexes" => []
  }
  action = decide(
    tables: [], views: [], matviews: %w[orphan_mv], sequences: [], indexes: [],
    version_count: 0, meta_count: 0, version: nil, meta_version: nil, checksum: nil,
    owners: { "orphan_mv" => "bootstrap" }, expected: expected, bootstrap_role: "bootstrap"
  )
  raise "expected fail got #{action}" unless action == "fail"
end

check(failures, "state-decision-extra-meta-cardinality-fail") do
  expected = {
    "version" => "001_init",
    "checksum" => "abc",
    "tables" => %w[schema_bootstrap_meta schema_migrations players],
    "views" => [],
    "materialized_views" => [],
    "sequences" => [],
    "indexes" => []
  }
  owners = expected["tables"].to_h { |t| [t, "bootstrap"] }
  action = decide(
    tables: expected["tables"], views: [], sequences: [], indexes: [],
    version_count: 1, meta_count: 2, version: "001_init", meta_version: "001_init", checksum: "abc",
    owners: owners, expected: expected, bootstrap_role: "bootstrap"
  )
  raise "expected fail got #{action}" unless action == "fail"
end

check(failures, "state-decision-checksum-mismatch-fail") do
  expected = {
    "version" => "001_init",
    "checksum" => "abc",
    "tables" => %w[schema_bootstrap_meta schema_migrations players],
    "views" => [],
    "materialized_views" => [],
    "sequences" => [],
    "indexes" => []
  }
  owners = expected["tables"].to_h { |t| [t, "bootstrap"] }
  action = decide(
    tables: expected["tables"], views: [], sequences: [], indexes: [],
    version_count: 1, meta_count: 1, version: "001_init", meta_version: "001_init", checksum: "zzz",
    owners: owners, expected: expected, bootstrap_role: "bootstrap"
  )
  raise "expected fail got #{action}" unless action == "fail"
end

check(failures, "state-decision-unexpected-index-fail") do
  expected = {
    "version" => "001_init",
    "checksum" => "abc",
    "tables" => %w[schema_bootstrap_meta schema_migrations players],
    "views" => [],
    "materialized_views" => [],
    "sequences" => [],
    "indexes" => %w[players_email_idx]
  }
  owners = expected["tables"].to_h { |t| [t, "bootstrap"] }
  action = decide(
    tables: expected["tables"], views: [], sequences: [],
    indexes: %w[players_email_idx audit_key],
    version_count: 1, meta_count: 1, version: "001_init", meta_version: "001_init", checksum: "abc",
    owners: owners, expected: expected, bootstrap_role: "bootstrap"
  )
  raise "expected fail got #{action}" unless action == "fail"
end

check(failures, "state-decision-unexpected-partitioned-index-fail") do
  # Partitioned indexes (relkind I) participate in full index-set equality.
  expected = {
    "version" => "001_init",
    "checksum" => "abc",
    "tables" => %w[schema_bootstrap_meta schema_migrations players],
    "views" => [],
    "materialized_views" => [],
    "sequences" => [],
    "indexes" => %w[players_email_idx]
  }
  owners = expected["tables"].to_h { |t| [t, "bootstrap"] }
  action = decide(
    tables: expected["tables"], views: [], sequences: [],
    indexes: %w[players_email_idx players_part_idx],
    version_count: 1, meta_count: 1, version: "001_init", meta_version: "001_init", checksum: "abc",
    owners: owners, expected: expected, bootstrap_role: "bootstrap"
  )
  raise "expected fail got #{action}" unless action == "fail"
end

check(failures, "state-decision-orphan-partitioned-index-is-nonempty") do
  expected = {
    "version" => "001_init",
    "checksum" => "abc",
    "tables" => %w[schema_bootstrap_meta schema_migrations players],
    "views" => [],
    "materialized_views" => [],
    "sequences" => [],
    "indexes" => []
  }
  action = decide(
    tables: [], views: [], sequences: [], indexes: %w[orphan_part_idx],
    version_count: 0, meta_count: 0, version: nil, meta_version: nil, checksum: nil,
    owners: { "orphan_part_idx" => "bootstrap" }, expected: expected, bootstrap_role: "bootstrap"
  )
  raise "expected fail got #{action}" unless action == "fail"
end

check(failures, "state-decision-ownership-drift-fail") do
  expected = {
    "version" => "001_init",
    "checksum" => "abc",
    "tables" => %w[schema_bootstrap_meta schema_migrations players],
    "views" => [],
    "materialized_views" => [],
    "sequences" => [],
    "indexes" => []
  }
  owners = {
    "schema_bootstrap_meta" => "bootstrap",
    "schema_migrations" => "bootstrap",
    "players" => "other_role"
  }
  action = decide(
    tables: expected["tables"], views: [], sequences: [], indexes: [],
    version_count: 1, meta_count: 1, version: "001_init", meta_version: "001_init", checksum: "abc",
    owners: owners, expected: expected, bootstrap_role: "bootstrap"
  )
  raise "expected fail got #{action}" unless action == "fail"
end

check(failures, "state-decision-exact-noop-with-ownership") do
  expected = {
    "version" => "001_init",
    "checksum" => "abc",
    "tables" => %w[schema_bootstrap_meta schema_migrations players],
    "views" => [],
    "materialized_views" => %w[players_mv],
    "sequences" => [],
    "indexes" => %w[players_email_idx]
  }
  owners = (expected["tables"] + expected["materialized_views"]).to_h { |t| [t, "bootstrap"] }
  action = decide(
    tables: expected["tables"], views: [], matviews: expected["materialized_views"],
    sequences: [], indexes: expected["indexes"],
    version_count: 1, meta_count: 1, version: "001_init", meta_version: "001_init", checksum: "abc",
    owners: owners, expected: expected, bootstrap_role: "bootstrap"
  )
  raise "expected noop got #{action}" unless action == "noop"
end

check(failures, "constraint-backed-index-oid-exclusion") do
  rows = [
    { name: "players_pkey", constraint_backed: true, relkind: "i" },
    { name: "players_email_key", constraint_backed: true, relkind: "i" },
    { name: "audit_key", constraint_backed: false, relkind: "i" },
    { name: "players_email_idx", constraint_backed: false, relkind: "i" },
    { name: "players_part_idx", constraint_backed: false, relkind: "I" }
  ]
  got = explicit_indexes(rows)
  raise "got=#{got.inspect}" unless got == %w[audit_key players_email_idx players_part_idx]
end

check(failures, "name-suffix-exclusion-is-unsound") do
  # Demonstrates why *_key suffix filtering is wrong: explicit audit_key must remain.
  by_suffix = %w[players_pkey players_email_key audit_key players_email_idx].reject { |n| n.match?(/_(pkey|key)$/) }
  by_oid = explicit_indexes([
    { name: "players_pkey", constraint_backed: true },
    { name: "players_email_key", constraint_backed: true },
    { name: "audit_key", constraint_backed: false },
    { name: "players_email_idx", constraint_backed: false }
  ])
  raise "suffix wrongly dropped audit_key" unless by_suffix == %w[players_email_idx]
  raise "oid path must keep audit_key" unless by_oid.include?("audit_key")
end

check(failures, "partitioned-and-matview-relkinds") do
  relkinds = %w[r p v m S i I]
  raise "missing partitioned p" unless relkinds.include?("p")
  raise "missing matview m" unless relkinds.include?("m")
  raise "missing partitioned index I" unless relkinds.include?("I")
  src = File.read(File.join(ROOT, "infrastructure/bootstrap/bin/bootstrap-postgres.sh"))
  raise "bootstrap missing ownership relkind set with I" unless src.match?(/relkind\s+IN\s*\(\s*'r'\s*,\s*'p'\s*,\s*'v'\s*,\s*'m'\s*,\s*'S'\s*,\s*'i'\s*,\s*'I'\s*\)/)
  raise "bootstrap missing index catalog relkind i,I" unless src.match?(/relkind\s+IN\s*\(\s*'i'\s*,\s*'I'\s*\)/)
end

check(failures, "identity-migration-stable") do
  path = File.join(ROOT, "services/identity/migrations/001_init.sql")
  raise "missing #{path}" unless File.file?(path)
  a = BootstrapFingerprint.fingerprint_for(path, context: "identity", engine: "postgres")
  b = BootstrapFingerprint.fingerprint_for(path, context: "identity", engine: "postgres")
  raise "unstable" unless a == b
  raise "missing players" unless a["tables"].include?("players")
  raise "missing meta" unless a["tables"].include?("schema_bootstrap_meta")
  raise "missing seq" unless a["sequences"].include?("outbox_events_outbox_id_seq")
  raise "missing materialized_views key" unless a.key?("materialized_views")
  raise "bad checksum" unless a["checksum"].size == 64
end

check(failures, "clickhouse-analytics-tables") do
  path = File.join(ROOT, "services/analytics/migrations/001_init.sql")
  raise "missing #{path}" unless File.file?(path)
  doc = BootstrapFingerprint.fingerprint_for(path, context: "analytics", engine: "clickhouse")
  raise "missing gameplay_metrics" unless doc["tables"].include?("gameplay_metrics")
  raise "missing meta" unless doc["tables"].include?("schema_bootstrap_meta")
  raise "engine" unless doc["engine"] == "clickhouse"
  raise "sentinel must not be in fingerprint" if doc["tables"].include?("_bootstrap_in_progress")
end

if failures.empty?
  puts "ok fingerprint_test all passed"
  exit 0
end
warn "\n#{failures.size} fingerprint test failure(s)"
exit 1
