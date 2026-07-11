#!/usr/bin/env ruby
# frozen_string_literal: true

# Offline unit fixtures for ClickHouse SQL statement splitting.
# ClickHouse HTTP rejects multi-statement bodies (Code 62); bootstrap must POST
# one statement per request. Ruby stdlib only — no network / no ClickHouse.

require "rbconfig"
require "tmpdir"
require "fileutils"

ROOT = File.expand_path("../../..", __dir__)
LIB = File.join(ROOT, "infrastructure/bootstrap/lib/clickhouse_sql_split.rb")
BIN = File.join(ROOT, "infrastructure/bootstrap/bin/split-clickhouse-sql.rb")
CH_BOOT = File.join(ROOT, "infrastructure/bootstrap/bin/bootstrap-clickhouse.sh")
ANALYTICS_MIGRATION = File.join(ROOT, "services/analytics/migrations/001_init.sql")

failures = []

def check(failures, name)
  yield
  puts "ok #{name}"
rescue StandardError => e
  failures << "#{name}: #{e.message}"
  warn "FAIL #{name}: #{e.message}"
end

# --- Load splitter (RED until lib exists) ---
check(failures, "splitter-lib-loads") do
  raise "missing #{LIB}" unless File.file?(LIB)
  load LIB
  raise "ClickhouseSqlSplit missing" unless defined?(ClickhouseSqlSplit)
  raise "split missing" unless ClickhouseSqlSplit.respond_to?(:split)
end

# Single-statement validator: exactly one complete statement after split.
def assert_single_statement!(sql, label:)
  stmts = ClickhouseSqlSplit.split(sql)
  raise "#{label}: expected 1 statement, got #{stmts.size}" unless stmts.size == 1
  stmts.fetch(0)
end

def assert_multi_statement!(sql, label:)
  stmts = ClickhouseSqlSplit.split(sql)
  raise "#{label}: expected multi-statement (got #{stmts.size})" unless stmts.size > 1
  stmts
end

# RED demonstration: full analytics migration is multi-statement — the legacy
# full-file HTTP body model is invalid for ClickHouse (Code 62).
check(failures, "legacy-full-file-request-is-multistatement") do
  raise "migration missing" unless File.file?(ANALYTICS_MIGRATION)
  sql = File.read(ANALYTICS_MIGRATION)
  stmts = assert_multi_statement!(sql, label: "analytics 001_init.sql")
  raise "expected CREATE DATABASE + TABLE + INSERT (got #{stmts.size})" if stmts.size < 3
  begin
    assert_single_statement!(sql, label: "full-file")
    raise "full-file single-statement validator should reject multi-statement migration"
  rescue StandardError => e
    raise if e.message.include?("should reject")
    raise "expected multi-statement rejection, got: #{e.message}" unless e.message.include?("expected 1 statement")
  end
end

check(failures, "split-semicolons-inside-single-double-backtick-strings") do
  sql = <<~SQL
    INSERT INTO t (a) VALUES ('a;b');
    INSERT INTO t (a) VALUES ("c;d");
    INSERT INTO t (a) VALUES (`e;f`);
  SQL
  stmts = ClickhouseSqlSplit.split(sql)
  raise "count=#{stmts.size}" unless stmts.size == 3
  raise "single" unless stmts[0].include?("'a;b'")
  raise "double" unless stmts[1].include?('"c;d"')
  raise "backtick" unless stmts[2].include?("`e;f`")
  stmts.each_with_index do |s, i|
    assert_single_statement!(s, label: "string-fixture[#{i}]")
  end
end

check(failures, "split-escaped-and-doubled-quotes") do
  sql = <<~SQL
    INSERT INTO t (a) VALUES ('it''s;fine');
    INSERT INTO t (a) VALUES ('line\\';still;one');
    INSERT INTO t (a) VALUES ("say\\"hi;there");
    INSERT INTO t (a) VALUES ("doubled"";ok");
  SQL
  stmts = ClickhouseSqlSplit.split(sql)
  raise "count=#{stmts.size}" unless stmts.size == 4
  stmts.each_with_index { |s, i| assert_single_statement!(s, label: "escape-fixture[#{i}]") }
end

# Backtick identifiers: doubled `` and backslash-escaped \` must not split on
# a following semicolon (same escape model as single/double quote states).
check(failures, "split-escaped-and-doubled-backticks") do
  sql = <<~SQL
    INSERT INTO t (a) VALUES (`e``x;still`);
    INSERT INTO t (a) VALUES (`e\\`x;still`);
    INSERT INTO t (a) VALUES (`trail\\`;keep`);
  SQL
  stmts = ClickhouseSqlSplit.split(sql)
  raise "count=#{stmts.size} (premature split on escaped backtick?)" unless stmts.size == 3
  raise "doubled" unless stmts[0].include?("`e``x;still`")
  raise "backslash-escaped" unless stmts[1].include?('`e\`x;still`')
  raise "trail-escaped" unless stmts[2].include?('`trail\`;keep`')
  stmts.each_with_index { |s, i| assert_single_statement!(s, label: "backtick-escape-fixture[#{i}]") }
end

check(failures, "split-semicolons-inside-line-and-block-comments") do
  sql = <<~SQL
    -- comment with ; semicolon
    CREATE TABLE a (id Int32); /* block; with; semis */
    CREATE TABLE b (id Int32);
    /* multi
       line; comment */
    CREATE TABLE c (id Int32);
  SQL
  stmts = ClickhouseSqlSplit.split(sql)
  raise "count=#{stmts.size}" unless stmts.size == 3
  raise "a" unless stmts[0].include?("CREATE TABLE a")
  raise "b" unless stmts[1].include?("CREATE TABLE b")
  raise "c" unless stmts[2].include?("CREATE TABLE c")
  stmts.each_with_index { |s, i| assert_single_statement!(s, label: "comment-fixture[#{i}]") }
end

check(failures, "split-rejects-unterminated-quote") do
  raise "ClickhouseSqlSplit not loaded" unless defined?(ClickhouseSqlSplit)
  begin
    ClickhouseSqlSplit.split("INSERT INTO t VALUES ('unterminated;")
    raise "expected rejection of unterminated single quote"
  rescue ClickhouseSqlSplit::Error
    # expected
  end
end

check(failures, "split-rejects-unterminated-block-comment") do
  raise "ClickhouseSqlSplit not loaded" unless defined?(ClickhouseSqlSplit)
  begin
    ClickhouseSqlSplit.split("CREATE TABLE t (id Int32); /* unterminated")
    raise "expected rejection of unterminated block comment"
  rescue ClickhouseSqlSplit::Error
    # expected
  end
end

check(failures, "split-rejects-empty-or-comments-only") do
  raise "ClickhouseSqlSplit not loaded" unless defined?(ClickhouseSqlSplit)
  begin
    ClickhouseSqlSplit.split("-- only comments\n/* and blocks */\n\n")
    raise "expected rejection of empty/invalid output"
  rescue ClickhouseSqlSplit::Error
    # expected
  end

  begin
    ClickhouseSqlSplit.split("   \n\t  ")
    raise "expected rejection of whitespace-only input"
  rescue ClickhouseSqlSplit::Error
    # expected
  end
end
check(failures, "split-real-analytics-migration-each-single-statement") do
  sql = File.read(ANALYTICS_MIGRATION)
  stmts = ClickhouseSqlSplit.split(sql)
  raise "expected >= 7 statements (CREATE DATABASE/TABLEs + INSERT), got #{stmts.size}" unless stmts.size >= 7
  raise "expected exactly 9 statements for current 001_init.sql, got #{stmts.size}" unless stmts.size == 9
  # Leading file header comments may attach to the first statement; body must still be CREATE DATABASE.
  raise "first must be CREATE DATABASE" unless stmts.first.match?(/\bCREATE\s+DATABASE\b/i)
  raise "last must be INSERT schema_migrations" unless stmts.last.match?(/\bINSERT\s+INTO\s+analytics\.schema_migrations\b/i)
  stmts.each_with_index do |s, i|
    assert_single_statement!(s, label: "analytics[#{i}]")
  end
  # Round-trip fingerprint: joined essential content still contains all CREATE TABLE names.
  joined = stmts.join("\n")
  %w[schema_migrations projection_generations active_generation gameplay_metrics tournament_statistics rating_statistics processed_events].each do |t|
    raise "missing table #{t} after split" unless joined.include?("analytics.#{t}")
  end
end

check(failures, "cli-writes-one-file-per-statement") do
  raise "missing CLI #{BIN}" unless File.file?(BIN)
  Dir.mktmpdir("ch-split-") do |dir|
    ok = system(RbConfig.ruby, BIN, ANALYTICS_MIGRATION, dir)
    raise "CLI exited non-zero" unless ok
    files = Dir.children(dir).sort
    raise "files=#{files.inspect}" unless files.size == 9
    files.each_with_index do |name, i|
      body = File.read(File.join(dir, name))
      assert_single_statement!(body, label: "cli-file[#{i}]")
    end
  end
end

# Structural: bootstrap must split + POST per statement; never full-file body or multiquery=1.
check(failures, "bootstrap-posts-split-statements-not-full-file") do
  src = File.read(CH_BOOT)
  raise "must invoke split-clickhouse-sql" unless src.include?("split-clickhouse-sql")
  raise "must not POST full migration file via --data-binary @MIGRATION_FILE" if src.match?(/--data-binary\s+@"\$\{MIGRATION_FILE\}"/)
  # Reject actual query-param use; allow prose that says not to use it.
  if src.lines.any? { |l| !l.lstrip.start_with?("#") && l.include?("multiquery=1") }
    raise "must not use unsupported multiquery=1"
  end
  raise "must stop on first statement failure (set -e / explicit)" unless src.match?(/^set -euo pipefail$/)
  # Nested $(cat …) in the curl/ch_query argument masks read failure (function still runs).
  active = src.lines.reject { |l| l.lstrip.start_with?("#") }.join
  if active.match?(/ch_query_bootstrap\s+"\$\(cat\b/)
    raise "must not nest $(cat …) inside ch_query_bootstrap/curl argument"
  end
  unless active.match?(/stmt="\$\(cat\s+"\$\{stmt_file\}"\)"\s*\|\|\s*/)
    raise "must read each statement via guarded assignment (stmt=\"$(cat …)\" || …)"
  end
  unless active.include?('[[ -z "${stmt}" ]]') || active.include?("[[ -z \"${stmt}\" ]]") ||
         active.match?(/\[\[\s+-z\s+"\$\{?stmt\}?"\s*\]\]/)
    raise "must reject empty statement after guarded read"
  end
  unless active.match?(/ch_query_bootstrap\s+"\$\{?stmt\}?"/)
    raise "must pass stmt variable to ch_query_bootstrap (not nested cat)"
  end
end

if failures.empty?
  puts "ok clickhouse_sql_split_test"
  exit 0
end

warn "\n#{failures.size} clickhouse_sql_split failure(s)"
failures.each { |f| warn "  - #{f}" }
exit 1
