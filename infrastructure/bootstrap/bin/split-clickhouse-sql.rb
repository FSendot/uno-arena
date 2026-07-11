#!/usr/bin/env ruby
# frozen_string_literal: true

# Split a ClickHouse migration into one .sql file per statement.
# Usage: split-clickhouse-sql.rb INPUT.sql OUTDIR
# Exit non-zero on unterminated quotes/comments or empty/invalid output.

require_relative "../lib/clickhouse_sql_split"

if ARGV.length != 2
  warn "usage: split-clickhouse-sql.rb INPUT.sql OUTDIR"
  exit 2
end

input, outdir = ARGV
unless File.file?(input)
  warn "input missing: #{input}"
  exit 1
end

begin
  n = ClickhouseSqlSplit.write_statement_files(File.read(input), outdir)
  warn "split-clickhouse-sql: #{n} statement(s) -> #{outdir}" if ENV["SPLIT_CLICKHOUSE_SQL_VERBOSE"]
rescue ClickhouseSqlSplit::Error => e
  warn "split-clickhouse-sql: #{e.message}"
  exit 1
end
