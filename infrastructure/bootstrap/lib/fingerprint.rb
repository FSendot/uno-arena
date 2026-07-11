# frozen_string_literal: true

# Deterministic schema fingerprint from embedded migration SQL.
# Strategy (explicit, offline-testable):
# - checksum = SHA-256 of the migration file bytes
# - tables / views / materialized views = CREATE TABLE / VIEW / MATERIALIZED VIEW names
# - sequences = SERIAL/BIGSERIAL columns → {table}_{column}_seq
# - indexes = explicitly named CREATE [UNIQUE] INDEX statements only
# - infrastructure adds schema_bootstrap_meta after apply; included in expected tables
# Exact runtime state = version + checksum + exact table/view/matview/sequence sets +
# exact explicit index set (full equality: unexpected and missing both fail).
# Runtime catalog discovery includes ordinary (i) and partitioned (I) indexes.
# Constraint-backed indexes are excluded at runtime by joining pg_constraint.conindid
# to pg_index.indexrelid (OID), never by *_pkey / *_key name suffixes.

require "digest"
require "json"

module BootstrapFingerprint
  META_TABLE = "schema_bootstrap_meta"
  VERSION = "001_init"

  module_function

  def sha256_file(path)
    Digest::SHA256.hexdigest(File.binread(path))
  end

  def scan_serials(line, table, sequences)
    line.scan(/\b(\w+)\s+(BIGSERIAL|SERIAL)\b/i) do |col, _|
      sequences << "#{table}_#{col}_seq"
    end
  end

  def parse_postgres_migration(sql, context:)
    tables = []
    views = []
    materialized_views = []
    indexes = []
    sequences = []
    current_table = nil
    depth = 0
    in_table = false

    sql.each_line do |raw|
      line = raw.gsub(/--.*/, "")
      if !in_table && (m = line.match(/\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:public\.)?(\w+)/i))
        current_table = m[1]
        tables << current_table
        in_table = true
        depth = line.count("(") - line.count(")")
        scan_serials(line, current_table, sequences)
        if depth <= 0 && line.include?(";")
          in_table = false
          current_table = nil
        end
        next
      end
      if in_table
        depth += line.count("(") - line.count(")")
        scan_serials(line, current_table, sequences)
        if depth <= 0 && line.include?(";")
          in_table = false
          current_table = nil
        end
        next
      end
      if (m = line.match(/\bCREATE\s+MATERIALIZED\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:public\.)?(\w+)/i))
        materialized_views << m[1]
        next
      end
      if (m = line.match(/\bCREATE\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:public\.)?(\w+)/i))
        views << m[1]
        next
      end
      if (m = line.match(/\bCREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)/i))
        indexes << m[1]
      end
    end

    {
      "engine" => "postgres",
      "context" => context,
      "version" => VERSION,
      "schema" => "public",
      "tables" => (tables + [META_TABLE]).uniq.sort,
      "views" => views.uniq.sort,
      "materialized_views" => materialized_views.uniq.sort,
      "sequences" => sequences.uniq.sort,
      "indexes" => indexes.uniq.sort
    }
  end

  def parse_clickhouse_migration(sql, context:)
    tables = sql.scan(/\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?analytics\.(\w+)/i).flatten
    views = sql.scan(/\bCREATE\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?analytics\.(\w+)/i).flatten
    {
      "engine" => "clickhouse",
      "context" => context,
      "version" => VERSION,
      "database" => "analytics",
      "tables" => (tables + [META_TABLE]).uniq.sort,
      "views" => views.uniq.sort
    }
  end

  def fingerprint_for(path, context:, engine:)
    sql = File.read(path)
    base =
      if engine == "postgres"
        parse_postgres_migration(sql, context: context)
      else
        parse_clickhouse_migration(sql, context: context)
      end
    base.merge("checksum" => sha256_file(path))
  end

  def write_json(path, doc)
    File.write(path, JSON.pretty_generate(doc) + "\n")
  end
end
