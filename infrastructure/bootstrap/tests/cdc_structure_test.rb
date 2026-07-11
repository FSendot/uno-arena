#!/usr/bin/env ruby
# frozen_string_literal: true

# Offline structure checks for CDC role/publication SQL and bootstrap wiring.
# No database connection.

require "pathname"

ROOT = File.expand_path("../../..", __dir__)
PG_BOOT = File.join(ROOT, "infrastructure/bootstrap/bin/bootstrap-postgres.sh")
CDC_APPLY = File.join(ROOT, "infrastructure/bootstrap/sql/postgres_cdc_apply.sql")
CDC_VERIFY = File.join(ROOT, "infrastructure/bootstrap/sql/postgres_cdc_verify.sql")
ENSURE = File.join(ROOT, "infrastructure/bootstrap/sql/postgres_ensure_roles.sql")
GRANT = File.join(ROOT, "infrastructure/bootstrap/sql/postgres_grant_runtime.sql")

failures = []

def fail_collect(failures, msg)
  failures << msg
  warn "FAIL: #{msg}"
end

[PG_BOOT, CDC_APPLY, CDC_VERIFY, ENSURE, GRANT].each do |path|
  fail_collect(failures, "missing #{path}") unless File.file?(path)
end

boot = File.read(PG_BOOT)
apply = File.read(CDC_APPLY)
verify = File.read(CDC_VERIFY)
ensure_roles = File.read(ENSURE)
grant_runtime = File.read(GRANT)

fail_collect(failures, "bootstrap must have standalone set -euo pipefail") unless boot.match?(/^set -euo pipefail$/)
fail_collect(failures, "bootstrap must not append set -euo to a comment (slots.set)") if boot.include?("slots.set")

%w[CDC_USER CDC_PASSWORD CDC_PUBLICATION CDC_TABLE].each do |var|
  fail_collect(failures, "bootstrap must require #{var}") unless boot.include?(": \"${#{var}:?}\"") || boot.include?("${#{var}:?}")
end
fail_collect(failures, "bootstrap must support optional CDC_REALTIME_*") unless boot.include?("CDC_REALTIME_USER")
fail_collect(failures, "bootstrap must apply CDC on do_apply") unless boot.include?("postgres_cdc_apply.sql")
fail_collect(failures, "bootstrap must verify CDC on apply and exact") unless boot.scan("postgres_cdc_verify.sql").size >= 2
exact = boot[/\\elif :do_exact.*?\\elif :do_fail/m]
if exact&.include?("postgres_cdc_apply.sql")
  fail_collect(failures, "exact path must not run postgres_cdc_apply")
end
fail_collect(failures, "exact path must verify CDC") unless exact&.include?("postgres_cdc_verify.sql")

fail_collect(failures, "CDC apply must CREATE ROLE LOGIN REPLICATION") unless apply.match?(/LOGIN\s+REPLICATION/)
fail_collect(failures, "CDC apply must GRANT SELECT on cdc_table only") unless apply.include?("GRANT SELECT ON TABLE")
fail_collect(failures, "CDC apply must CREATE PUBLICATION") unless apply.include?("CREATE PUBLICATION")
fail_collect(failures, "CDC apply must own publication as bootstrap_user") unless apply.match?(/ALTER PUBLICATION.*OWNER TO/m) && apply.include?(":'bootstrap_user'")
fail_collect(failures, "CDC apply must not transfer publication ownership to cdc_user") if apply.match?(/ALTER PUBLICATION.*OWNER TO %I',\s*pub,\s*cdc_role/m) || apply.match?(/OWNER TO %I',\s*:'cdc_publication',\s*:'cdc_user'/)
fail_collect(failures, "CDC apply must not grant database CREATE") if apply.match?(/GRANT\s+CREATE\s+ON\s+DATABASE/i)
fail_collect(failures, "CDC apply must revoke PUBLIC schema CREATE (PG14 default)") unless apply.match?(/REVOKE\s+CREATE\s+ON\s+SCHEMA\s+public\s+FROM\s+PUBLIC/i)
fail_collect(failures, "CDC apply must not default-GRANT sequence privileges") if apply.match?(/ALTER DEFAULT PRIVILEGES.*SEQUENCES/m)
fail_collect(failures, "CDC apply must not create logical slots") if apply.match?(/pg_create_logical_replication_slot|CREATE_REPLICATION_SLOT/i)
fail_collect(failures, "CDC verify must not create logical slots") if verify.match?(/pg_create_logical_replication_slot|CREATE_REPLICATION_SLOT/i)
fail_collect(failures, "bootstrap must not create logical slots") if boot.match?(/pg_create_logical_replication_slot|CREATE_REPLICATION_SLOT/i)

# psql :'vars' do not interpolate inside dollar-quoted DO — require set_config staging.
[apply, verify].each_with_index do |sql, idx|
  label = idx.zero? ? "apply" : "verify"
  fail_collect(failures, "CDC #{label} must stage DO inputs via set_config") unless sql.include?("set_config(")
  fail_collect(failures, "CDC #{label} DO must read current_setting") unless sql.include?("current_setting(")
  fail_collect(failures, "CDC #{label} set_config must be session-local (false)") unless sql.match?(/set_config\([^)]*,\s*false\s*\)/)
  do_bodies = sql.scan(/DO\s+\$\w*\$.*?\$\w*\$;/m)
  fail_collect(failures, "CDC #{label} must contain a DO block") if do_bodies.empty?
  do_bodies.each do |body|
    if body.match?(/:'[a-z_]+'/)
      fail_collect(failures, "CDC #{label} must not use psql :'vars' inside DO dollar quotes")
    end
  end
end

%w[
  uno_arena.cdc_user
  uno_arena.cdc_publication
  uno_arena.cdc_table
  uno_arena.cdc_peer_table
  uno_arena.bootstrap_user
].each do |key|
  fail_collect(failures, "CDC apply must set_config #{key}") unless apply.include?(key)
  fail_collect(failures, "CDC verify must set_config #{key}") unless verify.include?(key)
end
fail_collect(failures, "CDC verify must set_config runtime_user") unless verify.include?("uno_arena.runtime_user")

fail_collect(failures, "CDC verify must require LOGIN REPLICATION") unless verify.include?("rolreplication") && verify.include?("rolcanlogin")
fail_collect(failures, "CDC verify must reject runtime REPLICATION") unless verify.include?("runtime role") || verify.include?("must not have REPLICATION")
fail_collect(failures, "CDC verify must check publication table filter") unless verify.include?("pg_publication_tables")
fail_collect(failures, "CDC verify must enforce peer isolation") unless verify.include?("peer") && verify.include?("must not SELECT peer")
fail_collect(failures, "CDC verify must be SELECT-only on outbox") unless verify.include?("SELECT-only")
fail_collect(failures, "CDC verify must expect bootstrap publication owner") unless verify.include?("expected bootstrap role")
fail_collect(failures, "CDC verify must reject database CREATE on CDC role") unless verify.include?("must not have CREATE on database")

# Runtime / ensure roles must not grant REPLICATION.
fail_collect(failures, "ensure_roles must not grant REPLICATION") if ensure_roles.match?(/REPLICATION/i)
fail_collect(failures, "grant_runtime must not grant REPLICATION") if grant_runtime.match?(/REPLICATION/i)

# Default privileges must not blanket-GRANT SELECT on all tables to CDC.
if apply.match?(/ALTER DEFAULT PRIVILEGES.*GRANT SELECT ON TABLES/m)
  fail_collect(failures, "CDC must not default-GRANT SELECT on all tables")
end

if failures.empty?
  puts "ok cdc_structure_test"
  exit 0
end

warn "\n#{failures.size} CDC structure failure(s)"
exit 1
