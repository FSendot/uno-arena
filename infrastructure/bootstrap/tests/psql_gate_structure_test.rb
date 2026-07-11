#!/usr/bin/env ruby
# frozen_string_literal: true

# Offline structure checks for the Postgres bootstrap psql gate script.
# No database connection — validates boolean \\gset branching, lock/transaction
# structure, ownership-on-exact, OID-based index exclusion, partitioned indexes (I),
# and that the initial exact/no-op gate itself holds full catalog equality
# (leaving equality only in post-apply must fail).

require "pathname"

ROOT = File.expand_path("../../..", __dir__)
PG_BOOT = File.join(ROOT, "infrastructure/bootstrap/bin/bootstrap-postgres.sh")

failures = []

def fail_collect(failures, msg)
  failures << msg
  warn "FAIL: #{msg}"
end

src = File.read(PG_BOOT)

RELKIND_OWNERSHIP = /relkind\s+IN\s*\(\s*'r'\s*,\s*'p'\s*,\s*'v'\s*,\s*'m'\s*,\s*'S'\s*,\s*'i'\s*,\s*'I'\s*\)/
RELKIND_INDEXES = /relkind\s+IN\s*\(\s*'i'\s*,\s*'I'\s*\)/

GATE_EQUALITY_SNIPPETS = [
  "table_names IS NOT DISTINCT FROM expected_tables",
  "view_names IS NOT DISTINCT FROM expected_views",
  "matview_names IS NOT DISTINCT FROM expected_matviews",
  "sequence_names IS NOT DISTINCT FROM expected_sequences",
  "index_names IS NOT DISTINCT FROM expected_indexes"
].freeze

def extract_gate_do(src)
  src[/\bDO \\\$\\\$.*?INSERT INTO _bootstrap_gate/m]
end

def gate_has_full_equality?(gate_do)
  return false if gate_do.nil?

  GATE_EQUALITY_SNIPPETS.all? { |s| gate_do.include?(s) } &&
    gate_do.match?(RELKIND_INDEXES) &&
    gate_do.match?(RELKIND_OWNERSHIP) &&
    gate_do.include?("conindid") &&
    gate_do.include?("action := 'noop'")
end

# Expression-style \\if is invalid: psql wants a boolean, not `'x' = 'y'`.
expression_if = /\\if\s+:'[^']+'\s*=/
fail_collect(failures, "postgres gate must not use expression-style \\if (boolean \\gset vars only)") if src.match?(expression_if)

%w[do_apply do_exact do_fail].each do |var|
  fail_collect(failures, "postgres gate must \\gset boolean #{var}") unless src.match?(/AS\s+#{var}\b/i) && src.include?("\\gset")
  fail_collect(failures, "postgres gate must \\if :#{var}") unless src.match?(/\\if\s+:#{var}\b/) || src.match?(/\\elif\s+:#{var}\b/)
end

# Exactly one branch selector each; apply / exact / fail are mutually exclusive.
fail_collect(failures, "postgres gate missing \\if :do_apply") unless src.match?(/\\if\s+:do_apply\b/)
fail_collect(failures, "postgres gate missing \\elif :do_exact") unless src.match?(/\\elif\s+:do_exact\b/)
fail_collect(failures, "postgres gate missing \\elif :do_fail") unless src.match?(/\\elif\s+:do_fail\b/)
fail_collect(failures, "postgres gate missing \\endif") unless src.include?("\\endif")

# Lock + single-transaction structure.
fail_collect(failures, "postgres must use --single-transaction") unless src.include?("--single-transaction")
fail_collect(failures, "postgres must use pg_advisory_xact_lock") unless src.include?("pg_advisory_xact_lock")
lock_idx = src.index("pg_advisory_xact_lock")
ensure_idx = src.index("postgres_ensure_roles.sql")
fail_collect(failures, "advisory lock must precede role ensure") unless lock_idx && ensure_idx && lock_idx < ensure_idx
pre_gate = src.split("--single-transaction").first
if pre_gate&.include?("postgres_ensure_roles.sql")
  fail_collect(failures, "role ensure must not run before single-transaction gate")
end

gate_do = extract_gate_do(src)
if gate_do
  unless gate_has_full_equality?(gate_do)
    fail_collect(failures, "gate DO must hold full table/view/matview/sequence/index equality (incl. I), ownership, and OID exclusion before noop")
  end
  noop_in_gate = gate_do.index("action := 'noop'")
  owner_in_gate = gate_do.index("relkind IN ('r', 'p', 'v', 'm', 'S', 'i', 'I')")
  fail_collect(failures, "ownership check must precede noop in gate DO") unless owner_in_gate && noop_in_gate && owner_in_gate < noop_in_gate
  # Indexes must count toward empty/drift nonempty detection inside the gate.
  unless gate_do.match?(/user_object_count.*array_length\(index_names/m) ||
         (gate_do.include?("array_length(index_names") && gate_do.include?("user_object_count"))
    fail_collect(failures, "gate DO must include index_names in empty/drift user_object_count")
  end
else
  fail_collect(failures, "missing gate DO block before _bootstrap_gate insert")
end

# Index equality excludes constraint-backed indexes by OID join, not name suffix.
fail_collect(failures, "index exclusion must join pg_constraint.conindid") unless src.include?("pg_constraint") && src.include?("conindid")
fail_collect(failures, "index exclusion must not use _pkey|_key name suffix") if src.match?(/_\(pkey\|key\)/) || src.include?("_(pkey|key)$")
fail_collect(failures, "must not filter indexes by name ~ '_pkey|_key'") if src.match?(/indexname\s+!~/)
fail_collect(failures, "index catalog must include partitioned relkind I") unless src.match?(RELKIND_INDEXES)
fail_collect(failures, "ownership must include partitioned index relkind I") unless src.match?(RELKIND_OWNERSHIP)

# Materialized views in empty/drift/exact.
fail_collect(failures, "postgres gate must query pg_matviews") unless src.include?("pg_matviews")
fail_collect(failures, "postgres gate must compare matview_names") unless src.include?("matview_names IS NOT DISTINCT FROM expected_matviews") || src.include?("matview_names IS DISTINCT FROM expected_matviews")
fail_collect(failures, "postgres must source EXPECTED_MATERIALIZED_VIEWS") unless src.include?("EXPECTED_MATERIALIZED_VIEWS")

# Metadata cardinality / version / checksum on exact.
fail_collect(failures, "exact must require version_count <> 1 OR meta_count <> 1") unless src.include?("version_count <> 1") && src.include?("meta_count <> 1")
fail_collect(failures, "exact must compare checksum") unless src.include?("found_checksum IS NOT DISTINCT FROM")

# Static bad fixture must be detected by the same expression-style predicate.
bad_fixture = <<~BAD
  SELECT action AS bootstrap_action FROM _bootstrap_gate \\gset
  \\if :'bootstrap_action' = 'apply'
  \\echo apply
  \\elif :'bootstrap_action' = 'noop'
  \\echo noop
  \\endif
BAD
fail_collect(failures, "fixture: expression-style \\if detector inert") unless bad_fixture.match?(expression_if)

# Bad ownership-only-post-apply fixture model.
bad_ownership = <<~BAD
  IF table_names IS NOT DISTINCT FROM expected_tables THEN
    action := 'noop';
  END IF;
  -- ownership only later in post-apply
BAD
gate_accepts_without_owner = bad_ownership.include?("action := 'noop'") && !bad_ownership.include?("relkind IN")
fail_collect(failures, "fixture: ownership-before-noop detector inert") unless gate_accepts_without_owner

# Bad name-suffix index exclusion fixture.
bad_index = "WHERE schemaname = 'public' AND indexname !~ '_(pkey|key)$'"
fail_collect(failures, "fixture: name-suffix index detector inert") unless bad_index.match?(/_\(pkey\|key\)/) || bad_index.include?("_(pkey|key)$")

# Bad: catalog equality / ownership / I / OID exclusion only in post-apply, not the gate.
bad_post_apply_only = <<~BAD
  DO \\$\\$
  DECLARE
    action text;
  BEGIN
    IF found_version IS NOT DISTINCT FROM '001_init' THEN
      action := 'noop';
    END IF;
    INSERT INTO _bootstrap_gate(action) VALUES (action);
  END
  \\$\\$;

  -- post-apply only (must NOT satisfy gate_has_full_equality?)
  DO \\$\\$
  BEGIN
    IF table_names IS NOT DISTINCT FROM expected_tables
       AND view_names IS NOT DISTINCT FROM expected_views
       AND matview_names IS NOT DISTINCT FROM expected_matviews
       AND sequence_names IS NOT DISTINCT FROM expected_sequences
       AND index_names IS NOT DISTINCT FROM expected_indexes THEN
      NULL;
    END IF;
    FOR obj IN SELECT 1 WHERE false AND relkind IN ('r', 'p', 'v', 'm', 'S', 'i', 'I') LOOP NULL; END LOOP;
    PERFORM 1 FROM pg_constraint WHERE conindid IS NOT NULL;
    PERFORM 1 WHERE relkind IN ('i', 'I');
  END
  \\$\\$;
BAD
bad_gate = extract_gate_do(bad_post_apply_only)
fail_collect(failures, "fixture: post-apply-only equality detector inert") if gate_has_full_equality?(bad_gate)

# Unexpected partitioned index omitted when only relkind = 'i' (no I).
bad_no_I = <<~BAD
  SELECT coalesce(array_agg(i.relname ORDER BY i.relname), ARRAY[]::text[])
    INTO index_names
    FROM pg_catalog.pg_class i
   WHERE i.relkind = 'i';
BAD
fail_collect(failures, "fixture: missing-I index detector inert") if bad_no_I.match?(RELKIND_INDEXES)

if failures.empty?
  puts "ok psql_gate_structure_test"
  exit 0
end

warn "\n#{failures.size} psql gate structure failure(s)"
exit 1
