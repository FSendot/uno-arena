# frozen_string_literal: true

require "digest"
require "json"
require "minitest/autorun"
require "open3"
require "tmpdir"

class MigrationPlanTest < Minitest::Test
  SCRIPT = File.expand_path("../bin/migration-plan.rb", __dir__)

  def fixture
    Dir.mktmpdir do |dir|
      File.write(File.join(dir, "001_init.sql"), "select 1;\n")
      File.write(File.join(dir, "002_expand.sql"), "alter table example add column value text;\n")
      yield dir
    end
  end

  def run_plan(dir, rows = [])
    ledger = File.join(dir, "ledger.tsv")
    File.write(ledger, rows.join("\n") + (rows.empty? ? "" : "\n"))
    stdout, stderr, status = Open3.capture3("ruby", SCRIPT, dir, ledger)
    [stdout, stderr, status]
  end

  def test_nonempty_v1_has_only_v2_pending
    fixture do |dir|
      sha = Digest::SHA256.file(File.join(dir, "001_init.sql")).hexdigest
      stdout, stderr, status = run_plan(dir, ["001_init\t#{sha}"])
      assert status.success?, stderr
      assert_equal ["002_expand"], JSON.parse(stdout).fetch("pending").map { |row| row.fetch("version") }
    end
  end

  def test_exact_v2_is_noop
    fixture do |dir|
      rows = %w[001_init.sql 002_expand.sql].map do |file|
        "#{File.basename(file, '.sql')}\t#{Digest::SHA256.file(File.join(dir, file)).hexdigest}"
      end
      stdout, stderr, status = run_plan(dir, rows)
      assert status.success?, stderr
      assert_empty JSON.parse(stdout).fetch("pending")
    end
  end

  def test_checksum_drift_fails_closed
    fixture do |dir|
      _stdout, stderr, status = run_plan(dir, ["001_init\t#{'0' * 64}"])
      refute status.success?
      assert_includes stderr, "checksum drift"
    end
  end

  def test_non_prefix_history_fails_closed
    fixture do |dir|
      _stdout, stderr, status = run_plan(dir, ["002_expand\t"])
      refute status.success?
      assert_includes stderr, "exact release prefix"
    end
  end

  def test_incremental_migration_cannot_modify_runner_owned_ledger
    fixture do |dir|
      File.write(
        File.join(dir, "002_expand.sql"),
        "insert into schema_migrations(version) values ('002_expand');\n"
      )
      _stdout, stderr, status = run_plan(dir)
      refute status.success?
      assert_includes stderr, "runner-owned migration ledger"
    end
  end
end
