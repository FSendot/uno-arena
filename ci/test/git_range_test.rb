# frozen_string_literal: true

require_relative "test_helper"

class GitRangeTest < Minitest::Test
  def test_reads_exact_changed_paths_from_push_range
    Dir.mktmpdir do |dir|
      run!(dir, "git", "init", "-q")
      run!(dir, "git", "config", "user.email", "ci@example.invalid")
      run!(dir, "git", "config", "user.name", "CI")
      File.write(File.join(dir, "one"), "one\n")
      run!(dir, "git", "add", "one")
      run!(dir, "git", "commit", "-qm", "one")
      base = run!(dir, "git", "rev-parse", "HEAD").strip
      File.write(File.join(dir, "two"), "two\n")
      run!(dir, "git", "add", "two")
      run!(dir, "git", "commit", "-qm", "two")
      head = run!(dir, "git", "rev-parse", "HEAD").strip

      env = { "CI_PIPELINE_SOURCE" => "push", "CI_COMMIT_BEFORE_SHA" => base, "CI_COMMIT_SHA" => head }
      assert_equal ["two"], UnoArenaCI::GitRange.new(env, repository: dir).changed_paths
    end
  end

  def test_web_has_no_diff_and_zero_base_fails
    assert_equal [], UnoArenaCI::GitRange.new({ "CI_PIPELINE_SOURCE" => "web" }).changed_paths
    env = { "CI_PIPELINE_SOURCE" => "push", "CI_COMMIT_BEFORE_SHA" => "0" * 40, "CI_COMMIT_SHA" => "a" * 40 }
    assert_raises(UnoArenaCI::ImpactError) { UnoArenaCI::GitRange.new(env).changed_paths }
  end

  private

  def run!(dir, *command)
    stdout, stderr, status = Open3.capture3(*command, chdir: dir)
    raise stderr unless status.success?
    stdout
  end
end
