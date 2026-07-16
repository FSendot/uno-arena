# frozen_string_literal: true

require "open3"
require_relative "impact_map"

module UnoArenaCI
  class GitRange
    ZERO_SHA = /\A0{40,64}\z/

    def initialize(env = ENV, repository: Dir.pwd)
      @env = env
      @repository = repository
    end

    def source
      @env.fetch("CI_PIPELINE_SOURCE", "")
    end

    def changed_paths
      return [] if source == "web"
      base = base_sha
      head = @env.fetch("CI_COMMIT_SHA", "").strip
      raise ImpactError, "CI_COMMIT_SHA is required" if head.empty?
      raise ImpactError, "cannot plan from an all-zero base SHA" if base.match?(ZERO_SHA)

      stdout, stderr, status = Open3.capture3("git", "diff", "--name-only", "-z", base, head, chdir: @repository)
      raise ImpactError, "git diff #{base}..#{head} failed: #{stderr.strip}" unless status.success?
      stdout.split("\0").reject(&:empty?)
    end

    private

    def base_sha
      candidates = if source == "merge_request_event"
                     [@env["CI_MERGE_REQUEST_DIFF_BASE_SHA"], @env["CI_COMMIT_BEFORE_SHA"]]
                   else
                     [@env["CI_COMMIT_BEFORE_SHA"]]
                   end
      base = candidates.compact.map(&:strip).find { |value| !value.empty? }
      raise ImpactError, "no base SHA is available for #{source}" unless base
      base
    end
  end
end
