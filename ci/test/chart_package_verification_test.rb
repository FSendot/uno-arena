# frozen_string_literal: true

require "fileutils"
require_relative "test_helper"

class ChartPackageVerificationTest < Minitest::Test
  def test_refetched_package_bytes_must_match_promoted_digest
    Dir.mktmpdir do |dir|
      package = File.join(dir, "identity.tgz")
      File.write(package, "immutable package bytes\n")
      sha = Digest::SHA256.file(package).hexdigest
      expectations = File.join(dir, "expectations.json")
      File.write(expectations, JSON.generate(
        "applications" => {"production" => {}, "local-production" => {
          "uno-arena-local-production-identity" => {"chart" => {
            "repository" => "https://charts.test/stable", "name" => "identity",
            "version" => "0.1.42", "packageSha256" => sha
          }}
        }}
      ))
      fake_bin = File.join(dir, "bin")
      FileUtils.mkdir_p(fake_bin)
      File.write(File.join(fake_bin, "curl"), <<~SH)
        #!/usr/bin/env bash
        output=''; previous=''
        for argument in "$@"; do
          if [ "$previous" = '--output' ]; then output="$argument"; fi
          previous="$argument"
        done
        cp "$PACKAGE_FIXTURE" "$output"
      SH
      File.chmod(0o755, File.join(fake_bin, "curl"))
      env = {
        "PATH" => "#{fake_bin}:#{ENV.fetch('PATH')}", "PACKAGE_FIXTURE" => package,
        "CI_JOB_TOKEN" => "test_token", "ARGOCD_EXPECTATIONS_FILE" => expectations
      }
      stdout, stderr, status = Open3.capture3(env, "ci/bin/verify-chart-packages")
      assert status.success?, "#{stdout}\n#{stderr}"

      File.write(package, "mutated package bytes\n")
      _stdout, stderr, status = Open3.capture3(env, "ci/bin/verify-chart-packages")
      refute status.success?
      assert_match(/checksum mismatch/, stderr)
    end
  end
end
