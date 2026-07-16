# frozen_string_literal: true

require_relative "test_helper"

class PostDeployEvidenceTest < Minitest::Test
  POST_DEPLOY = File.expand_path("../bin/post-deploy-evidence", __dir__)

  def test_internal_component_uses_its_argo_resource_tree_without_public_exposure
    Dir.mktmpdir do |dir|
      env, log = evidence_environment(dir, "uno-arena-local-production-platform-secrets")
      stdout, stderr, status = Open3.capture3(env, "ruby", POST_DEPLOY, chdir: dir)
      assert status.success?, "#{stdout}\n#{stderr}"
      calls = File.readlines(log, chomp: true)
      assert_equal 1, calls.length
      assert_includes calls.first, "/api/v1/applications/uno-arena-local-production-platform-secrets/resource-tree"
    end
  end

  def test_gateway_uses_environment_specific_https_health_evidence
    Dir.mktmpdir do |dir|
      env, log = evidence_environment(dir, "uno-arena-local-production-gateway")
      edge_ca = File.join(dir, "edge-ca.pem")
      File.write(edge_ca, "edge ca")
      env["POST_DEPLOY_LOCAL_PRODUCTION_GATEWAY_URL"] = "https://uno-arena.local:8443/health"
      env["POST_DEPLOY_LOCAL_PRODUCTION_GATEWAY_BASE_URL"] = "https://uno-arena.local:8443"
      env["POST_DEPLOY_LOCAL_PRODUCTION_CA_FILE"] = edge_ca
      stdout, stderr, status = Open3.capture3(env, "ruby", POST_DEPLOY, chdir: dir)
      assert status.success?, "#{stdout}\n#{stderr}"
      calls = File.readlines(log, chomp: true)
      assert_equal 2, calls.length
      health_call = calls.find { |call| call.include?("https://uno-arena.local:8443/health") }
      assert_includes health_call, "--cacert #{edge_ca}"
      behavior = File.readlines(env.fetch("BEHAVIOR_LOG"), chomp: true)
      assert_equal ["https://uno-arena.local:8443 #{edge_ca}"], behavior
    end
  end

  def test_degraded_component_resource_fails_evidence
    Dir.mktmpdir do |dir|
      env, = evidence_environment(dir, "uno-arena-local-production-kafka", health: "Degraded")
      _stdout, stderr, status = Open3.capture3(env, "ruby", POST_DEPLOY, chdir: dir)
      refute status.success?
      assert_match(/resource tree is unhealthy/, stderr)
    end
  end

  def test_observability_fails_when_evidence_job_is_missing
    Dir.mktmpdir do |dir|
      env, = evidence_environment(dir, "uno-arena-local-production-observability")
      _stdout, stderr, status = Open3.capture3(env, "ruby", POST_DEPLOY, chdir: dir)
      refute status.success?
      assert_match(/missing observability post-sync evidence Job/, stderr)
    end
  end

  def test_observability_fails_when_evidence_job_has_not_succeeded
    Dir.mktmpdir do |dir|
      env, = evidence_environment(dir, "uno-arena-local-production-observability", evidence_job: :failed)
      _stdout, stderr, status = Open3.capture3(env, "ruby", POST_DEPLOY, chdir: dir)
      refute status.success?
      assert_match(/evidence Job did not report successful completion/, stderr)
    end
  end

  def test_observability_accepts_exact_successful_evidence_job
    Dir.mktmpdir do |dir|
      env, log = evidence_environment(dir, "uno-arena-local-production-observability", evidence_job: :succeeded)
      stdout, stderr, status = Open3.capture3(env, "ruby", POST_DEPLOY, chdir: dir)
      assert status.success?, "#{stdout}\n#{stderr}"
      assert_equal 1, File.readlines(log, chomp: true).length
    end
  end

  private

  def evidence_environment(dir, application, health: "Healthy", evidence_job: nil)
    ca = File.join(dir, "argo-ca.pem")
    log = File.join(dir, "curl.log")
    behavior_log = File.join(dir, "behavior.log")
    nodes = [{"kind" => "Deployment", "health" => {"status" => health}}]
    if evidence_job
      message = evidence_job == :succeeded ? "Job completed successfully" : "Job has not completed"
      info = evidence_job == :succeeded ? [{"name" => "status", "value" => "Succeeded"}] : []
      nodes << {
        "group" => "batch", "kind" => "Job", "namespace" => "observability",
        "name" => "uno-arena-observability-postsync-evidence",
        "health" => {"status" => "Healthy", "message" => message}, "info" => info
      }
    end
    resource_tree = JSON.generate("nodes" => nodes)
    File.write(ca, "argo ca")
    File.write(File.join(dir, "curl"), <<~SH)
      #!/usr/bin/env bash
      printf '%s\n' "$*" >>"$CURL_LOG"
      body='{"status":"ok","service":"gateway"}'
      case "$*" in
        */resource-tree) body='#{resource_tree}' ;;
      esac
      output=''
      previous=''
      for argument in "$@"; do
        if [ "$previous" = '--output' ]; then output="$argument"; fi
        previous="$argument"
      done
      if [ -n "$output" ]; then
        printf '%s\n' "$body" >"$output"
        printf '200'
      else
        printf '%s\n' "$body"
      fi
    SH
    File.chmod(0o755, File.join(dir, "curl"))
    parity_dir = File.join(dir, "client-checkpoint", "tests")
    FileUtils.mkdir_p(parity_dir)
    parity = File.join(parity_dir, "run-live-client-parity.sh")
    File.write(parity, <<~SH)
      #!/usr/bin/env bash
      printf '%s %s\n' "$UNOARENA_API_URL" "$CURL_CA_BUNDLE" >>"$BEHAVIOR_LOG"
    SH
    File.chmod(0o755, parity)
    env = {
      "PATH" => "#{dir}:#{ENV.fetch('PATH')}", "CURL_LOG" => log, "BEHAVIOR_LOG" => behavior_log,
      "DEPLOY_ENVIRONMENTS" => "local-production",
      "ARGOCD_LOCAL_PRODUCTION_SERVER" => "https://argocd.local.test",
      "ARGOCD_LOCAL_PRODUCTION_AUTH_TOKEN" => "token",
      "ARGOCD_LOCAL_PRODUCTION_CA_FILE" => ca,
      "ARGOCD_APPLICATIONS_LOCAL_PRODUCTION" => application
    }
    [env, log]
  end
end
