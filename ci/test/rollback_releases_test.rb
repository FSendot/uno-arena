# frozen_string_literal: true

require "fileutils"
require_relative "test_helper"

class RollbackReleasesTest < Minitest::Test
  def test_failure_before_promotion_is_a_no_snapshot_noop_without_credentials
    Dir.mktmpdir do |dir|
      FileUtils.mkdir_p(File.join(dir, "artifacts"))
      stdout, stderr, status = Open3.capture3("bash", File.expand_path("../bin/rollback-releases", __dir__), chdir: dir)
      assert status.success?, stderr
      assert_match(/rollback is not required/, stdout)
    end
  end

  def test_restored_inventory_is_waited_and_evidenced_even_when_remote_already_matches
    Dir.mktmpdir do |dir|
      FileUtils.cp_r("ci", dir)
      FileUtils.cp_r("environments", dir)
      FileUtils.ln_s(File.expand_path("services"), File.join(dir, "services"))
      FileUtils.ln_s(File.expand_path("infrastructure"), File.join(dir, "infrastructure"))
      FileUtils.cp_r("client-checkpoint", dir)
      prepare_released_gateway(dir)
      backup = File.join(dir, "artifacts/rollback/local-production")
      FileUtils.mkdir_p(backup)
      FileUtils.cp(File.join(dir, "environments/local-production/services/gateway.yaml"), File.join(backup, "gateway.yaml"))

      bin = File.join(dir, "fake-bin")
      FileUtils.mkdir_p(bin)
      git_log = File.join(dir, "git.log")
      curl_log = File.join(dir, "curl.log")
      File.write(File.join(bin, "git"), <<~SH)
        #!/usr/bin/env bash
        printf '%s\n' "$*" >>"$GIT_LOG"
        exit 0
      SH
      File.write(File.join(bin, "curl"), <<~SH)
        #!/usr/bin/env bash
        printf '%s\n' "$*" >>"$CURL_LOG"
        body='{"status":"ok","service":"gateway"}'
        case "$*" in
          */resource-tree) body='{"nodes":[{"kind":"Deployment","health":{"status":"Healthy"}}]}' ;;
          *api/v1/applications*) cat "$ARGO_RESPONSE"; exit 0 ;;
        esac
        output=''; previous=''
        for argument in "$@"; do
          if [ "$previous" = '--output' ]; then output="$argument"; fi
          previous="$argument"
        done
        if [ -n "$output" ]; then printf '%s\n' "$body" >"$output"; printf '200'; else printf '%s\n' "$body"; fi
      SH
      File.chmod(0o755, File.join(bin, "git"), File.join(bin, "curl"))
      behavior_log = File.join(dir, "behavior.log")
      parity = File.join(dir, "client-checkpoint/tests/run-live-client-parity.sh")
      File.write(parity, <<~SH)
        #!/usr/bin/env bash
        printf '%s %s\n' "$UNOARENA_API_URL" "$CURL_CA_BUNDLE" >>"$BEHAVIOR_LOG"
      SH
      File.chmod(0o755, parity)
      ca = File.join(dir, "ca.pem")
      File.write(ca, "ca")
      response = File.join(dir, "argo-response.json")
      File.write(response, JSON.generate(argo_gateway_response))
      env = {
        "PATH" => "#{bin}:#{ENV.fetch('PATH')}", "GIT_LOG" => git_log, "CURL_LOG" => curl_log,
        "BEHAVIOR_LOG" => behavior_log,
        "ARGO_RESPONSE" => response, "GITOPS_PUSH_TOKEN" => "token", "CI_SERVER_HOST" => "gitlab.test",
        "CI_PROJECT_PATH" => "group/project", "PROMOTE_COMPONENTS" => "gateway",
        "DEPLOY_ENVIRONMENTS" => "local-production", "GITLAB_USER_EMAIL" => "ci@test",
        "GITLAB_USER_NAME" => "CI", "ARGOCD_WAIT_SECONDS" => "1",
        "ARGOCD_LOCAL_PRODUCTION_SERVER" => "https://argocd.local.test",
        "ARGOCD_LOCAL_PRODUCTION_AUTH_TOKEN" => "argo-token", "ARGOCD_LOCAL_PRODUCTION_CA_FILE" => ca,
        "ARGOCD_APPLICATIONS_LOCAL_PRODUCTION" => "uno-arena-local-production-gateway",
        "ARGOCD_APPLICATIONS_PRODUCTION" => "", "POST_DEPLOY_LOCAL_PRODUCTION_GATEWAY_URL" => "https://uno-arena.local:8443/health",
        "POST_DEPLOY_LOCAL_PRODUCTION_GATEWAY_BASE_URL" => "https://uno-arena.local:8443",
        "POST_DEPLOY_LOCAL_PRODUCTION_CA_FILE" => ca
      }
      stdout, stderr, status = Open3.capture3(env, "bash", "ci/bin/rollback-releases", chdir: dir)
      assert status.success?, "#{stdout}\n#{stderr}"
      assert_match(/already matches/, stdout)
      calls = File.readlines(curl_log, chomp: true)
      assert calls.any? { |call| call.include?("/api/v1/applications/uno-arena-local-production-gateway") }
      assert calls.any? { |call| call.include?("/resource-tree") }
      assert calls.any? { |call| call.include?("https://uno-arena.local:8443/health") }
      assert_equal ["https://uno-arena.local:8443 #{ca}"], File.readlines(behavior_log, chomp: true)
    end
  end

  private

  def prepare_released_gateway(dir)
    path = File.join(dir, "environments/local-production/services/gateway.yaml")
    document = YAML.safe_load(File.read(path), [], [], true)
    document["enabled"] = true
    document["status"] = "released"
    document["chart"]["repository"] = "https://charts.test"
    document["chart"]["version"] = "0.1.42"
    document["chart"]["packageSha256"] = "c" * 64
    document["image"] = { "repository" => "registry.test/gateway", "digest" => "sha256:#{'a' * 64}" }
    document["values"]["repository"] = "https://git.test/project.git"
    document["values"]["revision"] = "b" * 40
    File.write(path, YAML.dump(document))
    enabled = File.join(dir, "environments/local-production/services/enabled")
    FileUtils.mkdir_p(enabled)
    FileUtils.cp(path, File.join(enabled, "gateway.yaml"))
  end

  def argo_gateway_response
    {
      "metadata" => { "name" => "uno-arena-local-production-gateway" },
      "spec" => { "sources" => [
        { "repoURL" => "https://charts.test", "chart" => "gateway", "targetRevision" => "0.1.42",
          "helm" => { "values" => YAML.dump("image" => { "repository" => "registry.test/gateway", "digest" => "sha256:#{'a' * 64}" }) } },
        { "repoURL" => "https://git.test/project.git", "targetRevision" => "b" * 40, "ref" => "values" }
      ] },
      "status" => { "sync" => { "status" => "Synced", "revisions" => ["0.1.42", "b" * 40] },
                    "health" => { "status" => "Healthy" } }
    }
  end
end
