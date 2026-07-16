# frozen_string_literal: true

require_relative "test_helper"

class WaitForArgocdTest < Minitest::Test
  def test_requires_environment_specific_ca_file
    env = {
      "DEPLOY_ENVIRONMENTS" => "production",
      "ARGOCD_PRODUCTION_SERVER" => "https://argocd.production.test",
      "ARGOCD_PRODUCTION_AUTH_TOKEN" => "production-token",
      "ARGOCD_APPLICATIONS_PRODUCTION" => "gateway"
    }
    _stdout, stderr, status = Open3.capture3(env, "ci/bin/wait-for-argocd")
    refute status.success?
    assert_match(/ARGOCD_PRODUCTION_CA_FILE is required/, stderr)
  end

  def test_uses_distinct_ca_files_without_disabling_tls_verification
    Dir.mktmpdir do |dir|
      production_ca = File.join(dir, "production-ca.pem")
      local_ca = File.join(dir, "local-production-ca.pem")
      expectations = File.join(dir, "expectations.json")
      gateway_response = File.join(dir, "gateway.json")
      identity_response = File.join(dir, "identity.json")
      log = File.join(dir, "curl.log")
      File.write(production_ca, "production ca")
      File.write(local_ca, "local production ca")
      File.write(File.join(dir, "curl"), <<~SH)
        #!/usr/bin/env bash
        printf '%s\n' "$*" >>"$CURL_LOG"
        case "$*" in
          *uno-arena-production-gateway) cat "$GATEWAY_RESPONSE" ;;
          *) cat "$IDENTITY_RESPONSE" ;;
        esac
      SH
      File.chmod(0o755, File.join(dir, "curl"))
      expected = {
        "schemaVersion" => 1,
        "applications" => {
          "production" => { "uno-arena-production-gateway" => service_expectation("production", "gateway") },
          "local-production" => { "uno-arena-local-production-identity" => service_expectation("local-production", "identity") }
        }
      }
      File.write(expectations, JSON.generate(expected))
      File.write(gateway_response, JSON.generate(service_response("production", "gateway")))
      File.write(identity_response, JSON.generate(service_response("local-production", "identity")))

      env = {
        "PATH" => "#{dir}:#{ENV.fetch("PATH")}",
        "CURL_LOG" => log,
        "GATEWAY_RESPONSE" => gateway_response,
        "IDENTITY_RESPONSE" => identity_response,
        "ARGOCD_EXPECTATIONS_FILE" => expectations,
        "ARGOCD_WAIT_SECONDS" => "1",
        "DEPLOY_ENVIRONMENTS" => "production,local-production",
        "ARGOCD_PRODUCTION_SERVER" => "https://argocd.production.test",
        "ARGOCD_PRODUCTION_AUTH_TOKEN" => "production-token",
        "ARGOCD_PRODUCTION_CA_FILE" => production_ca,
        "ARGOCD_APPLICATIONS_PRODUCTION" => "uno-arena-production-gateway",
        "ARGOCD_LOCAL_PRODUCTION_SERVER" => "https://argocd.local.test",
        "ARGOCD_LOCAL_PRODUCTION_AUTH_TOKEN" => "local-token",
        "ARGOCD_LOCAL_PRODUCTION_CA_FILE" => local_ca,
        "ARGOCD_APPLICATIONS_LOCAL_PRODUCTION" => "uno-arena-local-production-identity"
      }
      stdout, stderr, status = Open3.capture3(env, "ci/bin/wait-for-argocd")
      assert status.success?, "#{stdout}\n#{stderr}"
      calls = File.readlines(log, chomp: true)
      assert_equal 2, calls.length
      assert_includes calls[0], "--cacert #{production_ca}"
      assert_includes calls[0], "https://argocd.production.test/api/v1/applications/uno-arena-production-gateway"
      assert_includes calls[1], "--cacert #{local_ca}"
      assert_includes calls[1], "https://argocd.local.test/api/v1/applications/uno-arena-local-production-identity"
      refute calls.any? { |call| call.include?("--insecure") || call.include?(" -k ") }
    end
  end


  private

  def service_expectation(environment, component)
    {
      "kind" => "service", "component" => component, "environment" => environment,
      "chart" => { "repository" => "https://charts.test", "name" => component, "version" => "0.1.42" },
      "image" => { "repository" => "registry.test/#{component}", "digest" => "sha256:#{'a' * 64}" },
      "values" => { "repository" => "https://git.test/project.git", "revision" => "b" * 40 }
    }
  end

  def service_response(environment, component)
    expected = service_expectation(environment, component)
    {
      "metadata" => { "name" => "uno-arena-#{environment}-#{component}" },
      "spec" => { "sources" => [
        { "repoURL" => expected.dig("chart", "repository"), "chart" => component,
          "targetRevision" => expected.dig("chart", "version"),
          "helm" => { "values" => YAML.dump("image" => expected.fetch("image")) } },
        { "repoURL" => expected.dig("values", "repository"), "targetRevision" => expected.dig("values", "revision"), "ref" => "values" }
      ] },
      "status" => { "sync" => { "status" => "Synced", "revisions" => ["0.1.42", "b" * 40] },
                    "health" => { "status" => "Healthy" } }
    }
  end
end
