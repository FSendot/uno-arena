# frozen_string_literal: true

require "fileutils"
require_relative "test_helper"
require "reconciliation_expectations"

class ReconciliationExpectationsTest < Minitest::Test
  SHA = "a" * 40
  DIGEST = "sha256:#{'b' * 64}"

  def test_service_verification_rejects_stale_synced_healthy_revision
    Dir.mktmpdir do |dir|
      inventory = released_service_inventory(dir, "identity")
      application = inventory.fetch("application")
      expectation = UnoArenaCI::ReconciliationExpectations.new(dir).build(
        "production" => [application], "local-production" => []
      ).dig("applications", "production", application)
      response = service_response(inventory)
      ready, = UnoArenaCI::ArgoApplicationVerifier.new.verify(response, expectation, application)
      assert ready

      response["status"]["sync"]["revisions"][0] = "0.1.41"
      ready, reason = UnoArenaCI::ArgoApplicationVerifier.new.verify(response, expectation, application)
      refute ready
      assert_match(/has not synced the promoted source revisions/, reason)
    end
  end

  def test_platform_verification_binds_chart_revision_and_inline_values
    Dir.mktmpdir do |dir|
      inventory = released_platform_inventory(dir, "observability")
      application = inventory.fetch("application")
      expectation = UnoArenaCI::ReconciliationExpectations.new(dir).build(
        "production" => [], "local-production" => [application]
      ).dig("applications", "local-production", application)
      response = {
        "metadata" => { "name" => application },
        "spec" => { "source" => {
          "repoURL" => inventory.dig("chart", "repository"), "chart" => inventory.dig("chart", "name"),
          "targetRevision" => inventory.dig("chart", "version"), "helm" => { "values" => JSON.generate(inventory.fetch("values")) }
        } },
        "status" => { "sync" => { "status" => "Synced", "revision" => inventory.dig("chart", "version") },
                      "health" => { "status" => "Healthy" } }
      }
      ready, = UnoArenaCI::ArgoApplicationVerifier.new.verify(response, expectation, application)
      assert ready
      response["spec"]["source"]["helm"]["values"] = "{}"
      ready, reason = UnoArenaCI::ArgoApplicationVerifier.new.verify(response, expectation, application)
      refute ready
      assert_match(/values do not match/, reason)
    end
  end

  def test_production_platform_expectation_uses_managed_release_inventory
    Dir.mktmpdir do |dir|
      destination = File.join(dir, "environments/production/platform-releases")
      FileUtils.mkdir_p(destination)
      document = YAML.safe_load(
        File.read("environments/production/platform-releases/platform-secrets.yaml"), [], [], true
      )
      document["enabled"] = true
      document["status"] = "released"
      document["chart"]["repository"] = "https://gitlab.test/api/v4/projects/1/packages/helm/stable"
      File.write(File.join(destination, "platform-secrets.yaml"), YAML.dump(document))
      application = document.fetch("application")
      expectation = UnoArenaCI::ReconciliationExpectations.new(dir).build(
        "production" => [application], "local-production" => []
      ).dig("applications", "production", application)
      assert_equal "platform", expectation["kind"]
      assert_equal "production", expectation["environment"]
      assert_equal document["values"], expectation["values"]
    end
  end

  def test_control_plane_expectation_requires_exact_git_revision
    revision = "d" * 40
    application = "uno-arena-local-production-root"
    expectation = UnoArenaCI::ReconciliationExpectations.new(".", desired_revision: revision).build(
      "production" => [], "local-production" => [application]
    ).dig("applications", "local-production", application)
    response = {
      "metadata" => {"name" => application},
      "spec" => {"source" => {"path" => "environments/local-production/argocd"}},
      "status" => {"sync" => {"status" => "Synced", "revision" => revision}, "health" => {"status" => "Healthy"}}
    }
    ready, = UnoArenaCI::ArgoApplicationVerifier.new.verify(response, expectation, application)
    assert ready
    response["status"]["sync"]["revision"] = "e" * 40
    ready, reason = UnoArenaCI::ArgoApplicationVerifier.new.verify(response, expectation, application)
    refute ready
    assert_match(/exact desired-state revision/, reason)
  end

  private

  def released_service_inventory(dir, component)
    destination = File.join(dir, "environments/production/services")
    FileUtils.mkdir_p(destination)
    document = YAML.safe_load(File.read("environments/production/services/#{component}.yaml"), [], [], true)
    document["enabled"] = true
    document["status"] = "released"
    document["chart"]["repository"] = "https://gitlab.test/api/v4/projects/1/packages/helm/stable"
    document["chart"]["version"] = "0.1.42"
    document["image"] = { "repository" => "registry.gitlab.test/project/#{component}", "digest" => DIGEST }
    document["values"]["repository"] = "https://gitlab.test/project.git"
    document["values"]["revision"] = SHA
    File.write(File.join(destination, "#{component}.yaml"), YAML.dump(document))
    document
  end

  def released_platform_inventory(dir, component)
    destination = File.join(dir, "environments/local-production/platform")
    FileUtils.mkdir_p(destination)
    document = YAML.safe_load(File.read("environments/local-production/platform/#{component}.yaml"), [], [], true)
    document["enabled"] = true
    document["status"] = "released"
    document["chart"]["repository"] = "https://gitlab.test/api/v4/projects/1/packages/helm/stable"
    document["chart"]["version"] = "0.1.42"
    File.write(File.join(destination, "#{component}.yaml"), YAML.dump(document))
    document
  end

  def service_response(inventory)
    {
      "metadata" => { "name" => inventory.fetch("application") },
      "spec" => { "sources" => [
        { "repoURL" => inventory.dig("chart", "repository"), "chart" => inventory.dig("chart", "name"),
          "targetRevision" => inventory.dig("chart", "version"), "helm" => { "values" => YAML.dump("image" => inventory.fetch("image")) } },
        { "repoURL" => inventory.dig("values", "repository"), "targetRevision" => inventory.dig("values", "revision"), "ref" => "values" }
      ] },
      "status" => { "sync" => { "status" => "Synced", "revisions" => [inventory.dig("chart", "version"), SHA] },
                    "health" => { "status" => "Healthy" } }
    }
  end
end
