# frozen_string_literal: true

require "fileutils"
require_relative "test_helper"

class PlatformDeliveryTest < Minitest::Test
  SHA = "a" * 40
  DIGEST = "sha256:#{'b' * 64}"

  def test_context_bootstrap_promotion_updates_only_local_platform_canonical_and_enabled
    Dir.mktmpdir do |dir|
      copy_platform_inventory(dir, "context-bootstrap")
      write_artifact(dir, "context-bootstrap", "chart", {
        "chart" => { "repository" => "https://gitlab.test/api/v4/projects/1/packages/helm/stable", "name" => "context-bootstrap", "version" => "0.1.42" }
      })
      write_artifact(dir, "context-bootstrap", "image", {
        "image" => { "repository" => "registry.gitlab.test/project/bootstrap", "digest" => DIGEST }
      })
      env = promotion_env("context-bootstrap")
      stdout, stderr, status = Open3.capture3(env, "ruby", File.expand_path("../bin/promote-releases.rb", __dir__), chdir: dir)
      assert status.success?, "#{stdout}\n#{stderr}"
      canonical = YAML.safe_load(File.read(File.join(dir, "environments/local-production/platform/context-bootstrap.yaml")), [], [], true)
      enabled = YAML.safe_load(File.read(File.join(dir, "environments/local-production/platform/enabled/context-bootstrap.yaml")), [], [], true)
      assert_equal canonical, enabled
      assert_equal true, canonical["enabled"]
      assert_equal "released", canonical["status"]
      assert_equal "0.1.42", canonical.dig("chart", "version")
      assert_equal DIGEST, canonical.dig("values", "image", "digest")
      refute Dir.exist?(File.join(dir, "environments/production/platform/enabled"))
    end
  end

  def test_initial_context_bootstrap_promotion_fails_without_image_artifact
    Dir.mktmpdir do |dir|
      copy_platform_inventory(dir, "context-bootstrap")
      write_artifact(dir, "context-bootstrap", "chart", {
        "chart" => { "repository" => "https://gitlab.test/api/v4/projects/1/packages/helm/stable", "name" => "context-bootstrap", "version" => "0.1.42" }
      })
      _stdout, stderr, status = Open3.capture3(
        promotion_env("context-bootstrap"), "ruby", File.expand_path("../bin/promote-releases.rb", __dir__), chdir: dir
      )
      refute status.success?
      assert_match(/requires its immutable image artifact/, stderr)
    end
  end

  def test_released_context_bootstrap_can_promote_a_new_image_without_repackaging_chart
    Dir.mktmpdir do |dir|
      copy_platform_inventory(dir, "context-bootstrap")
      path = File.join(dir, "environments/local-production/platform/context-bootstrap.yaml")
      document = YAML.safe_load(File.read(path), [], [], true)
      document["enabled"] = true
      document["status"] = "released"
      document["chart"]["repository"] = "https://gitlab.test/api/v4/projects/1/packages/helm/stable"
      document["values"]["image"] = { "repository" => "registry.gitlab.test/project/bootstrap", "digest" => "sha256:#{'c' * 64}" }
      File.write(path, YAML.dump(document))
      write_artifact(dir, "context-bootstrap", "image", {
        "image" => { "repository" => "registry.gitlab.test/project/bootstrap", "digest" => DIGEST }
      })
      stdout, stderr, status = Open3.capture3(
        promotion_env("context-bootstrap"), "ruby", File.expand_path("../bin/promote-releases.rb", __dir__), chdir: dir
      )
      assert status.success?, "#{stdout}\n#{stderr}"
      promoted = YAML.safe_load(File.read(path), [], [], true)
      assert_equal "0.1.0", promoted.dig("chart", "version")
      assert_equal DIGEST, promoted.dig("values", "image", "digest")
    end
  end

  def test_platform_metadata_rejects_path_escape
    env = {
      "COMPONENT_KIND" => "platform", "COMPONENT" => "kafka", "CHART_NAME" => "kafka",
      "CHART_PATH" => "infrastructure/local-production/charts/../platform-secrets"
    }
    script = 'source ci/bin/platform-metadata.sh; configure_release_component'
    _stdout, stderr, status = Open3.capture3(env, "bash", "-c", script)
    refute status.success?
    assert_match(/unsafe platform CHART_PATH/, stderr)
  end

  def test_service_chart_promotion_loads_inventory_before_validating_chart_identity
    Dir.mktmpdir do |dir|
      %w[production local-production].each do |environment|
        destination = File.join(dir, "environments/#{environment}/services")
        FileUtils.mkdir_p(destination)
        document = YAML.safe_load(File.read("environments/#{environment}/services/identity.yaml"), [], [], true)
        document["enabled"] = true
        document["status"] = "released"
        document["chart"]["repository"] = "https://gitlab.test/api/v4/projects/1/packages/helm/stable"
        document["image"]["repository"] = "registry.gitlab.test/project/identity"
        document["image"]["digest"] = DIGEST
        document["values"]["repository"] = "https://gitlab.test/project.git"
        document["values"]["revision"] = SHA
        File.write(File.join(destination, "identity.yaml"), YAML.dump(document))
      end
      write_artifact(dir, "identity", "chart", {
        "chart" => { "repository" => "https://gitlab.test/api/v4/projects/1/packages/helm/stable", "name" => "identity", "version" => "0.1.42" }
      })
      env = promotion_env("", services: "identity")
      stdout, stderr, status = Open3.capture3(env, "ruby", File.expand_path("../bin/promote-releases.rb", __dir__), chdir: dir)
      assert status.success?, "#{stdout}\n#{stderr}"
      assert_equal "0.1.42", YAML.safe_load(File.read(File.join(dir, "environments/production/services/identity.yaml"))).dig("chart", "version")
    end
  end

  def test_observability_promotion_accepts_canonical_chart_name
    Dir.mktmpdir do |dir|
      copy_platform_inventory(dir, "observability")
      write_artifact(dir, "observability", "chart", {
        "chart" => { "repository" => "https://gitlab.test/api/v4/projects/1/packages/helm/stable", "name" => "uno-arena-observability", "version" => "0.1.42" }
      })
      stdout, stderr, status = Open3.capture3(
        promotion_env("observability").merge("DEPLOY_ENVIRONMENTS" => "local-production"),
        "ruby", File.expand_path("../bin/promote-releases.rb", __dir__), chdir: dir
      )
      assert status.success?, "#{stdout}\n#{stderr}"
      promoted = YAML.safe_load(File.read(File.join(dir, "environments/local-production/platform/observability.yaml")))
      assert_equal "uno-arena-observability", promoted.dig("chart", "name")
      assert_equal "0.1.42", promoted.dig("chart", "version")
    end
  end

  def test_production_observability_promotion_requires_real_configuration
    Dir.mktmpdir do |dir|
      destination = File.join(dir, "environments/production/platform-releases")
      FileUtils.mkdir_p(destination)
      source = "environments/production/platform-releases/observability.yaml"
      FileUtils.cp(source, destination)
      write_artifact(dir, "observability", "chart", {
        "chart" => { "repository" => "https://gitlab.test/api/v4/projects/1/packages/helm/stable", "name" => "uno-arena-observability", "version" => "0.1.42" }
      })
      env = promotion_env("observability").merge("DEPLOY_ENVIRONMENTS" => "production")
      _stdout, stderr, status = Open3.capture3(env, "ruby", File.expand_path("../bin/promote-releases.rb", __dir__), chdir: dir)
      refute status.success?
      assert_match(/still contains placeholder configuration/, stderr)

      path = File.join(destination, "observability.yaml")
      document = YAML.safe_load(File.read(path), [], [], true)
      document["values"]["storage"]["s3"].merge!(
        "endpoint" => "https://s3.production.test", "region" => "us-test-1",
        "lokiBucket" => "uno-loki", "tempoBucket" => "uno-tempo"
      )
      File.write(path, YAML.dump(document))
      stdout, stderr, status = Open3.capture3(env, "ruby", File.expand_path("../bin/promote-releases.rb", __dir__), chdir: dir)
      assert status.success?, "#{stdout}\n#{stderr}"
      promoted = YAML.safe_load(File.read(path), [], [], true)
      assert_equal true, promoted["enabled"]
      assert_equal promoted, YAML.safe_load(File.read(File.join(destination, "enabled/observability.yaml")), [], [], true)
    end
  end

  def test_promotion_rejects_empty_or_duplicate_environment_selection
    ["", "local-production,local-production"].each do |selection|
      env = promotion_env("observability").merge("DEPLOY_ENVIRONMENTS" => selection)
      _stdout, stderr, status = Open3.capture3(env, "ruby", File.expand_path("../bin/promote-releases.rb", __dir__))
      refute status.success?
      assert_match(/promotion environments must (?:not be empty|be unique)/, stderr)
    end
  end

  private

  def copy_platform_inventory(dir, component)
    destination = File.join(dir, "environments/local-production/platform")
    FileUtils.mkdir_p(destination)
    FileUtils.cp("environments/local-production/platform/#{component}.yaml", destination)
  end

  def write_artifact(dir, component, kind, payload)
    destination = File.join(dir, "artifacts")
    FileUtils.mkdir_p(destination)
    if kind == "chart"
      chart = payload.fetch("chart")
      package = File.join(destination, "#{chart.fetch('name')}-#{chart.fetch('version')}.tgz")
      File.write(package, "test Helm package bytes for #{component}\n")
      chart["packageSha256"] = Digest::SHA256.file(package).hexdigest
    end
    document = { "schemaVersion" => 1, "component" => component, "sourceSha" => SHA }.merge(payload)
    File.write(File.join(destination, "#{component}-#{kind}.json"), JSON.pretty_generate(document))
  end

  def promotion_env(component, services: "")
    {
      "PROMOTE_COMPONENTS" => services, "PROMOTE_PLATFORM_COMPONENTS" => component,
      "DEPLOY_ENVIRONMENTS" => "production,local-production", "CI_COMMIT_SHA" => SHA,
      "CI_PROJECT_URL" => "https://gitlab.test/project"
    }
  end
end
