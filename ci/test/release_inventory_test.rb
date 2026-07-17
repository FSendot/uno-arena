# frozen_string_literal: true

require "fileutils"
require_relative "test_helper"

class ReleaseInventoryTest < Minitest::Test
  def test_repository_inventories_are_valid
    assert UnoArenaCI::ReleaseInventory.new.validate_all!
  end

  def test_platform_inventory_catalog_and_stateful_applicationset_are_valid
    inventory = UnoArenaCI::ReleaseInventory.new
    refute_empty inventory.platform_component_names
    inventory.platform_component_names.each do |component|
      path = "environments/local-production/platform/#{component}.yaml"
      assert_equal component, inventory.validate_platform_file!(path, component: component)["component"]
    end
    appset = File.read("environments/local-production/argocd/platform-applicationset.yaml")
    assert_includes appset, "preserveResourcesOnDeletion: true"
    refute_includes appset, "automated:"
  end

  def test_production_managed_platform_catalog_is_valid_but_disabled
    inventory = UnoArenaCI::ReleaseInventory.new
    assert_equal %w[observability platform-secrets], inventory.production_managed_platform_component_names
    inventory.production_managed_platform_component_names.each do |component|
      path = "environments/production/platform-releases/#{component}.yaml"
      document = inventory.validate_platform_file!(path, component: component, environment: "production")
      assert_equal false, document["enabled"]
    end
  end

  def test_released_production_platform_inventory_rejects_placeholder_configuration
    Dir.mktmpdir do |dir|
      FileUtils.cp_r("environments", dir)
      path = File.join(dir, "environments/production/platform-releases/observability.yaml")
      document = YAML.safe_load(File.read(path), [], [], true)
      document["enabled"] = true
      document["status"] = "released"
      document["chart"]["repository"] = "https://gitlab.test/api/v4/projects/1/packages/helm/stable"
      File.write(path, YAML.dump(document))
      error = assert_raises(UnoArenaCI::ConfigurationError) do
        UnoArenaCI::ReleaseInventory.new(dir).validate_platform_file!(
          path, component: "observability", environment: "production"
        )
      end
      assert_match(/placeholder coordinates or configuration/, error.message)
    end
  end

  def test_released_context_bootstrap_rejects_zero_digest
    Dir.mktmpdir do |dir|
      FileUtils.cp_r("environments", dir)
      path = File.join(dir, "environments/local-production/platform/context-bootstrap.yaml")
      document = YAML.safe_load(File.read(path), [], [], true)
      document["enabled"] = true
      document["status"] = "released"
      document["chart"]["repository"] = "https://gitlab.test/api/v4/projects/1/packages/helm/stable"
      document["values"]["image"]["repository"] = "registry.gitlab.test/project/bootstrap"
      document["values"]["image"]["digest"] = "sha256:#{'0' * 64}"
      File.write(path, YAML.dump(document))
      error = assert_raises(UnoArenaCI::ConfigurationError) do
        UnoArenaCI::ReleaseInventory.new(dir).validate_platform_file!(path, component: "context-bootstrap")
      end
      assert_match(/zero digest/, error.message)
    end
  end

  def test_released_inventory_rejects_placeholder_coordinates
    Dir.mktmpdir do |dir|
      FileUtils.cp_r("environments", dir)
      copy_gateway_values(dir)
      path = File.join(dir, "environments/production/services/gateway.yaml")
      document = YAML.safe_load(File.read(path), [], [], true)
      document["enabled"] = true
      document["status"] = "released"
      File.write(path, YAML.dump(document))
      error = assert_raises(UnoArenaCI::ConfigurationError) do
        UnoArenaCI::ReleaseInventory.new(dir).validate_file!(path, environment: "production", component: "gateway")
      end
      assert_match(/placeholder coordinates/, error.message)
    end
  end

  def test_filename_and_component_must_agree
    path = "environments/production/services/gateway.yaml"
    assert_raises(UnoArenaCI::ConfigurationError) do
      UnoArenaCI::ReleaseInventory.new.validate_file!(path, environment: "production", component: "identity")
    end
  end


  def test_applicationsets_watch_only_released_copies_with_exact_overlays
    local = File.read("environments/local-production/argocd/services-applicationset.yaml")
    production = File.read("environments/production/argocd/services-applicationset.yaml")
    assert_includes local, "services/enabled/*.yaml"
    assert_includes local, "values.production.yaml"
    assert_includes local, "values.local-production.yaml"
    assert_includes production, "services/enabled/*.yaml"
    refute_includes production, "values.local-production.yaml"
  end

  def test_released_inventory_requires_immutable_values_revision
    Dir.mktmpdir do |dir|
      FileUtils.cp_r("environments", dir)
      copy_gateway_values(dir)
      path = File.join(dir, "environments/production/services/gateway.yaml")
      document = YAML.safe_load(File.read(path), [], [], true)
      document["enabled"] = true
      document["status"] = "released"
      document["chart"]["repository"] = "https://gitlab.test/api/packages/helm/stable"
      document["image"]["repository"] = "registry.gitlab.test/project/gateway"
      document["image"]["digest"] = "sha256:#{'a' * 64}"
      document["values"]["repository"] = "https://gitlab.test/project.git"
      document["values"]["revision"] = "main"
      File.write(path, YAML.dump(document))
      error = assert_raises(UnoArenaCI::ConfigurationError) do
        UnoArenaCI::ReleaseInventory.new(dir).validate_file!(path, environment: "production", component: "gateway")
      end
      assert_match(/immutable commit SHA/, error.message)
    end
  end

  def test_validate_all_rejects_a_production_platform_simulator_contract
    Dir.mktmpdir do |dir|
      FileUtils.cp_r("environments", dir)
      FileUtils.ln_s(File.expand_path("services"), File.join(dir, "services"))
      FileUtils.ln_s(File.expand_path("infrastructure"), File.join(dir, "infrastructure"))
      path = File.join(dir, "environments/production/platform/kafka.yaml")
      document = YAML.safe_load(File.read(path), [], [], true)
      document["schemaVersion"] = 999
      document["mode"] = "simulator"
      document["managedInCluster"] = true
      File.write(path, YAML.dump(document))
      error = assert_raises(UnoArenaCI::ConfigurationError) do
        UnoArenaCI::ReleaseInventory.new(dir).validate_all!
      end
      assert_match(/schemaVersion must be 1/, error.message)
    end
  end


  private

  def copy_gateway_values(dir)
    destination = File.join(dir, "services/gateway/helm/gateway")
    FileUtils.mkdir_p(destination)
    FileUtils.cp("services/gateway/helm/gateway/values.production.yaml", destination)
    FileUtils.cp("services/gateway/helm/gateway/values.local-production.yaml", destination)
  end
end
