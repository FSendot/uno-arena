# frozen_string_literal: true

require "fileutils"
require_relative "test_helper"

class ImpactMapTest < Minitest::Test
  include ImpactTestHelpers

  def test_every_repository_path_has_exactly_one_owner
    stdout, stderr, status = Open3.capture3("git", "ls-files", "--cached", "--others", "--exclude-standard")
    assert status.success?, stderr
    paths = stdout.lines.map(&:chomp).reject { |path| path.empty? || path.start_with?("generated/", "artifacts/") }
    refute_empty paths
    paths.each { |path| impact_map.owner_for(path) }
  end

  def test_unknown_path_fails_closed
    error = assert_raises(UnoArenaCI::ImpactError) { impact_map.owner_for("mystery/new-file.txt") }
    assert_match(/unowned changed path/, error.message)
  end

  def test_component_catalog_is_closed
    assert_equal %w[analytics game-integrity gateway identity ranking room-gameplay spectator-view tournament-orchestration], impact_map.component_names
    assert_raises(UnoArenaCI::ImpactError) { impact_map.component("unknown") }
  end

  def test_platform_catalog_owns_each_current_chart_and_inventory
    chart_names = Dir["infrastructure/local-production/charts/*/Chart.yaml"].map { |path| File.basename(File.dirname(path)) }
    chart_names << "observability" if File.file?("infrastructure/observability/helm/uno-arena-observability/Chart.yaml")
    chart_names.sort!
    inventory_names = Dir["environments/local-production/platform/*.yaml"].map { |path| File.basename(path, ".yaml") }.sort
    assert_equal chart_names, inventory_names
    chart_names.each do |name|
      assert_includes impact_map.platform_component_names, name
      config = impact_map.platform_component(name)
      assert_equal "platform_component", impact_map.owner_for("#{config['chartPath']}/Chart.yaml")[:type]
      assert_equal "platform_inventory", impact_map.owner_for("environments/local-production/platform/#{name}.yaml")[:type]
    end
  end

  def test_partial_platform_chart_inventory_pair_fails_closed
    Dir.mktmpdir do |dir|
      FileUtils.mkdir_p(File.join(dir, "ci"))
      FileUtils.cp("ci/impact-map.yaml", File.join(dir, "ci/impact-map.yaml"))
      FileUtils.mkdir_p(File.join(dir, "infrastructure/local-production/charts/kafka"))
      File.write(File.join(dir, "infrastructure/local-production/charts/kafka/Chart.yaml"), "name: kafka\n")
      map = UnoArenaCI::ImpactMap.load(File.join(dir, "ci/impact-map.yaml"))
      assert_raises(UnoArenaCI::ConfigurationError) { map.platform_component_names }
    end
  end


  def test_deployment_graph_rejects_unknown_nodes_and_cycles
    raw = YAML.safe_load(File.read("ci/impact-map.yaml"), [], [], true)
    raw["components"]["gateway"]["dependsOn"] = ["unknown"]
    assert_raises(UnoArenaCI::ConfigurationError) { UnoArenaCI::ImpactMap.new(raw) }

    raw = YAML.safe_load(File.read("ci/impact-map.yaml"), [], [], true)
    raw["platformNodes"]["runtime-platform"]["dependsOn"] = ["gateway"]
    assert_raises(UnoArenaCI::ConfigurationError) { UnoArenaCI::ImpactMap.new(raw) }

    raw = YAML.safe_load(File.read("ci/impact-map.yaml"), [], [], true)
    raw["platformComponents"]["kafka"]["dependsOn"] = ["unknown-platform"]
    assert_raises(UnoArenaCI::ConfigurationError) { UnoArenaCI::ImpactMap.new(raw) }
  end
end
