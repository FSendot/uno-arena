# frozen_string_literal: true

require "json"
require "digest"
require "minitest/autorun"
require "open3"
require "tmpdir"
require "yaml"

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "impact_map"
require "impact_planner"
require "pipeline_renderer"
require "git_range"
require "release_inventory"

module ImpactTestHelpers
  def impact_map
    @impact_map ||= UnoArenaCI::ImpactMap.load(File.expand_path("../impact-map.yaml", __dir__))
  end

  def plan(paths, source: "push", branch: "main", run_component: nil, run_service: nil,
           deploy_environments: "local-production")
    UnoArenaCI::ImpactPlanner.new(
      impact_map,
      source: source,
      changed_paths: paths,
      run_component: run_component,
      run_service: run_service,
      branch: branch,
      deploy_environments: deploy_environments,
      production_preflight_executable: deploy_environments.to_s.split(",").include?("production")
    ).plan
  end

  def actions(impact, component)
    impact.fetch("components").fetch(component)
  end

  def platform_actions(impact, component)
    impact.fetch("platformComponents").fetch(component)
  end
end
