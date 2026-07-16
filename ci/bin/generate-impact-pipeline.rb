#!/usr/bin/env ruby
# frozen_string_literal: true

require "json"
require "optparse"
require "fileutils"
require_relative "../lib/impact_map"
require_relative "../lib/impact_planner"
require_relative "../lib/git_range"
require_relative "../lib/pipeline_renderer"

options = {
  map: "ci/impact-map.yaml",
  output: "generated/child-pipeline.yml",
  impact_output: "generated/impact.json"
}

OptionParser.new do |parser|
  parser.on("--map PATH") { |value| options[:map] = value }
  parser.on("--output PATH") { |value| options[:output] = value }
  parser.on("--impact-output PATH") { |value| options[:impact_output] = value }
  parser.on("--changed-file PATH") { |value| options[:changed_file] = value }
  parser.on("--source SOURCE") { |value| options[:source] = value }
  parser.on("--branch BRANCH") { |value| options[:branch] = value }
  parser.on("--run-component NAME") { |value| options[:run_component] = value }
  parser.on("--run-service NAME") { |value| options[:run_service] = value }
  parser.on("--deploy-environments LIST") { |value| options[:deploy_environments] = value }
end.parse!

begin
  impact_map = UnoArenaCI::ImpactMap.load(options[:map])
  source = options[:source] || ENV.fetch("CI_PIPELINE_SOURCE", "")
  changed_paths = if options[:changed_file]
                    File.readlines(options[:changed_file], chomp: true).reject(&:empty?)
                  elsif source == "web"
                    []
                  else
                    UnoArenaCI::GitRange.new.changed_paths
                  end
  impact = UnoArenaCI::ImpactPlanner.new(
    impact_map,
    source: source,
    changed_paths: changed_paths,
    run_component: options[:run_component] || ENV["RUN_COMPONENT"],
    run_service: options[:run_service] || ENV["RUN_SERVICE"],
    branch: options[:branch] || ENV["CI_COMMIT_BRANCH"],
    deploy_environments: options[:deploy_environments] || ENV["DEPLOY_ENVIRONMENTS"]
  ).plan
  pipeline = UnoArenaCI::PipelineRenderer.new(impact_map, impact).render

  [options[:output], options[:impact_output]].each { |path| FileUtils.mkdir_p(File.dirname(path)) }
  File.write(options[:output], pipeline)
  File.write(options[:impact_output], JSON.pretty_generate(impact) + "\n")
rescue UnoArenaCI::ConfigurationError, UnoArenaCI::ImpactError, OptionParser::ParseError => e
  warn "impact planning failed: #{e.message}"
  exit 1
end
