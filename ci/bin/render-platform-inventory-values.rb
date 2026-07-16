#!/usr/bin/env ruby
# frozen_string_literal: true

require "open3"
require "tempfile"
require "yaml"
require_relative "../lib/impact_map"

component = ENV.fetch("COMPONENT")
chart_path = ENV.fetch("CHART_PATH")
map = UnoArenaCI::ImpactMap.load(File.expand_path("../impact-map.yaml", __dir__))
config = map.platform_component(component)

config.fetch("environments").each do |environment|
  directory = environment == "production" ? "platform-releases" : "platform"
  inventory_path = File.join("environments", environment, directory, "#{component}.yaml")
  abort "missing #{environment} platform inventory for #{component}" unless File.file?(inventory_path)
  document = YAML.safe_load(File.read(inventory_path), [], [], true)
  abort "platform inventory identity mismatch: #{inventory_path}" unless document["component"] == component && document["environment"] == environment
  values = document.fetch("values")
  Tempfile.create(["#{component}-#{environment}-values", ".yaml"]) do |file|
    file.write(YAML.dump(values))
    file.flush
    command = ["helm", "template", "#{component}-#{environment}", chart_path,
               "--namespace", document.fetch("destinationNamespace"), "--values", file.path]
    stdout, stderr, status = Open3.capture3(*command)
    abort "#{environment} inventory values do not render for #{component}: #{stderr}\n#{stdout}" unless status.success?
  end
  puts "rendered #{component} with exact #{environment} inventory values"
end
