#!/usr/bin/env ruby
# frozen_string_literal: true

require "optparse"
require_relative "../lib/release_inventory"

options = { root: Dir.pwd, all: false }
OptionParser.new do |parser|
  parser.on("--root PATH") { |value| options[:root] = value }
  parser.on("--all") { options[:all] = true }
end.parse!

abort "validate-inventories.rb currently requires --all" unless options[:all]

begin
  inventory = UnoArenaCI::ReleaseInventory.new(options[:root])
  inventory.validate_all!
  puts "validated 16 service inventories, #{inventory.platform_component_names.length} local platform inventories, " \
       "#{inventory.production_managed_platform_component_names.length} production managed platform inventories, " \
       "#{inventory.production_platform_component_names.length} external production platform contracts, and 10 Argo definitions"
rescue UnoArenaCI::ConfigurationError => e
  warn "inventory validation failed: #{e.message}"
  exit 1
end
