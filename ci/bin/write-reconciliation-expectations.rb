#!/usr/bin/env ruby
# frozen_string_literal: true

require "fileutils"
require "json"
require "optparse"
require_relative "../lib/reconciliation_expectations"

options = { root: Dir.pwd, output: "artifacts/reconciliation-expectations.json" }
OptionParser.new do |parser|
  parser.on("--root PATH") { |value| options[:root] = value }
  parser.on("--output PATH") { |value| options[:output] = value }
  parser.on("--production LIST") { |value| options[:production] = value }
  parser.on("--local-production LIST") { |value| options[:local_production] = value }
end.parse!

applications = {
  "production" => (options[:production] || ENV.fetch("ARGOCD_APPLICATIONS_PRODUCTION", "")).split(","),
  "local-production" => (options[:local_production] || ENV.fetch("ARGOCD_APPLICATIONS_LOCAL_PRODUCTION", "")).split(",")
}

begin
  document = UnoArenaCI::ReconciliationExpectations.new(options[:root]).build(applications)
  FileUtils.mkdir_p(File.dirname(options[:output]))
  File.write(options[:output], JSON.pretty_generate(document) + "\n")
rescue UnoArenaCI::ConfigurationError => e
  warn "cannot build reconciliation expectations: #{e.message}"
  exit 1
end
