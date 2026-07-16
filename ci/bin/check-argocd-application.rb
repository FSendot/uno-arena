#!/usr/bin/env ruby
# frozen_string_literal: true

require "json"
require "optparse"
require_relative "../lib/reconciliation_expectations"

options = {}
OptionParser.new do |parser|
  parser.on("--expectations PATH") { |value| options[:expectations] = value }
  parser.on("--environment NAME") { |value| options[:environment] = value }
  parser.on("--application NAME") { |value| options[:application] = value }
end.parse!

begin
  expected = JSON.parse(File.read(options.fetch(:expectations)))
  expectation = expected.fetch("applications").fetch(options.fetch(:environment)).fetch(options.fetch(:application))
  actual = JSON.parse($stdin.read)
  ready, reason = UnoArenaCI::ArgoApplicationVerifier.new.verify(actual, expectation, options.fetch(:application))
  warn reason unless ready
  exit(ready ? 0 : 1)
rescue KeyError, JSON::ParserError, Errno::ENOENT => e
  warn "cannot verify Argo application: #{e.message}"
  exit 1
end
