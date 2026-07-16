#!/usr/bin/env ruby
# frozen_string_literal: true

require "yaml"

abort "usage: parse-yaml.rb FILE..." if ARGV.empty?
ARGV.each do |path|
  YAML.parse_stream(File.read(path), filename: path)
end
