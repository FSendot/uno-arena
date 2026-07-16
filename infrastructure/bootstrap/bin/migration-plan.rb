#!/usr/bin/env ruby
# frozen_string_literal: true

require "digest"
require "json"

directory, ledger_path = ARGV
abort "usage: migration-plan.rb MIGRATION_DIR [LIVE_LEDGER_TSV]" unless directory
abort "migration directory missing: #{directory}" unless Dir.exist?(directory)

files = Dir[File.join(directory, "*.sql")].sort
abort "migration directory is empty: #{directory}" if files.empty?
expected = files.map.with_index(1) do |path, index|
  name = File.basename(path, ".sql")
  match = name.match(/\A(\d{3})_[a-z0-9_]+\z/)
  abort "invalid migration filename: #{File.basename(path)}" unless match
  abort "migration sequence must be contiguous from 001: #{name}" unless match[1].to_i == index
  if index > 1 && File.read(path).match?(/\bschema_migrations\b|\bschema_migration_(?:checksums|attempts)\b/i)
    abort "migration #{name} must not modify the runner-owned migration ledger"
  end
  {"version" => name, "checksum" => Digest::SHA256.file(path).hexdigest, "path" => path}
end

live = []
if ledger_path && File.file?(ledger_path)
  File.readlines(ledger_path, chomp: true).reject(&:empty?).each do |line|
    version, checksum, extra = line.split("\t", -1)
    abort "invalid live ledger row: #{line}" if version.to_s.empty? || extra
    live << {"version" => version, "checksum" => checksum.to_s}
  end
end
abort "live migration ledger is longer than the immutable release" if live.length > expected.length

live.each_with_index do |entry, index|
  wanted = expected.fetch(index)
  abort "live migration history is not an exact release prefix at #{entry['version']}" unless entry["version"] == wanted["version"]
  next if entry["checksum"].empty? && index.zero?
  abort "migration checksum drift for #{entry['version']}" unless entry["checksum"] == wanted["checksum"]
end

puts JSON.pretty_generate(
  "schemaVersion" => 1,
  "expected" => expected,
  "applied" => live,
  "pending" => expected.drop(live.length),
  "latest" => expected.last.fetch("version"),
  "legacyChecksumBackfill" => live.length == 1 && live.first.fetch("checksum").empty?
)
