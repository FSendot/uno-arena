#!/usr/bin/env ruby
# frozen_string_literal: true

# Generate expected catalog fingerprints from services/*/migrations (no SQL duplication).
# Run from repo root or via make kind-render / bootstrap image build.

require "fileutils"

ROOT = File.expand_path("../../..", __dir__)
require_relative "../lib/fingerprint"

OUT = File.expand_path("../fingerprints", __dir__)

SOURCES = [
  { context: "identity", engine: "postgres",
    path: File.join(ROOT, "services/identity/migrations/001_init.sql") },
  { context: "room-gameplay", engine: "postgres",
    path: File.join(ROOT, "services/room-gameplay/migrations/001_init.sql") },
  { context: "tournament-orchestration", engine: "postgres",
    path: File.join(ROOT, "services/tournament-orchestration/migrations/001_init.sql") },
  { context: "ranking", engine: "postgres",
    path: File.join(ROOT, "services/ranking/migrations/001_init.sql") },
  { context: "analytics", engine: "clickhouse",
    path: File.join(ROOT, "services/analytics/migrations/001_init.sql") }
].freeze

def write_env(path, doc)
  lines = [
    "EXPECTED_CHECKSUM=#{doc.fetch('checksum')}",
    "EXPECTED_TABLES=#{Array(doc['tables']).join(',')}",
    "EXPECTED_VIEWS=#{Array(doc['views']).join(',')}",
    "EXPECTED_MATERIALIZED_VIEWS=#{Array(doc['materialized_views']).join(',')}",
    "EXPECTED_SEQUENCES=#{Array(doc['sequences']).join(',')}",
    "EXPECTED_INDEXES=#{Array(doc['indexes']).join(',')}"
  ]
  File.write(path, lines.join("\n") + "\n")
end

FileUtils.mkdir_p(OUT)
SOURCES.each do |src|
  raise "missing migration #{src[:path]}" unless File.file?(src[:path])
  doc = BootstrapFingerprint.fingerprint_for(src[:path], context: src[:context], engine: src[:engine])
  out = File.join(OUT, "#{src[:context]}.json")
  BootstrapFingerprint.write_json(out, doc)
  write_env(File.join(OUT, "#{src[:context]}.env"), doc)
  puts "ok fingerprint #{src[:context]} checksum=#{doc['checksum'][0, 12]}… tables=#{doc['tables'].size}"
end
