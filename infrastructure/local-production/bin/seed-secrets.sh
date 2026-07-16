#!/usr/bin/env bash
# Operator-only secret seed. The input file must be ignored and contain one Secret.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd ruby

SECRET_SEED_MANIFEST="${SECRET_SEED_MANIFEST:-}"
[[ -n "${SECRET_SEED_MANIFEST}" ]] || die "SECRET_SEED_MANIFEST is required"
[[ -f "${SECRET_SEED_MANIFEST}" ]] || die "secret seed manifest not found: ${SECRET_SEED_MANIFEST}"
SEED_SCHEMA="${LOCAL_PRODUCTION_DIR}/charts/platform-secrets/seed.schema.json"
[[ -f "${SEED_SCHEMA}" ]] || die "secret seed schema not found: ${SEED_SCHEMA}"

ruby -rbase64 -rjson -ryaml -e '
  docs = YAML.load_stream(File.read(ARGV.fetch(0))).compact
  abort "seed must contain exactly one document" unless docs.length == 1
  doc = docs.first
  abort "seed document must be v1/Secret" unless doc.is_a?(Hash) && doc["apiVersion"] == "v1" && doc["kind"] == "Secret"
  metadata = doc["metadata"]
  abort "seed Secret metadata must be a mapping" unless metadata.is_a?(Hash)
  abort "seed Secret namespace must be secret-seed" unless metadata["namespace"] == "secret-seed"
  abort "seed Secret name must be uno-arena-local-credentials" unless metadata["name"] == "uno-arena-local-credentials"

  data = doc.fetch("data", {})
  string_data = doc.fetch("stringData", {})
  abort "seed Secret data must be a mapping" unless data.is_a?(Hash)
  abort "seed Secret stringData must be a mapping" unless string_data.is_a?(Hash)
  abort "seed Secret must contain data or stringData" if data.empty? && string_data.empty?

  schema = JSON.parse(File.read(ARGV.fetch(1)))
  required = schema.fetch("required")
  supplied = (data.keys + string_data.keys).uniq
  missing = required - supplied
  abort "seed Secret is missing mapped properties: #{missing.sort.join(", ")}" unless missing.empty?

  decoded = {}
  invalid_data = []
  data.each do |key, value|
    unless value.is_a?(String)
      invalid_data << key
      next
    end
    begin
      decoded[key] = Base64.strict_decode64(value)
    rescue ArgumentError
      invalid_data << key
    end
  end
  abort "seed Secret has invalid base64 data properties: #{invalid_data.sort.join(", ")}" unless invalid_data.empty?
  string_data.each do |key, value|
    abort "seed Secret stringData property #{key} must be a string" unless value.is_a?(String)
    decoded[key] = value
  end

  empty = required.select { |key| decoded.fetch(key).empty? }
  abort "seed Secret has empty mapped properties: #{empty.sort.join(", ")}" unless empty.empty?
  placeholders = required.select { |key| decoded.fetch(key) == "REPLACE_WITH_SECRET" }
  abort "seed Secret still has template placeholders: #{placeholders.sort.join(", ")}" unless placeholders.empty?

  schema.fetch("properties", {}).each do |key, rules|
    next unless decoded.key?(key)
    value = decoded.fetch(key)
    abort "seed Secret property #{key} is shorter than #{rules.fetch("minLength")}" if rules["minLength"] && value.length < rules["minLength"]
    abort "seed Secret property #{key} has an invalid format" if rules["pattern"] && !Regexp.new(rules["pattern"]).match?(value)
  end
' "${SECRET_SEED_MANIFEST}" "${SEED_SCHEMA}"

require_cmd kubectl
assert_simulator_cluster_exists
assert_exact_context
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" get namespace secret-seed >/dev/null 2>&1 ||
  die "namespace secret-seed is not ready; reconcile the secret-provider platform Application first"
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" apply -f "${SECRET_SEED_MANIFEST}"
echo "ok local-production-seed-secrets context=${LOCAL_PRODUCTION_CONTEXT}"
