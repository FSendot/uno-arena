#!/usr/bin/env bash
# Explicit proof that the latest authoritative GameCompleted remains one trace
# across Gateway, Room, Game Integrity, durable CDC, and Ranking.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"
# shellcheck source=observability-acceptance-lib.sh
source "${SCRIPT_DIR}/observability-acceptance-lib.sh"

require_cmd curl
require_cmd kubectl
require_cmd ruby
assert_kind_context
"${SCRIPT_DIR}/wait-observability.sh"

TEMPO_PORT="${TEMPO_LOCAL_PORT:-13200}"
LOKI_PORT="${LOKI_LOCAL_PORT:-13100}"
obs_forward observability service/tempo "${TEMPO_PORT}" 3200
obs_forward observability service/loki "${LOKI_PORT}" 3100
trap obs_cleanup_forwards EXIT INT TERM
TEMPO="http://127.0.0.1:${TEMPO_PORT}"
LOKI="http://127.0.0.1:${LOKI_PORT}"
obs_wait_http "${TEMPO}/ready"
obs_wait_http "${LOKI}/ready"

row="$(kubectl -n "${KIND_NAMESPACE}" exec deploy/postgres-room-gameplay -- \
  psql -U room_admin -d room_gameplay -tA -F '|' -v ON_ERROR_STOP=1 -c \
  "SELECT correlation_id, traceparent FROM integration_outbox_events WHERE event_type='GameCompleted' AND payload->>'roomType'='ad_hoc' AND traceparent IS NOT NULL AND traceparent <> '' AND occurred_at >= now() - interval '2 hours' ORDER BY outbox_id DESC LIMIT 1" 2>/dev/null | tr -d '\r')"
[[ -n "${row}" && "${row}" == *'|'* ]] || die "no recent authoritative casual GameCompleted with durable trace context; run business acceptance first"
correlation_id="${row%%|*}"
traceparent="${row#*|}"
[[ -n "${correlation_id}" ]] || die "GameCompleted outbox row omitted correlation_id"
trace_id="$(obs_trace_id_from_traceparent "${traceparent}")"

trace_file="$(mktemp "${TMPDIR:-/tmp}/uno-tempo-trace.XXXXXX")"
cleanup() { rm -f "${trace_file}"; obs_cleanup_forwards; }
trap cleanup EXIT INT TERM
deadline=$((SECONDS + ${OBS_TRACE_TIMEOUT_SECONDS:-180}))
while (( SECONDS < deadline )); do
  if curl -fsS -H 'Accept: application/json' "${TEMPO}/api/traces/${trace_id}" >"${trace_file}" 2>/dev/null \
      && ruby -rjson -e 'body=JSON.parse(File.read(ARGV[0])); exit(body.empty? ? 1 : 0)' "${trace_file}"; then
    break
  fi
  sleep 3
done
[[ -s "${trace_file}" ]] || die "Tempo did not return trace ${trace_id}"

ruby -rjson - "${trace_file}" <<'RUBY'
path = ARGV.fetch(0)
body = JSON.parse(File.read(path))
resource_spans = body["batches"] || body["resourceSpans"] || body["resource_spans"] || []
seen = Hash.new { |h, k| h[k] = [] }
http_servers = {}
sql_leaks = []
scalar = lambda do |value|
  next value unless value.is_a?(Hash)
  value["stringValue"] || value["string_value"] || value["intValue"] || value["int_value"] || value.values.first
end
resource_spans.each do |batch|
  attrs = batch.dig("resource", "attributes") || []
  service = attrs.each_with_object({}) { |a, out| out[a["key"]] = scalar.call(a["value"]) }["service.name"].to_s
  scopes = batch["scopeSpans"] || batch["scope_spans"] || batch["instrumentationLibrarySpans"] || []
  scopes.each do |scope|
    (scope["spans"] || []).each do |span|
      seen[service] << span["name"].to_s
      span_attrs = (span["attributes"] || []).each_with_object({}) do |attribute, out|
        out[attribute["key"]] = scalar.call(attribute["value"])
      end
      sql_leaks << "#{service}:#{span["name"]}" if span["name"].to_s.match?(/\Aquery\s/i)
      sql_leaks.concat(span_attrs.keys.grep(/\A(?:db\.statement|db\.query\.text)\z/).map { |key| "#{service}:#{key}" })
      http_servers[service] = true if span["kind"] == "SPAN_KIND_SERVER" &&
        !span_attrs["http.request.method"].to_s.empty? && !span_attrs["url.path"].to_s.empty?
    end
  end
end
abort "SQL text leaked into trace telemetry: #{sql_leaks.uniq.join(', ')}" unless sql_leaks.empty?
required = {
  "gateway" => [/gateway\.http\.client/],
  "room-gameplay" => [/\Aroom-gameplay\.command\.handle\z/, /\Aroom-gameplay\.command\.commit\z/, /\Aroom-gameplay\.outbox\.persist\z/],
  "game-integrity" => [/game-integrity\.kurrent\.append/],
  "ranking" => [/\Aranking\.kafka\.process\z/, /\Aranking\.database\.commit\z/, /\Aranking\.kafka\.commit\z/],
}
missing = []
required.each do |service, patterns|
  spans = seen[service]
  missing << "service=#{service}" if spans.empty?
  patterns.each { |pattern| missing << "#{service}:#{pattern.inspect}" unless spans.any? { |name| pattern.match?(name) } }
end
%w[gateway room-gameplay game-integrity].each do |service|
  missing << "#{service}:semantic-http-server-span" unless http_servers[service]
end
abort "canonical trace stages missing: #{missing.join(', ')}\nobserved=#{seen}" unless missing.empty?
puts "canonical trace services=#{required.keys.join(',')} spans=#{seen.values.flatten.length}"
RUBY

query_loki_field() {
  local field="$1" needle="$2"
  curl -fsS --get \
    --data-urlencode "query={namespace=\"uno-arena\"} | json | ${field}=\"${needle}\"" \
    --data-urlencode "start=$(( ($(date +%s) - 3600) * 1000000000 ))" \
    --data-urlencode "end=$(( $(date +%s) * 1000000000 ))" \
    --data-urlencode 'limit=100' "${LOKI}/loki/api/v1/query_range"
}

assert_bounded_loki_series() {
  curl -fsS --get \
    --data-urlencode 'match[]={namespace="uno-arena"}' \
    --data-urlencode "start=$(( ($(date +%s) - 3600) * 1000000000 ))" \
    --data-urlencode "end=$(( $(date +%s) * 1000000000 ))" \
    "${LOKI}/loki/api/v1/series" | ruby -rjson -e '
      body = JSON.parse(STDIN.read)
      abort "Loki series query failed" unless body["status"] == "success"
      series = body["data"] || []
      abort "no indexed Uno Arena Loki series" if series.empty?
      allowed = %w[namespace service component container level]
      series.each do |labels|
        unexpected = labels.keys - allowed
        abort "unbounded indexed Loki labels: #{unexpected.join(",")}" unless unexpected.empty?
        abort "indexed Loki series missing namespace" if labels["namespace"].to_s.empty?
      end
    '
}

assert_correlated_logs() {
  local field="$1" expected="$2" body
  body="$(query_loki_field "${field}" "${expected}")"
  printf '%s' "${body}" | ruby -rjson -e '
    field, expected = ARGV
    body = JSON.parse(STDIN.read)
    abort "Loki query failed" unless body["status"] == "success"
    results = body.dig("data", "result") || []
    abort "no parsed Loki streams for #{field}" if results.empty?
    required_labels = %w[namespace service component container level]
    forbidden_key = /(?:password|token|secret|credential|authorization|private_state|payload|connection_string|dsn|encryption_material)/i
    rows = 0
    results.each do |result|
      labels = result.fetch("stream")
      required_labels.each { |key| abort "Loki stream missing bounded label #{key}" if labels[key].to_s.empty? }
      (result["values"] || []).each do |value|
        parsed = JSON.parse(value.fetch(1))
        abort "parsed #{field} mismatch" unless parsed[field].to_s == expected
        %w[timestamp level service component environment version instance_id event].each do |key|
          abort "structured correlated log missing #{key}" unless parsed.key?(key)
        end
        stack = [parsed]
        until stack.empty?
          node = stack.pop
          next unless node.is_a?(Hash)
          node.each do |key, child|
            abort "sensitive structured-log field present: #{key}" if forbidden_key.match?(key)
            stack << child if child.is_a?(Hash)
          end
        end
        rows += 1
      end
    end
    abort "no correlated structured log rows" if rows.zero?
  ' "${field}" "${expected}"
  echo "Loki parsed-field linkage found by ${field}"
}

assert_bounded_loki_series
assert_correlated_logs trace_id "${trace_id}"
assert_correlated_logs correlationId "${correlation_id}"

state="${OBS_ACCEPTANCE_STATE_FILE:-${TMPDIR:-/tmp}/uno-arena-observability-acceptance.state}"
umask 077
printf 'TRACE_ID=%q\nCORRELATION_ID=%q\n' "${trace_id}" "${correlation_id}" >"${state}"
echo "ok kind-observability-trace trace_id=${trace_id} correlationId=${correlation_id} state=${state}"
