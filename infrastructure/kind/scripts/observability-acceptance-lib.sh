#!/usr/bin/env bash
# Shared helpers for the explicit kind observability acceptance probes.
# Callers own `set -euo pipefail`, context validation, and cleanup traps.

OBS_PIDS=()
OBS_LOGS=()

obs_cleanup_forwards() {
  local pid file
  for pid in "${OBS_PIDS[@]:-}"; do
    kill "${pid}" >/dev/null 2>&1 || true
    wait "${pid}" >/dev/null 2>&1 || true
  done
  for file in "${OBS_LOGS[@]:-}"; do
    rm -f "${file}"
  done
  OBS_PIDS=()
  OBS_LOGS=()
}

obs_forward() {
  local namespace="$1" resource="$2" local_port="$3" remote_port="$4"
  local log pid
  log="$(mktemp "${TMPDIR:-/tmp}/uno-observability-pf.XXXXXX")"
  kubectl -n "${namespace}" port-forward --address=127.0.0.1 \
    "${resource}" "${local_port}:${remote_port}" >"${log}" 2>&1 &
  pid=$!
  OBS_PIDS+=("${pid}")
  OBS_LOGS+=("${log}")
  for _ in $(seq 1 60); do
    kill -0 "${pid}" >/dev/null 2>&1 || die "port-forward exited: ${namespace}/${resource} ($(cat "${log}" 2>/dev/null || true))"
    if grep -Fq "Forwarding from 127.0.0.1:${local_port}" "${log}"; then
      return 0
    fi
    sleep 0.25
  done
  die "port-forward did not become ready: ${namespace}/${resource}"
}

obs_wait_http() {
  local url="$1" timeout="${2:-60}"
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    curl -fsS --max-time 3 "${url}" >/dev/null 2>&1 && return 0
    sleep 1
  done
  die "endpoint did not become ready: ${url}"
}

obs_json_field() {
  local field="$1"
  ruby -rjson -e 'value=JSON.parse(STDIN.read); ARGV[0].split(".").each { |key| value=value.fetch(key) }; puts(value.is_a?(String) ? value : JSON.generate(value))' "${field}"
}

obs_prom_query_optional() {
  local base="$1" query="$2"
  curl -fsS --get --data-urlencode "query=${query}" "${base}/api/v1/query" | ruby -rjson -e '
    body=JSON.parse(STDIN.read)
    abort "Prometheus query failed: #{body}" unless body["status"] == "success"
    result=body.dig("data", "result") || []
    puts(result.empty? ? "" : result.fetch(0).fetch("value").fetch(1))
  '
}

obs_prom_query() {
  local value
  value="$(obs_prom_query_optional "$1" "$2")"
  [[ -n "${value}" ]] || die "Prometheus query returned no series: $2"
  printf '%s\n' "${value}"
}

obs_wait_prom_present() {
  local base="$1" query="$2" timeout="${3:-45}"
  local deadline value
  deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    value="$(obs_prom_query_optional "${base}" "${query}")"
    if [[ -n "${value}" ]]; then
      printf '%s\n' "${value}"
      return 0
    fi
    sleep 2
  done
  die "Prometheus did not scrape a baseline series within ${timeout}s: ${query}"
}

obs_assert_greater() {
  local after="$1" before="$2" label="$3"
  ruby -e 'after,before=ARGV[0,2].map(&:to_f); abort "#{ARGV[2]}: #{after} is not greater than #{before}" unless after > before' \
    "${after}" "${before}" "${label}"
}

obs_assert_equal_number() {
  local actual="$1" expected="$2" label="$3"
  ruby -e 'a,b=ARGV[0,2].map(&:to_f); abort "#{ARGV[2]}: #{a} != #{b}" unless a == b' \
    "${actual}" "${expected}" "${label}"
}

obs_wait_prom_greater() {
  local base="$1" query="$2" before="$3" timeout="${4:-45}"
  local deadline value=0
  deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    value="$(obs_prom_query "${base}" "${query}")"
    if ruby -e 'exit(ARGV[0].to_f > ARGV[1].to_f ? 0 : 1)' "${value}" "${before}"; then
      printf '%s\n' "${value}"
      return 0
    fi
    sleep 2
  done
  die "Prometheus value did not increase for ${query}: before=${before} last=${value}"
}

obs_wait_prom_counter_progress() {
  local base="$1" metric="$2" before_total="$3" before_increase="$4" timeout="${5:-45}"
  local deadline total=0 increase=0 total_query increase_query
  local history_progress_at=-1 history_settle_seconds="${OBS_METRIC_SCRAPE_SETTLE_SECONDS:-16}"
  total_query="sum(${metric})"
  increase_query="sum(increase(${metric}[30m]))"
  deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    total="$(obs_prom_query "${base}" "${total_query}")"
    increase="$(obs_prom_query "${base}" "${increase_query}")"
    if ruby -e 'exit(ARGV[0].to_f > ARGV[1].to_f ? 0 : 1)' "${total}" "${before_total}"; then
      printf '%s\n' "${total}"
      return 0
    fi
    if ruby -e 'exit(ARGV[0].to_f > ARGV[1].to_f ? 0 : 1)' "${increase}" "${before_increase}"; then
      if (( history_progress_at < 0 )); then
        history_progress_at=${SECONDS}
      elif (( SECONDS - history_progress_at >= history_settle_seconds )); then
        # A history-only advance can mean either a process reset or that the
        # instant query is one scrape behind. Confirm one local scrape interval
        # before returning the post-reset raw value to the stability check.
        printf '%s\n' "${total}"
        return 0
      fi
    else
      history_progress_at=-1
    fi
    sleep 2
  done
  die "Prometheus counter did not progress for ${metric}: total=${before_total}->${total} increase=${before_increase}->${increase}"
}

obs_wait_prom_equal() {
  local base="$1" query="$2" expected="$3" stable_seconds="${4:-20}"
  local deadline value
  deadline=$((SECONDS + stable_seconds))
  while (( SECONDS < deadline )); do
    value="$(obs_prom_query "${base}" "${query}")"
    obs_assert_equal_number "${value}" "${expected}" "unexpected counter change for ${query}"
    sleep 2
  done
}

obs_assert_counter_contract() {
  local endpoint="$1" metric="$2" help="$3" body
  body="$(curl -fsS --max-time 5 "${endpoint}/metrics")"
  grep -Fxq "# HELP ${metric} ${help}" <<<"${body}" || die "HELP contract missing for ${metric}"
  grep -Fxq "# TYPE ${metric} counter" <<<"${body}" || die "TYPE contract missing for ${metric}"
  if grep -Eq "^${metric}\\{" <<<"${body}"; then
    die "business counter must have no instrument labels: ${metric}"
  fi
}

obs_trace_id_from_traceparent() {
  local traceparent="$1"
  [[ "${traceparent}" =~ ^[[:xdigit:]]{2}-([[:xdigit:]]{32})-[[:xdigit:]]{16}-[[:xdigit:]]{2}$ ]] || \
    die "invalid W3C traceparent in durable outbox: ${traceparent}"
  printf '%s\n' "${BASH_REMATCH[1]}" | tr '[:upper:]' '[:lower:]'
}

obs_unique() {
  printf '%s-%s-%s\n' "$1" "$(date +%s)" "$$"
}
