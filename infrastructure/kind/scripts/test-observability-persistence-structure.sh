#!/usr/bin/env bash
# Offline guard for the object-store persistence probe's cache-isolation order.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIVE="${SCRIPT_DIR}/test-observability-persistence-live.sh"

die() {
  echo "error: $*" >&2
  exit 1
}

line_of() {
  local pattern="$1"
  local line
  line="$(grep -nF "${pattern}" "${LIVE}" | head -n1 | cut -d: -f1 || true)"
  [[ -n "${line}" ]] || die "persistence proof missing: ${pattern}"
  printf '%s\n' "${line}"
}

last_line_of() {
  local pattern="$1"
  local line
  line="$(grep -nF "${pattern}" "${LIVE}" | tail -n1 | cut -d: -f1 || true)"
  [[ -n "${line}" ]] || die "persistence proof missing: ${pattern}"
  printf '%s\n' "${line}"
}

bash -n "${LIVE}"

minio_ready_line="$(line_of 'new_minio_uid="$(wait_replacement_uid minio "${old_minio_uid}")"')"
fresh_loki_capture_line="$(line_of 'post_minio_loki_name="$(ready_pod_name loki)"')"
fresh_loki_delete_line="$(line_of 'delete pod "${post_minio_loki_name}" --ignore-not-found')"
fresh_loki_ready_line="$(line_of 'final_loki_uid="$(wait_replacement_uid loki "${post_minio_loki_uid}")"')"
cold_loki_http_ready_line="$(last_line_of 'obs_wait_http "${LOKI}/ready" 90')"
final_query_line="$(line_of 'if assert_loki_result >/dev/null 2>&1 && assert_tempo_result')"

(( minio_ready_line < fresh_loki_capture_line &&
   fresh_loki_capture_line < fresh_loki_delete_line &&
   fresh_loki_delete_line < fresh_loki_ready_line &&
   fresh_loki_ready_line < cold_loki_http_ready_line &&
   cold_loki_http_ready_line < final_query_line )) ||
  die "persistence proof must replace Loki after MinIO and before the final query"

echo "ok observability-persistence-structure"
