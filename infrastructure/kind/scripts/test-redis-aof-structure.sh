#!/usr/bin/env bash
# Offline structure checks for kind Redis AOF foundation + live acceptance script.
# No cluster, no network, no mutation.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

die() {
  echo "FAIL: $*" >&2
  exit 1
}

REDIS_MANIFEST="${MANIFESTS_DIR}/20-redis/redis.yaml"
LIVE="${SCRIPT_DIR}/test-redis-aof.sh"
STRUCT="${SCRIPT_DIR}/test-redis-aof-structure.sh"

[[ -f "${REDIS_MANIFEST}" ]] || die "missing ${REDIS_MANIFEST}"
[[ -f "${LIVE}" ]] || die "missing ${LIVE}"
[[ -f "${STRUCT}" ]] || die "missing ${STRUCT}"

# --- Manifest: AOF under emptyDir /data, disposable, not authoritative ---
grep -qF 'emptyDir: {}' "${REDIS_MANIFEST}" || die "redis must retain emptyDir data volume"
grep -qF 'mountPath: /data' "${REDIS_MANIFEST}" || die "redis must mount emptyDir at /data"

if grep -E -- '--appendonly[[:space:]]+"?no"?' "${REDIS_MANIFEST}" >/dev/null 2>&1; then
  die "redis must not disable AOF (--appendonly no)"
fi
grep -E -- '--appendonly([[:space:]]|,|$)' "${REDIS_MANIFEST}" >/dev/null 2>&1 || die "redis args must set --appendonly"
# Require explicit yes after appendonly flag (YAML list or single-line args).
if ! grep -E -- '--appendonly[[:space:]]+yes|"--appendonly"[[:space:]]*,?[[:space:]]*"yes"|- --appendonly[[:space:]]*$' "${REDIS_MANIFEST}" >/dev/null 2>&1 \
  && ! awk '
    /--appendonly/ { want=1; next }
    want && /yes/ { found=1; exit }
    want && /^[[:space:]]*-/ && $0 !~ /yes/ { exit }
    END { exit !found }
  ' "${REDIS_MANIFEST}"; then
  die "redis must set --appendonly yes"
fi

if ! grep -E -- '--appendfsync[[:space:]]+everysec|"--appendfsync"[[:space:]]*,?[[:space:]]*"everysec"|- --appendfsync' "${REDIS_MANIFEST}" >/dev/null 2>&1; then
  die "redis must set --appendfsync everysec"
fi
grep -E 'everysec' "${REDIS_MANIFEST}" >/dev/null 2>&1 || die "redis must include appendfsync everysec"

# Deterministic append directory under /data (appenddirname relative to dir).
grep -E -- '--appenddirname|--dir' "${REDIS_MANIFEST}" >/dev/null 2>&1 || die "redis must set --dir and/or --appenddirname"
grep -E 'appendonlydir' "${REDIS_MANIFEST}" >/dev/null 2>&1 || die "redis must use deterministic appenddirname appendonlydir under /data"
grep -E -- '--dir[[:space:]]+/data|"--dir"[[:space:]]*,?[[:space:]]*"/data"|- --dir' "${REDIS_MANIFEST}" >/dev/null 2>&1 || die "redis must set --dir /data"

# Keep RDB periodic save disabled; AOF is the same-pod restart path.
grep -E -- '--save[[:space:]]+""|"--save"[[:space:]]*,?[[:space:]]*""' "${REDIS_MANIFEST}" >/dev/null 2>&1 \
  || grep -E -- '- --save[[:space:]]*$' "${REDIS_MANIFEST}" >/dev/null 2>&1 \
  || die "redis must keep --save \"\" (no periodic RDB)"

# Readiness must require PONG so LOADING during AOF replay fails the probe.
if ! grep -E 'grep.*PONG|PONG' "${REDIS_MANIFEST}" >/dev/null 2>&1; then
  die "redis readinessProbe must require PONG (wait for AOF load completion)"
fi

# Comments must not claim Redis authoritative / PVC durability.
if grep -Ei 'authoritative|PVC|persistentVolumeClaim|backup claim' "${REDIS_MANIFEST}" >/dev/null 2>&1; then
  # Allow explicit negation of those claims.
  if ! grep -Ei 'No PVC|disposable|not authoritative|rebuild' "${REDIS_MANIFEST}" >/dev/null 2>&1; then
    die "redis manifest must not claim authoritative/PVC durability without disposable wording"
  fi
fi
grep -Ei 'Disposable|emptyDir|rebuild' "${REDIS_MANIFEST}" >/dev/null 2>&1 || die "redis header must retain disposable/rebuild policy wording"

# --- Live acceptance script: DB15 + prefix, kill PID 1, no FLUSHDB, no Room/Spectator DBs ---
grep -E 'REDIS_TEST_DB=15|SAFE_DB=15|-n[[:space:]]+15|SELECT 15' "${LIVE}" >/dev/null 2>&1 || die "live AOF test must use isolated Redis DB 15"
grep -E 'PREFIX|prefix' "${LIVE}" >/dev/null 2>&1 || die "live AOF test must use a generated key prefix"
grep -E 'kill[[:space:]]+1|kill[[:space:]]+-TERM[[:space:]]+1|kill[[:space:]]+-15[[:space:]]+1' "${LIVE}" >/dev/null 2>&1 || die "live AOF test must kill only Redis PID 1"
grep -E 'sleep[[:space:]]+[12]|fsync|everysec|APPENDFSYNC_WAIT' "${LIVE}" >/dev/null 2>&1 || die "live AOF test must wait past appendfsync everysec before kill"
grep -E 'restartCount|containerID|rollout|wait.*ready|readiness' "${LIVE}" >/dev/null 2>&1 || die "live AOF test must wait for container restart and ready"
grep -E 'XADD|xadd' "${LIVE}" >/dev/null 2>&1 || die "live AOF test must write a stream entry"
grep -E 'GET |get |EXISTS|exists' "${LIVE}" >/dev/null 2>&1 || die "live AOF test must prove string key survival"

# Ignore comment-only mentions; reject executable FLUSHDB/FLUSHALL usage.
if grep -E '^[[:space:]]*[^#[:space:]].*\b(FLUSHDB|FLUSHALL)\b' "${LIVE}" >/dev/null 2>&1 \
  || grep -E 'redis-cli[^\n]*\b(FLUSHDB|FLUSHALL)\b' "${LIVE}" >/dev/null 2>&1; then
  die "live AOF test must not FLUSHDB/FLUSHALL (cleanup prefix only; protect Room/Spectator DBs)"
fi
# Do not SELECT/use Room timer DB 2 or Spectator DB 5 as the test target.
if grep -E 'REDIS_TEST_DB=2\b|SAFE_DB=2\b|redis-cli[^\n]*-n[[:space:]]+2\b|SELECT 2\b' "${LIVE}" >/dev/null 2>&1; then
  die "live AOF test must not target Room timer Redis DB 2"
fi
if grep -E 'REDIS_TEST_DB=5\b|SAFE_DB=5\b|redis-cli[^\n]*-n[[:space:]]+5\b|SELECT 5\b' "${LIVE}" >/dev/null 2>&1; then
  die "live AOF test must not target Spectator Redis DB 5"
fi
grep -E 'DEL |UNLINK |cleanup|SCAN' "${LIVE}" >/dev/null 2>&1 || die "live AOF test must cleanup only its prefix"

grep -E 'assert_kind_context' "${LIVE}" >/dev/null 2>&1 || die "live AOF test must assert kind context"
grep -Ei 'not authoritative|disposable|rebuild|emptyDir|Kafka' "${LIVE}" >/dev/null 2>&1 || die "live AOF test must note Redis is disposable/not authoritative"

echo "ok redis-aof-structure"
