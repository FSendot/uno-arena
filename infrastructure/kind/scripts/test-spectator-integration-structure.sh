#!/usr/bin/env bash
# Offline structural assertions for test-spectator-integration.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER="${SCRIPT_DIR}/test-spectator-integration.sh"

die() {
  echo "FAIL: $*" >&2
  exit 1
}

[[ -f "${ADAPTER}" ]] || die "adapter script missing: ${ADAPTER}"

if ! grep -E 'assert_kind_context' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must call assert_kind_context"
fi

if ! grep -E 'SAFE_DB=14|/14' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must target Redis DB 14"
fi
if ! grep -E 'refuse_caller_redis_url|SPECTATOR_REDIS_URL' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must refuse caller-supplied SPECTATOR_REDIS_URL"
fi
if grep -E 'SPECTATOR_REDIS_URL="\$\{' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must not assign SPECTATOR_REDIS_URL from caller environment"
fi

if ! grep -E '0:\$\{REMOTE_PORT\}|0:6379' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must kubectl port-forward with local port 0"
fi
if ! grep -E 'port-forward[[:space:]].*--address=127\.0\.0\.1' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must kubectl port-forward --address=127.0.0.1"
fi
if ! grep -E 'svc/\$\{SERVICE_NAME\}|svc/redis' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must port-forward svc/redis"
fi
if ! grep -E 'PF_PID=\$!' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must record port-forward child as PF_PID=\$!"
fi
if ! grep -E 'jobs[[:space:]]+-pr' "${ADAPTER}" >/dev/null 2>&1; then
  die "cleanup must check jobs -pr membership before kill"
fi
if ! grep -E 'kill[[:space:]]+"\$\{PF_PID\}"|kill[[:space:]]+"\$PF_PID"' "${ADAPTER}" >/dev/null 2>&1; then
  die "cleanup must kill only PF_PID"
fi
if grep -E 'lsof' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must not use lsof"
fi

if ! grep -F 'make' "${ADAPTER}" >/dev/null 2>&1 || ! grep -F 'test-spectator-integration' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must run make test-spectator-integration"
fi
if ! grep -E 'trap[[:space:]]+cleanup[[:space:]]+EXIT' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must trap cleanup EXIT"
fi
if ! grep -F 'does not exercise live Kafka delivery' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must note suite does not exercise live Kafka delivery"
fi
if ! grep -E 'Redis/Lua|quarantine' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must note Redis/Lua quarantine verification scope"
fi

if bash -c "SPECTATOR_REDIS_URL=redis://evil:6379/0; source '${ADAPTER}'" >/dev/null 2>&1; then
  : # sourced scripts with set -e may exit via die — ensure refuse works via extracted fn
fi

# Extract and unit-test refuse_caller_redis_url
runtime_dir="$(mktemp -d "${TMPDIR:-/tmp}/spectator-int-struct.XXXXXX")"
cleanup_runtime() { rm -rf "${runtime_dir}"; }
trap cleanup_runtime EXIT
sed -n '/^refuse_caller_redis_url()/,/^}/p' "${ADAPTER}" >"${runtime_dir}/helpers.sh"
cat >>"${runtime_dir}/helpers.sh" <<'EOF'
die() { echo "die: $*" >&2; exit 1; }
EOF
if bash -c "source '${runtime_dir}/helpers.sh'; SPECTATOR_REDIS_URL=nope refuse_caller_redis_url" >/dev/null 2>&1; then
  die "refuse_caller_redis_url must reject SPECTATOR_REDIS_URL"
fi

echo "ok kind-test-spectator-integration-structure"
