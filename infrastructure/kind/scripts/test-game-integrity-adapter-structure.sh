#!/usr/bin/env bash
# Offline structural assertions for test-game-integrity-adapter.sh.
# Ensures the adapter never kills arbitrary 2113 listeners and lets kubectl
# allocate the local port atomically (0:2113) to avoid bind-then-close TOCTOU.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER="${SCRIPT_DIR}/test-game-integrity-adapter.sh"

die() {
  echo "FAIL: $*" >&2
  exit 1
}

[[ -f "${ADAPTER}" ]] || die "adapter script missing: ${ADAPTER}"

# Must not inspect/kill arbitrary listeners via lsof (esp. port 2113).
if grep -E 'lsof[[:space:]].*(-tiTCP:|-iTCP:|:2113)' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must not use lsof to inspect/kill listeners (found lsof TCP pattern)"
fi
if grep -E 'lsof' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must not invoke lsof at all"
fi

# Kill of stale listeners by port must not appear (kill ${stale} pattern from old bug).
if grep -E 'kill[[:space:]]+\$\{?stale' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must not kill stale listeners discovered by port scan"
fi

# Must NOT pre-allocate via bind-then-close (TOCTOU); kubectl owns 0:2113.
if grep -E "bind\(['\"]127\.0\.0\.1['\"],[[:space:]]*0\)" "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must not pre-bind ephemeral ports (TOCTOU); use kubectl 0:2113"
fi
if grep -E 'allocate_ephemeral_local_port' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must not define allocate_ephemeral_local_port"
fi

# Prefer kubectl local-port 0 unless GAME_INTEGRITY_KURRENT_LOCAL_PORT is set.
if ! grep -F 'GAME_INTEGRITY_KURRENT_LOCAL_PORT' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter should honor GAME_INTEGRITY_KURRENT_LOCAL_PORT override without killing listeners"
fi
if ! grep -E '0:\$\{REMOTE_PORT\}|0:2113' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must kubectl port-forward with local port 0 (syntax 0:2113)"
fi

# Default must NOT hardcode LOCAL_PORT=2113.
if grep -E 'LOCAL_PORT="\$\{GAME_INTEGRITY_KURRENT_LOCAL_PORT:-2113\}"' "${ADAPTER}" >/dev/null 2>&1; then
  die "default LOCAL_PORT must not be 2113; let kubectl allocate via 0:2113"
fi

# Explicit override port must be validated as decimal 1..65535 (no unsafe interpolation).
if ! grep -E '1\.\.65535|65535' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must validate explicit local port as decimal 1..65535"
fi
if grep -E "int\('\$\{" "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must not interpolate unvalidated port into python -c source"
fi

# Reject digit strings longer than 5 BEFORE Bash arithmetic (overflow wrap).
if ! grep -F '${#port}' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must reject explicit ports longer than 5 digits before Bash arithmetic (\${#port})"
fi

# Runtime validation cases (no listeners / no kubectl / no kills).
runtime_dir="$(mktemp -d "${TMPDIR:-/tmp}/gi-port-validate.XXXXXX")"
cleanup_runtime() { rm -rf "${runtime_dir}"; }
trap cleanup_runtime EXIT

# Extract validate_local_port + a local die for offline evaluation.
cat >"${runtime_dir}/validate_port.sh" <<'EOF'
set -euo pipefail
die() { echo "die: $*" >&2; exit 1; }
EOF
sed -n '/^validate_local_port()/,/^}/p' "${ADAPTER}" >>"${runtime_dir}/validate_port.sh"

assert_port_rejected() {
  local port="$1"
  local label="$2"
  if bash -c "source '${runtime_dir}/validate_port.sh'; validate_local_port '${port}'" >/dev/null 2>&1; then
    die "runtime must reject ${label} port: ${port}"
  fi
}
assert_port_accepted() {
  local port="$1"
  local label="$2"
  if ! bash -c "source '${runtime_dir}/validate_port.sh'; validate_local_port '${port}'" >/dev/null 2>&1; then
    die "runtime must accept ${label} port: ${port}"
  fi
}

assert_port_rejected "18446744073709551617" "overflow-width"
assert_port_rejected "100000" "six-digit"
assert_port_rejected "0" "zero"
assert_port_rejected "65536" "above-max"
assert_port_rejected "08" "leading-zero"
assert_port_accepted "1" "min"
assert_port_accepted "65535" "max"
assert_port_accepted "2113" "typical"
# Per-run mktemp log (never shared /tmp/gi-kurrent-pf.log).
if ! grep -E 'mktemp' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must capture port-forward output in a per-run mktemp log"
fi
if grep -F '/tmp/gi-kurrent-pf.log' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must not use shared /tmp/gi-kurrent-pf.log"
fi

# Parse confirmed Forwarding from 127.0.0.1:<port> -> 2113 while checking child PID.
if ! grep -F 'Forwarding from 127' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must parse confirmed 'Forwarding from 127.0.0.1:<port> -> 2113' line"
fi
if ! grep -E 'LOCAL_PORT="\$\(sed' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must extract LOCAL_PORT from the Forwarding from line via sed"
fi

# Trap cleanup must target only PF_PID, gated against shell job table (PID reuse).
if ! grep -E 'PF_PID=\$!' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must record port-forward child as PF_PID=\$!"
fi
if ! grep -E 'jobs[[:space:]]+-pr' "${ADAPTER}" >/dev/null 2>&1; then
  die "cleanup must check jobs -pr membership before kill (avoid PID reuse)"
fi
if ! grep -E 'kill[[:space:]]+"\$\{PF_PID\}"|kill[[:space:]]+"\$PF_PID"' "${ADAPTER}" >/dev/null 2>&1; then
  die "cleanup must kill only PF_PID"
fi
if ! grep -E 'wait[[:space:]]+"\$\{PF_PID\}"|wait[[:space:]]+"\$PF_PID"' "${ADAPTER}" >/dev/null 2>&1; then
  die "cleanup must wait only PF_PID"
fi

# Health check before tests (after parsing allocated port).
if ! grep -E 'nc[[:space:]]+-z[[:space:]]+127\.0\.0\.1[[:space:]]+"\$\{LOCAL_PORT\}"' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must health-check via nc -z 127.0.0.1 \$LOCAL_PORT before tests"
fi

# require_cmd nc (health check dependency must be explicit).
if ! grep -E 'require_cmd[[:space:]]+nc' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must require_cmd nc"
fi

# Port-forward must bind localhost only.
if ! grep -E 'port-forward[[:space:]].*--address=127\.0\.0\.1' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must kubectl port-forward --address=127.0.0.1"
fi

# Readiness must verify PF_PID is still alive (port steal / early exit).
if ! grep -E 'kill[[:space:]]+-0[[:space:]]+"\$\{PF_PID\}"' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter readiness must check kill -0 \"\${PF_PID}\" while polling"
fi

# Export integration URL using the chosen local port — never reuse a stale caller URL.
if ! grep -F 'KURRENTDB_INTEGRATION_URL' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must export KURRENTDB_INTEGRATION_URL"
fi
if ! grep -E '127\.0\.0\.1:\$\{LOCAL_PORT\}' "${ADAPTER}" >/dev/null 2>&1; then
  die "KURRENTDB_INTEGRATION_URL must use the allocated LOCAL_PORT"
fi
if grep -E 'KURRENTDB_INTEGRATION_URL="\$\{KURRENTDB_INTEGRATION_URL:-' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must not honor a pre-set KURRENTDB_INTEGRATION_URL over its own port-forward"
fi

echo "ok kind-test-game-integrity-adapter-structure"
