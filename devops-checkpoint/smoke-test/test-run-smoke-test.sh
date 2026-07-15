#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/uno-smoke-test.XXXXXX")"
trap 'rm -rf "${TMP_DIR}"' EXIT

cat >"${TMP_DIR}/unoarena" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${SMOKE_CALL_LOG}"
case "$1" in
  register) printf '%s\n' '{"status":"ok","username":"smoketest-test-job"}' ;;
  login) printf '%s\n' '{"token":"smoke-token","playerId":"player-1","sessionId":"session-1"}' ;;
  whoami)
    [[ "$2" == "--token" && "$3" == "smoke-token" ]]
    printf '%s\n' '{"username":"smoketest-test-job"}'
    ;;
  *) exit 64 ;;
esac
EOF
chmod +x "${TMP_DIR}/unoarena"

export PATH="${TMP_DIR}:${PATH}"
export SMOKE_CALL_LOG="${TMP_DIR}/calls.log"
export UNOARENA_API_URL="http://127.0.0.1:8080"
export CI_JOB_ID="test-job"

bash "${SCRIPT_DIR}/run-smoke-test.sh" >/dev/null

grep -Fxq 'register --user smoketest-test-job --pass testpass' "${SMOKE_CALL_LOG}"
grep -Fxq 'login --user smoketest-test-job --pass testpass' "${SMOKE_CALL_LOG}"
grep -Fxq 'whoami --token smoke-token' "${SMOKE_CALL_LOG}"
if grep -Fq -- '--json' "${SMOKE_CALL_LOG}"; then
  echo "FAIL: smoke test uses unsupported global --json" >&2
  exit 1
fi

echo "ok devops-smoke-test-cli-contract"
