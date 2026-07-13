#!/usr/bin/env bash
# Offline Dockerfile / packaging structure checks (no docker build, no pull).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DF="${ROOT}/Dockerfile"
DC="${ROOT}/.dockerignore"
PASS=0
FAIL=0

ok() {
  echo "ok - $*"
  PASS=$((PASS + 1))
}

bad() {
  echo "not ok - $*" >&2
  FAIL=$((FAIL + 1))
}

[ -f "$DF" ] || { echo "missing Dockerfile" >&2; exit 1; }
[ -f "$DC" ] || { echo "missing .dockerignore" >&2; exit 1; }
[ -f "${ROOT}/bin/unoarena" ] || { echo "missing bin/unoarena" >&2; exit 1; }
[ -f "${ROOT}/lib/game_client.py" ] || { echo "missing lib/game_client.py" >&2; exit 1; }

grep -q 'ENTRYPOINT \["unoarena"\]' "$DF" && ok "ENTRYPOINT is unoarena" || bad "ENTRYPOINT"
grep -q 'USER ' "$DF" && ok "nonroot USER" || bad "USER"
grep -q 'bash' "$DF" && ok "bash present" || bad "bash"
grep -q 'curl' "$DF" && ok "curl present" || bad "curl"
grep -q 'python3' "$DF" && ok "python3 present" || bad "python3"
grep -q 'apk add' "$DF" && ok "apk-based image" || bad "apk"
grep -qv 'pip install\|npm install\|go get\|cargo ' "$DF" && ok "no language package installs" || bad "unexpected package installs"
grep -q 'bin/unoarena' "$DC" && ok ".dockerignore keeps bin/unoarena" || bad ".dockerignore"
grep -q 'lib/game_client.py' "$DF" && grep -q 'lib/game_client.py' "$DC" && ok "Stage B runtime is packaged" || bad "Stage B runtime packaging"

echo "Passed: $PASS  Failed: $FAIL"
[ "$FAIL" -eq 0 ]
