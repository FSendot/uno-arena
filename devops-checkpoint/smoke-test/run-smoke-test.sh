#!/usr/bin/env bash
set -euo pipefail

API_URL="${UNOARENA_API_URL:?UNOARENA_API_URL must be set}"
JOB_ID="${CI_JOB_ID:-local-test}"
USERNAME="smoketest-${JOB_ID}"
PASSWORD="testpass"

echo "==> Registering user ${USERNAME} against ${API_URL}"
REGISTER_RESP=$(unoarena register --user "$USERNAME" --pass "$PASSWORD" --json)
echo "$REGISTER_RESP"

STATUS=$(echo "$REGISTER_RESP" | jq -r '.status')
if [ "$STATUS" != "ok" ]; then
  echo "FAIL: register returned status=${STATUS}"
  exit 1
fi

echo "==> Calling whoami for ${USERNAME}"
WHOAMI_RESP=$(unoarena whoami --user "$USERNAME" --json)
echo "$WHOAMI_RESP"

RETURNED_USERNAME=$(echo "$WHOAMI_RESP" | jq -r '.username')
if [ "$RETURNED_USERNAME" != "$USERNAME" ]; then
  echo "FAIL: whoami returned username=${RETURNED_USERNAME}, expected ${USERNAME}"
  exit 1
fi

echo "==> Smoke test PASSED"
