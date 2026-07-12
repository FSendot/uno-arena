#!/usr/bin/env bash
# Compatibility alias for test-gateway-si-redis-admission-live.sh
# (Redis DB6 durable SI cross-Hub admission only — not Kafka→SSE E2E).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/test-gateway-si-redis-admission-live.sh"
