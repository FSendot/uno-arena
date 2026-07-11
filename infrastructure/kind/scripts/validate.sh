#!/usr/bin/env bash
# Offline validation for kind foundation. Never pulls images or contacts a cluster.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd ruby

# Idempotent fingerprint + Kafka render, then validate (including generated Kafka plan).
ruby "${REPO_ROOT}/infrastructure/bootstrap/bin/generate-fingerprints.rb" >/dev/null
ruby "${REPO_ROOT}/infrastructure/bootstrap/tests/fingerprint_test.rb"
ruby "${REPO_ROOT}/infrastructure/bootstrap/tests/psql_gate_structure_test.rb"
ruby "${REPO_ROOT}/infrastructure/bootstrap/tests/cdc_structure_test.rb"
bash "${REPO_ROOT}/infrastructure/bootstrap/tests/postgres_bootstrap_fail_closed_test.sh"
bash "${REPO_ROOT}/infrastructure/bootstrap/tests/postgres_bootstrap_render_test.sh"
bash "${REPO_ROOT}/infrastructure/bootstrap/tests/cdc_sql_integration_test.sh"
ruby "${SCRIPT_DIR}/render-kafka-topics.rb" >/dev/null
ruby "${KIND_DIR}/tests/kafka_retention_dlq_test.rb"
ruby "${KIND_DIR}/tests/validate_kind.rb"

# Shell / Ruby syntax checks (offline).
require_cmd bash
bash -n "${SCRIPT_DIR}/lib.sh"
bash -n "${SCRIPT_DIR}/apply.sh"
bash -n "${SCRIPT_DIR}/wait.sh"
bash -n "${SCRIPT_DIR}/reset.sh"
bash -n "${SCRIPT_DIR}/validate.sh"
bash -n "${SCRIPT_DIR}/render.sh"
bash -n "${SCRIPT_DIR}/create-cluster.sh"
bash -n "${SCRIPT_DIR}/build-load-bootstrap.sh"
bash -n "${SCRIPT_DIR}/test-game-integrity-adapter.sh"
bash -n "${SCRIPT_DIR}/test-game-integrity-adapter-structure.sh"
bash -n "${SCRIPT_DIR}/test-identity-integration.sh"
bash -n "${SCRIPT_DIR}/test-identity-integration-structure.sh"
bash -n "${SCRIPT_DIR}/test-identity-adapter.sh"
bash -n "${SCRIPT_DIR}/test-identity-adapter-structure.sh"
bash -n "${SCRIPT_DIR}/test-redis-aof.sh"
bash -n "${SCRIPT_DIR}/test-redis-aof-structure.sh"
"${SCRIPT_DIR}/test-identity-integration-structure.sh"
"${SCRIPT_DIR}/test-redis-aof-structure.sh"
bash -n "${REPO_ROOT}/infrastructure/bootstrap/bin/bootstrap-entrypoint.sh"
bash -n "${REPO_ROOT}/infrastructure/bootstrap/bin/bootstrap-postgres.sh"
bash -n "${REPO_ROOT}/infrastructure/bootstrap/bin/bootstrap-clickhouse.sh"
bash -n "${GENERATED_DIR}/kafka-create-topics.sh"
ruby -c "${SCRIPT_DIR}/render-kafka-topics.rb" >/dev/null
ruby -c "${KIND_DIR}/tests/validate_kind.rb" >/dev/null
ruby -c "${REPO_ROOT}/infrastructure/bootstrap/lib/fingerprint.rb" >/dev/null
ruby -c "${REPO_ROOT}/infrastructure/bootstrap/bin/generate-fingerprints.rb" >/dev/null
ruby -c "${REPO_ROOT}/infrastructure/bootstrap/tests/fingerprint_test.rb" >/dev/null

echo "ok kind-validate"
