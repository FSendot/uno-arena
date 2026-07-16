#!/usr/bin/env bash
# Bootstrap entrypoint. Modes: postgres | clickhouse | migrate-postgres | migrate-clickhouse
set -euo pipefail

mode="${1:-}"
case "$mode" in
  postgres)
    exec /bootstrap/bin/bootstrap-postgres.sh
    ;;
  clickhouse)
    exec /bootstrap/bin/bootstrap-clickhouse.sh
    ;;
  migrate-postgres)
    exec /bootstrap/bin/migrate-postgres.sh
    ;;
  migrate-clickhouse)
    exec /bootstrap/bin/migrate-clickhouse.sh
    ;;
  *)
    echo "usage: bootstrap-entrypoint.sh <postgres|clickhouse|migrate-postgres|migrate-clickhouse>" >&2
    exit 2
    ;;
esac
