#!/usr/bin/env bash
# Bootstrap entrypoint. Modes: postgres | clickhouse
set -euo pipefail

mode="${1:-}"
case "$mode" in
  postgres)
    exec /bootstrap/bin/bootstrap-postgres.sh
    ;;
  clickhouse)
    exec /bootstrap/bin/bootstrap-clickhouse.sh
    ;;
  *)
    echo "usage: bootstrap-entrypoint.sh <postgres|clickhouse>" >&2
    exit 2
    ;;
esac
