#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
export GOCACHE=/tmp/uno-arena-go-cache
go test ./domain/ -count=1 2>&1
