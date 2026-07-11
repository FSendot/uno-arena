#!/bin/sh
cd "$(dirname "$0")"
export GOCACHE=/private/tmp/uno-arena-go-cache
exec go test ./domain/ -count=1 -v
