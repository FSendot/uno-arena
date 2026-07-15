#!/usr/bin/env bash
# Fail before cluster creation when the host cannot hold the complete kind lane.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

minimum_gib="${KIND_MIN_HOST_FREE_GIB:-20}"
minimum_engine_memory_gib="${KIND_MIN_ENGINE_MEMORY_GIB:-7}"
[[ "${minimum_gib}" =~ ^[1-9][0-9]*$ ]] || die "KIND_MIN_HOST_FREE_GIB must be a positive integer"
[[ "${minimum_engine_memory_gib}" =~ ^[1-9][0-9]*$ ]] || die "KIND_MIN_ENGINE_MEMORY_GIB must be a positive integer"
(( minimum_gib >= 20 )) || die "KIND_MIN_HOST_FREE_GIB may raise but not lower the 20 GiB acceptance floor"
(( minimum_engine_memory_gib >= 7 )) || die "KIND_MIN_ENGINE_MEMORY_GIB may raise but not lower the 7 GiB acceptance floor"
require_cmd docker

available_kib="$(df -Pk "${REPO_ROOT}" | awk 'END {print $4}')"
[[ "${available_kib}" =~ ^[0-9]+$ ]] || die "could not determine host filesystem free space"

required_kib=$((minimum_gib * 1024 * 1024))
available_gib=$((available_kib / 1024 / 1024))
if (( available_kib < required_kib )); then
  die "host filesystem has ${available_gib} GiB free; the complete kind lane requires at least ${minimum_gib} GiB before cluster creation (remove unused Docker data or free host disk)"
fi

engine_memory_bytes="$(docker info --format '{{.MemTotal}}')"
[[ "${engine_memory_bytes}" =~ ^[0-9]+$ ]] || die "could not determine container-engine memory"
required_engine_memory_bytes=$((minimum_engine_memory_gib * 1024 * 1024 * 1024))
engine_memory_gib=$((engine_memory_bytes / 1024 / 1024 / 1024))
if (( engine_memory_bytes < required_engine_memory_bytes )); then
  die "container engine exposes ${engine_memory_gib} GiB; the complete kind lane requires at least ${minimum_engine_memory_gib} GiB"
fi

echo "ok kind-host-capacity available_gib=${available_gib} required_gib=${minimum_gib} engine_memory_gib=${engine_memory_gib} required_engine_memory_gib=${minimum_engine_memory_gib}"
