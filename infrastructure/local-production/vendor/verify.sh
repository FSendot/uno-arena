#!/usr/bin/env bash
# Offline, fail-closed integrity check for the complete foundation inventory.
set -euo pipefail

VENDOR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${VENDOR_DIR}/../../.." && pwd)"
LOCK_SHA256="a75f186b3c36a768228ff3087b9644a9190766fbe825796ec2771d9adbe6a835"
command -v shasum >/dev/null 2>&1 || { echo "shasum required" >&2; exit 1; }
[[ "$(shasum -a 256 "${VENDOR_DIR}/sources.lock.tsv" | awk '{print $1}')" == "${LOCK_SHA256}" ]] || {
  echo "source ledger checksum mismatch" >&2
  exit 1
}

expected_inventory="$(awk '!/^#/ && $5 !~ /^@existing:/ { print $5 }' "${VENDOR_DIR}/sources.lock.tsv" | LC_ALL=C sort)"
actual_inventory="$(find "${VENDOR_DIR}" -mindepth 2 -type f -print | sed "s#^${VENDOR_DIR}/##" | LC_ALL=C sort)"
[[ "${actual_inventory}" == "${expected_inventory}" ]] || {
  echo "vendored artifact inventory mismatch" >&2
  diff -u <(printf '%s\n' "${expected_inventory}") <(printf '%s\n' "${actual_inventory}") >&2 || true
  exit 1
}

expected_checksums="$(awk '!/^#/ && $5 !~ /^@existing:/ { print $4 "  " $5 }' \
  "${VENDOR_DIR}/sources.lock.tsv" | LC_ALL=C sort)"
actual_checksums="$(LC_ALL=C sort "${VENDOR_DIR}/SHA256SUMS")"
[[ "${actual_checksums}" == "${expected_checksums}" ]] || {
  echo "SHA256SUMS does not match the pinned source ledger" >&2
  exit 1
}

if find "${VENDOR_DIR}" -mindepth 1 -type l -print -quit | grep -q .; then
  echo "vendored inventory must not contain symlinks" >&2
  exit 1
fi

(cd "${VENDOR_DIR}" && shasum -a 256 -c SHA256SUMS)
"${REPO_ROOT}/infrastructure/istio/verify.sh"
echo "ok local-production-vendor"
