#!/usr/bin/env bash
# Explicitly networked acquisition step. Installation scripts never call this file.
set -euo pipefail

VENDOR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCK="${VENDOR_DIR}/sources.lock.tsv"
LOCK_SHA256="a75f186b3c36a768228ff3087b9644a9190766fbe825796ec2771d9adbe6a835"
SOURCE_DIR=""

usage() {
  cat <<'EOF'
Usage: fetch.sh [--source-dir DIRECTORY]

Without --source-dir, download each pinned official release over HTTPS. With
--source-dir, import files by their upstream basename from an operator-reviewed
directory. Every input is checked against sources.lock.tsv before replacement.
EOF
}

if [[ "${1:-}" == "--source-dir" ]]; then
  [[ $# -eq 2 ]] || { usage >&2; exit 2; }
  SOURCE_DIR="$(cd "$2" && pwd -P)"
elif [[ $# -ne 0 ]]; then
  usage >&2
  exit 2
fi

command -v shasum >/dev/null 2>&1 || { echo "shasum required" >&2; exit 1; }
[[ "$(shasum -a 256 "${LOCK}" | awk '{print $1}')" == "${LOCK_SHA256}" ]] || {
  echo "source ledger checksum mismatch" >&2
  exit 1
}
if [[ -z "${SOURCE_DIR}" ]]; then
  command -v curl >/dev/null 2>&1 || { echo "curl required" >&2; exit 1; }
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

while IFS=$'\t' read -r component version url expected_sha relative_path; do
  [[ -n "${component}" && "${component}" != \#* ]] || continue
  [[ "${expected_sha}" =~ ^[a-f0-9]{64}$ ]] || { echo "invalid checksum for ${component}" >&2; exit 1; }
  [[ "${url}" == https://* ]] || { echo "non-HTTPS source for ${component}" >&2; exit 1; }
  case "${url}" in
    https://raw.githubusercontent.com/argoproj/argo-cd/*|\
    https://github.com/kubernetes-sigs/gateway-api/*|\
    https://github.com/cert-manager/cert-manager/*|\
    https://charts.external-secrets.io/*|\
    https://github.com/kubernetes-sigs/metrics-server/*|\
    https://github.com/istio/istio/*) ;;
    *) echo "unapproved source host/path for ${component}: ${url}" >&2; exit 1 ;;
  esac

  if [[ "${relative_path}" == @existing:* ]]; then
    echo "skip ${component} ${version}: verified by infrastructure/istio/verify.sh"
    continue
  fi

  candidate="${tmp_dir}/${component}"
  if [[ -n "${SOURCE_DIR}" ]]; then
    source_file="${SOURCE_DIR}/$(basename "${url}")"
    [[ -f "${source_file}" && ! -L "${source_file}" ]] || {
      echo "missing reviewed source file: ${source_file}" >&2
      exit 1
    }
    cp "${source_file}" "${candidate}"
  else
    curl --proto '=https' --tlsv1.2 --fail --location --silent --show-error \
      --output "${candidate}" "${url}"
  fi

  actual_sha="$(shasum -a 256 "${candidate}" | awk '{print $1}')"
  [[ "${actual_sha}" == "${expected_sha}" ]] || {
    echo "checksum mismatch for ${component} ${version}" >&2
    exit 1
  }
  destination="${VENDOR_DIR}/${relative_path}"
  mkdir -p "$(dirname "${destination}")"
  install -m 0644 "${candidate}" "${destination}"
  echo "ok ${component} ${version} ${relative_path}"
done <"${LOCK}"

"${VENDOR_DIR}/verify.sh"
