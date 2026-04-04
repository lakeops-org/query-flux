#!/usr/bin/env bash
# Install libduckdb (.so + headers) from the official GitHub release archive.
# Requires: curl, unzip, sha256sum, sudo
# Env: DUCKDB_VERSION, DUCKDB_LIBDUCKDB_LINUX_AMD64_SHA256
set -euo pipefail

: "${DUCKDB_VERSION:?DUCKDB_VERSION is required}"
: "${DUCKDB_LIBDUCKDB_LINUX_AMD64_SHA256:?DUCKDB_LIBDUCKDB_LINUX_AMD64_SHA256 is required}"

readonly url="https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/libduckdb-linux-amd64.zip"
readonly zip="/tmp/duckdb.zip"
readonly extract_dir="/tmp/duckdb"

curl -fsSL "${url}" -o "${zip}"
echo "${DUCKDB_LIBDUCKDB_LINUX_AMD64_SHA256}  ${zip}" | sha256sum -c -

rm -rf "${extract_dir}"
mkdir -p "${extract_dir}"
unzip -o "${zip}" -d "${extract_dir}"

sudo cp "${extract_dir}/libduckdb.so" /usr/local/lib/
sudo cp "${extract_dir}/duckdb.h" /usr/local/include/
if [[ -f "${extract_dir}/duckdb.hpp" ]]; then
  sudo cp "${extract_dir}/duckdb.hpp" /usr/local/include/
fi
sudo ldconfig
