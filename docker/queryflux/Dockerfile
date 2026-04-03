# Multi-stage build for the QueryFlux **server only** (no Studio UI).
# For server + Studio and multi-arch releases, use ../queryflux-unified/Dockerfile.
#
# Multi-stage build for queryflux with cargo-chef (fast rebuilds when only code changes).
#
# PyO3/sqlglot: builder + runtime both python:3.12.13-slim-trixie (bookworm has no python3.12 apt).
# Rust: rustup 1.91.1 on top of that image.
# DuckDB: DUCKDB_DOWNLOAD_LIB + DUCKDB_STATIC (mold/lld fail on -lduckdb dylib). .so may still be copied for loader path.
#
# Build (context MUST be this repo root so Cargo.toml + Cargo.lock match):
#   cd /path/to/queryflux && docker build -f docker/queryflux/Dockerfile -t queryflux:local .
#
# Lockfile (matches rust:1.91.1 in this image):
#   rustup run 1.91.1 cargo generate-lockfile
#
# Final `cargo build` has no `--locked` (stub vs full crate sources can make `cargo --locked` fail);
# `cargo chef cook` uses `--locked` so deps match Cargo.lock.

# =============================================================================
# Stage 1: Chef base — same Python as runtime + Rust toolchains via rustup
# =============================================================================
FROM python:3.12.13-slim-trixie AS chef

# Prefer /usr/local Python 3.12 over Debian /usr/bin/python3 (often 3.11 on trixie slim).
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/bin:/usr/local/cargo/bin:$PATH

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    git \
    pkg-config \
    libssl-dev \
    libffi-dev \
    build-essential \
    zlib1g-dev \
    cmake \
    ninja-build \
    perl \
    golang-go \
    mold \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    --default-toolchain 1.91.1 \
    --profile minimal \
    && rustup component add rustfmt clippy --toolchain 1.91.1

RUN cargo install cargo-chef --locked

WORKDIR /build

# =============================================================================
# Stage 2: Planner
# =============================================================================
FROM chef AS planner

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates

RUN cargo chef prepare --recipe-path recipe.json

# =============================================================================
# Stage 3: Builder — cook dependencies (cached), then full build
# =============================================================================
FROM chef AS builder

# If unset, Docker passes "" and Cargo errors: "Number of parallel jobs should be `default` or a number."
ARG CARGO_BUILD_JOBS=default
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}

COPY requirements.txt .
RUN /usr/local/bin/python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt && \
    ln -sfn "$(/opt/venv/bin/python3 -c 'import site; print(site.getsitepackages()[0])')" /opt/venv/site-packages && \
    /opt/venv/bin/python3 -c 'import sys; assert sys.version_info[:2] == (3, 12), f"expected 3.12, got {sys.version}"'

ENV PYO3_PYTHON=/opt/venv/bin/python3
ENV PYTHONPATH=/opt/venv/site-packages
ENV RUSTFLAGS="-C link-arg=-fuse-ld=mold"
ENV DUCKDB_DOWNLOAD_LIB=1
ENV DUCKDB_STATIC=1
ENV LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/usr/lib/aarch64-linux-gnu

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY --from=planner /build/recipe.json recipe.json

# Strip sources for cook; cargo-chef needs member manifests + stub .rs (workspace layout).
COPY crates ./crates
RUN set -eu && find crates -type f -name '*.rs' -delete && \
    for c in queryflux-core queryflux-auth queryflux-persistence queryflux-routing \
        queryflux-translation queryflux-engine-adapters queryflux-frontend \
        queryflux-cluster-manager queryflux-config queryflux-metrics queryflux-e2e-tests; do \
      mkdir -p "crates/$c/src" && echo '// cargo-chef placeholder' > "crates/$c/src/lib.rs"; \
    done && \
    mkdir -p crates/queryflux/src crates/queryflux-bench/src && \
    printf 'fn main() {}\n' | tee crates/queryflux/src/main.rs > crates/queryflux-bench/src/main.rs

# id=… avoids reusing target/ from older images (e.g. Bookworm PyO3 linking -lpython3.11).
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=queryflux-registry \
    --mount=type=cache,target=/build/target,id=queryflux-target-py312 \
    bash -o pipefail -c 'rm -rf target/release/build/pyo3-* target/release/build/pyo3_ffi-* 2>/dev/null || true; \
    cargo chef cook --release --locked --recipe-path recipe.json --package queryflux'

COPY crates ./crates

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=queryflux-registry \
    --mount=type=cache,target=/build/target,id=queryflux-target-py312 \
    bash -o pipefail -c 'rm -rf target/release/build/pyo3-* target/release/build/pyo3_ffi-* \
      target/release/deps/libpyo3_ffi-* \
      target/release/.fingerprint/pyo3-* target/release/.fingerprint/pyo3_ffi-* 2>/dev/null || true; \
    cargo build --release -p queryflux --bin queryflux && \
    cp target/release/queryflux /queryflux-binary && \
    cp target/release/deps/libduckdb.so /queryflux-libduckdb.so'

# =============================================================================
# Stage 4: Runtime
# =============================================================================
FROM python:3.12.13-slim-trixie

RUN apt-get update && apt-get install --no-install-recommends -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 1000 queryflux \
    && mkdir -p /app \
    && chown -R queryflux:queryflux /app

COPY requirements.txt /tmp/requirements.txt
RUN python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

COPY --from=builder /queryflux-binary /app/queryflux
COPY --from=builder /queryflux-libduckdb.so /usr/local/lib/libduckdb.so
COPY docker/queryflux/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/queryflux /app/entrypoint.sh

# Prebuilt libduckdb (DUCKDB_DOWNLOAD_LIB) is dynamically linked; loader search path must include it.
ENV LD_LIBRARY_PATH=/usr/local/lib
ENV PYO3_PYTHON=/opt/venv/bin/python3
WORKDIR /app
USER queryflux

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["--config", "/etc/queryflux/config.yaml"]
