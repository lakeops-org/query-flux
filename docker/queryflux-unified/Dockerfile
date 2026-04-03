# syntax=docker/dockerfile:1
# Unified image: QueryFlux server (Rust + PyO3 + DuckDB) + QueryFlux Studio (Next.js standalone).
# Multi-arch: build with buildx, e.g.
#   docker buildx build --platform linux/amd64,linux/arm64 \
#     -f docker/queryflux-unified/Dockerfile -t queryflux:unified --push .
#
# Server-only image remains at docker/queryflux/Dockerfile.

# =============================================================================
# QueryFlux server — chef / planner / builder (keep aligned with docker/queryflux/Dockerfile)
# =============================================================================
FROM python:3.12.13-slim-trixie AS chef

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

FROM chef AS planner

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates

RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS queryflux-builder

ARG TARGETARCH=amd64
ARG CARGO_BUILD_JOBS=default
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}

COPY requirements.txt .
RUN /usr/local/bin/python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt && \
    ln -sfn "$(/opt/venv/bin/python3 -c 'import site; print(site.getsitepackages()[0])')" /opt/venv/site-packages && \
    /opt/venv/bin/python3 -c 'import sys; assert sys.version_info[:2] == (3, 12), f"expected 3.12, got {sys.version}"'

ENV PYO3_PYTHON=/opt/venv/bin/python3
ENV PYTHONPATH=/opt/venv/site-packages
ENV DUCKDB_DOWNLOAD_LIB=1
ENV DUCKDB_STATIC=1
ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/lib/x86_64-linux-gnu:/usr/lib/aarch64-linux-gnu:/usr/lib/arm-linux-gnueabihf:/usr/lib/i386-linux-gnu

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY --from=planner /build/recipe.json recipe.json

COPY crates ./crates
RUN set -eu && find crates -type f -name '*.rs' -delete && \
    for c in queryflux-core queryflux-auth queryflux-persistence queryflux-routing \
        queryflux-translation queryflux-engine-adapters queryflux-frontend \
        queryflux-cluster-manager queryflux-config queryflux-metrics queryflux-e2e-tests; do \
      mkdir -p "crates/$c/src" && echo '// cargo-chef placeholder' > "crates/$c/src/lib.rs"; \
    done && \
    mkdir -p crates/queryflux/src crates/queryflux-bench/src && \
    printf 'fn main() {}\n' | tee crates/queryflux/src/main.rs > crates/queryflux-bench/src/main.rs

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=queryflux-registry \
    --mount=type=cache,target=/build/target,id=queryflux-target-${TARGETARCH} \
    bash -o pipefail -c 'set -eu; \
    if [ "$TARGETARCH" = "amd64" ] || [ "$TARGETARCH" = "arm64" ]; then export RUSTFLAGS="-C link-arg=-fuse-ld=mold"; fi; \
    rm -rf target/release/build/pyo3-* target/release/build/pyo3_ffi-* 2>/dev/null || true; \
    cargo chef cook --release --locked --recipe-path recipe.json --package queryflux'

COPY crates ./crates

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=queryflux-registry \
    --mount=type=cache,target=/build/target,id=queryflux-target-${TARGETARCH} \
    bash -o pipefail -c 'set -eu; \
    if [ "$TARGETARCH" = "amd64" ] || [ "$TARGETARCH" = "arm64" ]; then export RUSTFLAGS="-C link-arg=-fuse-ld=mold"; fi; \
    rm -rf target/release/build/pyo3-* target/release/build/pyo3_ffi-* \
      target/release/deps/libpyo3_ffi-* \
      target/release/.fingerprint/pyo3-* target/release/.fingerprint/pyo3_ffi-* 2>/dev/null || true; \
    cargo build --release -p queryflux --bin queryflux && \
    cp target/release/queryflux /queryflux-binary && \
    cp target/release/deps/libduckdb.so /queryflux-libduckdb.so'

# =============================================================================
# QueryFlux Studio (Next.js standalone)
# =============================================================================
FROM node:20-bookworm-slim AS studio-deps
WORKDIR /studio
COPY queryflux-studio/package.json queryflux-studio/package-lock.json ./
RUN npm ci

FROM node:20-bookworm-slim AS studio-builder
WORKDIR /studio
COPY --from=studio-deps /studio/node_modules ./node_modules
COPY queryflux-studio/ .
ENV NEXT_TELEMETRY_DISABLED=1
RUN npm run build

# =============================================================================
# Runtime: Python venv + Node.js tarball (per TARGETARCH) + both apps
# =============================================================================
FROM python:3.12.13-slim-trixie AS runtime

ARG NODE_VERSION=20.18.1

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    xz-utils \
    tini \
    && DPKG_ARCH=$(dpkg --print-architecture) \
    && case "$DPKG_ARCH" in \
         amd64) NODE_ARCH=x64 ;; \
         arm64) NODE_ARCH=arm64 ;; \
         armhf) NODE_ARCH=armv7l ;; \
         i386)  NODE_ARCH=x86 ;; \
         *) echo "unsupported arch=$DPKG_ARCH"; exit 1 ;; \
       esac \
    && curl -fsSL "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-${NODE_ARCH}.tar.xz" \
         | tar -xJf - -C /usr/local --strip-components=1 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 1000 queryflux \
    && mkdir -p /app/studio \
    && chown -R queryflux:queryflux /app

COPY requirements.txt /tmp/requirements.txt
RUN python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

COPY --from=queryflux-builder /queryflux-binary /app/queryflux
COPY --from=queryflux-builder /queryflux-libduckdb.so /usr/local/lib/libduckdb.so
COPY --from=studio-builder /studio/public /app/studio/public
COPY --from=studio-builder --chown=queryflux:queryflux /studio/.next/standalone /app/studio
COPY --from=studio-builder --chown=queryflux:queryflux /studio/.next/static /app/studio/.next/static

COPY docker/queryflux-unified/entrypoint-unified.sh /app/entrypoint-unified.sh
RUN chmod +x /app/queryflux /app/entrypoint-unified.sh

ENV LD_LIBRARY_PATH=/usr/local/lib
ENV PYO3_PYTHON=/opt/venv/bin/python3
WORKDIR /app
USER queryflux

EXPOSE 3000 8080 9000

ENTRYPOINT ["/usr/bin/tini", "--", "/app/entrypoint-unified.sh"]
CMD ["--config", "/etc/queryflux/config.yaml"]
