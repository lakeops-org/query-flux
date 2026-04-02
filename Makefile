CARGO        := $(HOME)/.cargo/bin/cargo
COMPOSE      := docker compose -f docker/docker-compose.yml --project-directory .
COMPOSE_TEST := docker compose -f docker/docker-compose.test.yml --project-directory .

# Trino `tpch` schema used when loading Iceberg tables (see docker/fixtures/init.sql + data-loader).
# tiny = default fast tests; sf1 ≈ 1.5M orders (long load, heavy E2E).
TPCH_SCALE ?= tiny
export TPCH_SCALE

.PHONY: dev stop logs build lint clippy check test benchmark test-e2e clean setup

## Create virtualenv and install Python dependencies (sqlglot etc.)
setup:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt
	@echo "Python env ready. Run: export PYO3_PYTHON=$$(pwd)/.venv/bin/python3"

## Start all services (Trino, StarRocks, Lakekeeper + MinIO, Postgres, observability),
## load TPC-H data into Iceberg, then run QueryFlux locally.
env:
	@test -f .venv/bin/python3 || (echo "Run 'make setup' first" && exit 1)
	@pkill -f "queryflux.*config.local.yaml" 2>/dev/null; true
	$(COMPOSE) up -d --wait trino starrocks postgres sentinel
	$(COMPOSE) run --rm -T data-loader
	$(COMPOSE) run --rm -T starrocks-catalog-setup


server:
	PYO3_PYTHON=$(shell pwd)/.venv/bin/python3 \
	PYTHONPATH=$(shell pwd)/.venv/lib/python3.13/site-packages \
	RUST_LOG=queryflux=info,queryflux_frontend=info \
	DUCKDB_DOWNLOAD_LIB=1 \
	$(CARGO) run --bin queryflux -- --config config.local.yaml
## Stop Docker services and any running QueryFlux process
stop:
	@pkill -f "queryflux.*config.local.yaml" 2>/dev/null; true
	$(COMPOSE) down

## Stream logs from Docker services
logs:
	$(COMPOSE) logs -f

## Build the proxy binary (release mode)
build:
	$(CARGO) build --release

## Run clippy lints (no external services needed).
lint: clippy
clippy:
	$(CARGO) clippy --all-targets --all-features -- -D warnings

## Run unit/integration tests (no external services needed).
## PYO3_PYTHON + PYTHONPATH: PyO3 (routing + translation). The venv must include `sqlglot`
## (`pip install -r requirements.txt` via `make setup`) for `queryflux-translation` transform tests.
test:
	@test -f .venv/bin/python3 || (echo "Run 'make setup' first" && exit 1)
	PYO3_PYTHON=$(shell pwd)/.venv/bin/python3 \
	PYTHONPATH=$(shell pwd)/.venv/lib/python3.13/site-packages \
	$(CARGO) test --tests --workspace --exclude queryflux-e2e-tests

## Micro-benchmark: mock Trino + StarRocks backends vs QueryFlux (release build).
## Optional: QUERYFLUX_BENCH_WARMUP, QUERYFLUX_BENCH_ITERATIONS, QUERYFLUX_BENCH_TRINO_POLL.
benchmark:
	@test -f .venv/bin/python3 || (echo "Run 'make setup' first" && exit 1)
	PYO3_PYTHON=$(shell pwd)/.venv/bin/python3 \
	PYTHONPATH=$(shell pwd)/.venv/lib/python3.13/site-packages \
	$(CARGO) build --release --bin queryflux
	PYO3_PYTHON=$(shell pwd)/.venv/bin/python3 \
	PYTHONPATH=$(shell pwd)/.venv/lib/python3.13/site-packages \
	$(CARGO) run --release -p queryflux-bench

## Run E2E tests. Spins up Trino + StarRocks + Lakekeeper via Docker.
## Requires reachable engines; see docker/docker-compose.test.yml.
## `--test-threads=1`: StarRocks Iceberg is slow; default parallel libtest + `#[serial]` makes
## every test report libtest's 60s "slow test" spam while threads wait on the serial lock.
## Iceberg/Lakekeeper tables are created by the e2e crate (no TPC-H loader).
test-e2e:
	@test -f .venv/bin/python3 || (echo "Run 'make setup' first" && exit 1)
	PYO3_PYTHON=$(shell pwd)/.venv/bin/python3 \
	PYTHONPATH=$(shell pwd)/.venv/lib/python3.13/site-packages \
	$(COMPOSE_TEST) up -d --wait trino starrocks sentinel
	PYO3_PYTHON=$(shell pwd)/.venv/bin/python3 \
	PYTHONPATH=$(shell pwd)/.venv/lib/python3.13/site-packages \
	TRINO_URL=http://localhost:18081 \
	STARROCKS_URL=mysql://root@localhost:9030 \
	LAKEKEEPER_URL=http://localhost:18181 \
	MINIO_ENDPOINT=localhost:19000 \
	DUCKDB_DOWNLOAD_LIB=1 \
	$(CARGO) test -p queryflux-e2e-tests --manifest-path Cargo.toml -- --test-threads=1 --include-ignored --nocapture
	$(COMPOSE_TEST) down

## Remove build artifacts and Docker volumes
clean:
	$(CARGO) clean
	$(COMPOSE) down -v
