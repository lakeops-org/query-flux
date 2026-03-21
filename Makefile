CARGO        := $(HOME)/.cargo/bin/cargo
COMPOSE      := docker compose -f docker/docker-compose.yml --project-directory .
COMPOSE_TEST := docker compose -f docker/docker-compose.test.yml --project-directory .

.PHONY: dev stop logs build check test-e2e clean setup

## Create virtualenv and install Python dependencies (sqlglot etc.)
setup:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt
	@echo "Python env ready. Run: export PYO3_PYTHON=$$(pwd)/.venv/bin/python3"

## Start all services (Trino, StarRocks, Lakekeeper + MinIO, Postgres, observability),
## load TPC-H data into Iceberg, then run QueryFlux locally.
dev:
	@test -f .venv/bin/python3 || (echo "Run 'make setup' first" && exit 1)
	@pkill -f "queryflux.*config.local.yaml" 2>/dev/null; true
	$(COMPOSE) up -d --wait trino starrocks postgres sentinel
	$(COMPOSE) run --rm -T data-loader
	$(COMPOSE) run --rm -T starrocks-catalog-setup
	PYO3_PYTHON=$(shell pwd)/.venv/bin/python3 \
	PYTHONPATH=$(shell pwd)/.venv/lib/python3.13/site-packages \
	RUST_LOG=queryflux=info,queryflux_frontend=info \
	# $(CARGO) run --bin queryflux -- --config config.local.yaml

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

## Run clippy + unit tests (no external services needed)
check:
	$(CARGO) clippy --all-targets --all-features -- -D warnings
	$(CARGO) test --workspace --exclude queryflux-e2e-tests

## Run E2E tests. Spins up Trino + StarRocks + Lakekeeper via Docker.
## DuckDB tests always run (embedded). Iceberg tests require all services.
test-e2e:
	@test -f .venv/bin/python3 || (echo "Run 'make setup' first" && exit 1)
	$(COMPOSE_TEST) up -d --wait trino starrocks sentinel
	$(COMPOSE_TEST) run --rm -T data-loader
	PYO3_PYTHON=$(shell pwd)/.venv/bin/python3 \
	PYTHONPATH=$(shell pwd)/.venv/lib/python3.13/site-packages \
	$(CARGO) test -p queryflux-e2e-tests --manifest-path Cargo.toml -- --include-ignored --nocapture
	$(COMPOSE_TEST) down

## Remove build artifacts and Docker volumes
clean:
	$(CARGO) clean
	$(COMPOSE) down -v
