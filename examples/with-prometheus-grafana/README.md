# QueryFlux + Prometheus + Grafana

Same **Postgres**, **Trino**, and **QueryFlux** stack as [`examples/minimal`](../minimal/), plus **Prometheus** and **Grafana** wired like [`docker/docker-compose.yml`](../../docker/docker-compose.yml).

- **Grafana** provisioning and dashboards come from the repo [`grafana/`](../../grafana/) (`provisioning/`, `dashboards/`).
- **Prometheus** uses the same image, flags, and retention as in `docker/docker-compose.yml`. Scrape targets are defined in [`prometheus.yml`](prometheus.yml) in this folder so the `queryflux` container is scraped on port **9000** (`/metrics`). The top-level [`prometheus/prometheus.yml`](../../prometheus/prometheus.yml) still targets `host.docker.internal` for setups where QueryFlux runs on the host.

## Run

```bash
cd examples/with-prometheus-grafana
docker compose up -d --wait
```

| Service | URL |
|--------|-----|
| Trino via QueryFlux | http://localhost:8080 |
| Trino (direct) | http://localhost:8081 |
| Admin + raw metrics | http://localhost:9000 |
| Prometheus UI | http://localhost:9090 |
| Grafana | http://localhost:3000 — login **admin** / **admin** (anonymous view enabled) |

In Grafana, open **Dashboards** and use the bundled QueryFlux dashboard (provisioned from `grafana/dashboards/`).

## Run SQL through QueryFlux (Trino CLI)

Same as **minimal**: from the host use `http://localhost:8080`, or `docker compose exec -it trino bash` and `trino --server http://queryflux:8080 --user test`. See [`examples/minimal/README.md`](../minimal/README.md#run-sql-with-the-trino-cli).

## Stop

```bash
docker compose down
```

Remove Prometheus/Grafana state volumes as well:

```bash
docker compose down -v
```

## Port note

**Grafana uses localhost:3000** (same as QueryFlux Studio in other examples). Run only one of those stacks at a time, or change the published Grafana port in `docker-compose.yml`.
