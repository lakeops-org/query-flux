# Minimal QueryFlux example (in-memory persistence)

Same Trino-through-QueryFlux shape as [`examples/minimal`](../minimal/), but **no Postgres**. QueryFlux uses `persistence.type: inMemory` so cluster definitions, routing, and `config.yaml` stay in the process; in-flight query state is not shared across replicas and is lost on restart.

## Start the stack

From this directory:

```bash
docker compose up -d --wait
```

Uses the **same host ports** as `minimal/` (`8080`, `8081`, `3000`, `9000`). Stop the other example first if it is running.

| Service | URL |
|--------|-----|
| Trino over HTTP via QueryFlux | http://localhost:8080 |
| Trino (direct) | http://localhost:8081 |
| Admin API | http://localhost:9000 |
| Studio | http://localhost:3000 |

## Run SQL with the Trino CLI

Traffic must go to **QueryFlux** on the Trino HTTP port (`8080` in this compose file), not straight to the Trino coordinator on `8081` (host) or `localhost:8080` inside the Trino container.

Install the CLI from Trino’s docs if you run it on your machine: **[Command line interface](https://trino.io/docs/current/client/cli.html)** (requirements, download, and `java -jar` fallback on Windows).

### From your machine (host)

After you have the executable JAR, make it runnable (the docs often rename it to `trino`). From the **host**, QueryFlux is published on `localhost:8080`:

```bash
./trino --server http://localhost:8080 --user test
```

If you did not rename the JAR, use `./<that-filename>` or `java -jar …` as in the Trino docs.

### From inside the Trino container

The `trinodb/trino` image includes the **`trino`** CLI on `PATH`. Open a shell in the **Trino** service and use the **QueryFlux** hostname on the Compose network:

```bash
docker compose exec -it trino bash
```

If `bash` is not available, try `docker compose exec -it trino sh`.

```bash
trino --server http://queryflux:8080 --user test
```

At the `trino>` prompt:

```sql
select 1;
```

Type `quit` or **Ctrl+D** to exit the CLI, then `exit` to leave the container shell.

**Contrast:** `trino --server http://localhost:8080` **inside** the Trino container talks only to the coordinator in that container (bypasses QueryFlux). `trino --server http://queryflux:8080` goes **through QueryFlux**, same idea as `http://localhost:8080` on the host.

### If queries seem to skip QueryFlux

| You run the CLI on… | Use this `--server` | Through QueryFlux? |
|---------------------|---------------------|--------------------|
| **Host** | `http://localhost:8080` | Yes |
| **Host** | `http://localhost:8081` | **No** (Trino direct) |
| **Inside `trino` container** | `http://queryflux:8080` | Yes |
| **Inside `trino` container** | `http://localhost:8080` or `http://trino:8080` | **No** |

Run `docker compose logs -f queryflux` and execute a query; if QueryFlux is in the path you should see `New query submitted`. If not, fix `--server` using the table above.

## Studio

You can open **http://localhost:3000**. Endpoints that **require Postgres** (query history, persisted cluster/group CRUD, routing scripts in DB, etc.) return **503** with in-memory mode. Pages that only need live cluster snapshots or static registry data may still load.

For the full Studio experience with query history and persisted config, use **[`examples/minimal`](../minimal/)** instead.

## Change configuration

Edit `config.yaml` and **restart** QueryFlux (e.g. `docker compose restart queryflux`). There is no DB sync for clusters or groups.

## Stop

```bash
docker compose down
```
