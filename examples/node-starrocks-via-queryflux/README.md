# Node.js → QueryFlux (MySQL wire / StarRocks path)

Uses [`mysql2`](https://github.com/sidorares/node-mysql2) against QueryFlux’s **MySQL wire** frontend. With routing like [`examples/full-stack/config.yaml`](../full-stack/config.yaml) (`mysqlWire` → `starrocks-default`), queries run on **StarRocks**.

## Prerequisite

A stack where QueryFlux listens on MySQL wire (e.g. full-stack with `3306` published):

```bash
cd examples/full-stack
docker compose up -d --wait
# optional: loader + starrocks catalog — see examples README
```

## Run

```bash
cd examples/node-starrocks-via-queryflux
npm install
npm start
```

Override connection (defaults match full-stack cluster auth on the host):

```bash
QUERYFLUX_MYSQL_HOST=127.0.0.1 QUERYFLUX_MYSQL_PORT=3306 \
QUERYFLUX_MYSQL_USER=root QUERYFLUX_MYSQL_PASSWORD= \
npm start
```

From another container on the same Compose network, use host `queryflux` and port `3306`.

Copy `.env.example` to `.env` if you use a tool that loads it; this sample only reads `process.env` (Node does not load `.env` by itself).
