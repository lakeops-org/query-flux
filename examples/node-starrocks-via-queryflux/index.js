/**
 * Minimal Node client: MySQL wire → QueryFlux → StarRocks (when routing matches full-stack).
 *
 *   cd examples/node-starrocks-via-queryflux && npm install && npm start
 *
 * Optional: copy .env.example to .env and adjust (this script only reads process.env).
 */

import mysql from "mysql2/promise";

function env(name, fallback = "") {
  const v = process.env[name];
  return v === undefined || v === "" ? fallback : v;
}

const host = env("QUERYFLUX_MYSQL_HOST", "127.0.0.1");
const port = Number.parseInt(env("QUERYFLUX_MYSQL_PORT", "3306"), 10);
const user = env("QUERYFLUX_MYSQL_USER", "root");
const password = env("QUERYFLUX_MYSQL_PASSWORD", "");
const database = env("QUERYFLUX_MYSQL_DATABASE", undefined);

async function main() {
  console.error(
    `Connecting mysql2 → QueryFlux at ${host}:${port} as ${user}…`,
  );

  const conn = await mysql.createConnection({
    host,
    port,
    user,
    password,
    ...(database ? { database } : {}),
    connectTimeout: 15_000,
  });

  try {
    const [rows] = await conn.query("SELECT 1 AS ok");
    console.log("SELECT 1:", rows);

    const [ver] = await conn.query("SELECT VERSION() AS version");
    console.log("VERSION():", ver);
  } finally {
    await conn.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
