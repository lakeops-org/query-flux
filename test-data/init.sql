-- Load TPC-H data into Lakekeeper Iceberg tables via Trino.
--
-- Trino (with CATALOG_MANAGEMENT=dynamic) persists the catalog for the
-- container lifetime. StarRocks and DuckDB register their own catalog
-- references at test harness startup.
--
-- Data source: tpch.tiny (built-in Trino connector, ~150 customers, ~1500 orders).

DROP CATALOG IF EXISTS lakekeeper;

CREATE CATALOG lakekeeper USING iceberg
WITH (
    "iceberg.catalog.type" = 'rest',
    "iceberg.rest-catalog.uri" = 'http://lakekeeper:8181/catalog',
    "iceberg.rest-catalog.warehouse" = 'demo',
    "iceberg.rest-catalog.security" = 'NONE',
    "s3.region" = 'local',
    "s3.path-style-access" = 'true',
    -- Use a hostname that works from both:
    -- - Docker network (where Trino runs)
    -- - the host process running DuckDB (used by QueryFlux tests)
    "s3.endpoint" = 'http://host.docker.internal:19000',
    "fs.native-s3.enabled" = 'true',
    "s3.aws-access-key" = 'minio-root-user',
    "s3.aws-secret-key" = 'minio-root-password'
);

CREATE SCHEMA IF NOT EXISTS lakekeeper.tpch;

-- (Re)create TPCH tables with the exact column names expected by tests.
-- We use DROP + CREATE instead of IF NOT EXISTS so repeated test runs
-- don't get stuck with an older schema.
DROP TABLE IF EXISTS lakekeeper.tpch.customer;
DROP TABLE IF EXISTS lakekeeper.tpch.orders;
DROP TABLE IF EXISTS lakekeeper.tpch.nation;

CREATE TABLE lakekeeper.tpch.customer AS
SELECT
    custkey AS c_custkey,
    name AS c_name,
    nationkey AS c_nationkey,
    acctbal AS c_acctbal
FROM tpch.tiny.customer;

CREATE TABLE lakekeeper.tpch.orders AS
SELECT
    orderkey AS o_orderkey,
    custkey AS o_custkey,
    totalprice AS o_totalprice,
    orderstatus AS o_orderstatus
FROM tpch.tiny.orders;

CREATE TABLE lakekeeper.tpch.nation AS
SELECT
    nationkey AS n_nationkey,
    name AS n_name
FROM tpch.tiny.nation;
