-- Load TPC-H data into Lakekeeper Iceberg tables via Trino.
--
-- Trino (with CATALOG_MANAGEMENT=dynamic) persists the catalog for the
-- container lifetime. StarRocks and DuckDB register their own catalog
-- references at test harness startup.
--
-- Data source: tpch.tiny (built-in Trino connector, ~150 customers, ~1500 orders).

CREATE CATALOG lakekeeper USING iceberg
WITH (
    "iceberg.catalog.type" = 'rest',
    "iceberg.rest-catalog.uri" = 'http://lakekeeper:8181/catalog',
    "iceberg.rest-catalog.warehouse" = 'demo',
    "iceberg.rest-catalog.security" = 'NONE',
    "s3.region" = 'local',
    "s3.path-style-access" = 'true',
    "s3.endpoint" = 'http://minio:9000',
    "fs.native-s3.enabled" = 'true',
    "s3.aws-access-key" = 'minio-root-user',
    "s3.aws-secret-key" = 'minio-root-password'
);

CREATE SCHEMA IF NOT EXISTS lakekeeper.tpch;

CREATE TABLE IF NOT EXISTS lakekeeper.tpch.customer AS
SELECT custkey, name, nationkey, acctbal
FROM tpch.tiny.customer;

CREATE TABLE IF NOT EXISTS lakekeeper.tpch.orders AS
SELECT orderkey, custkey, totalprice, orderstatus
FROM tpch.tiny.orders;
