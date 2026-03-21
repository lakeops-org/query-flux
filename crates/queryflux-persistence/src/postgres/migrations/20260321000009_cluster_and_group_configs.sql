-- Persisted cluster configurations.
-- Populated on first start by seeding from YAML; managed via the Admin API thereafter.
-- When Postgres persistence is configured, QueryFlux reads cluster/group config from
-- these tables instead of the YAML file, allowing runtime edits without a restart of
-- the whole proxy (endpoint/auth changes take effect on next restart; enabled /
-- max_running_queries take effect immediately via the PATCH /admin/clusters endpoint).

CREATE TABLE IF NOT EXISTS cluster_configs (
    name                     TEXT        PRIMARY KEY,
    engine_key               TEXT        NOT NULL,   -- 'trino' | 'duckDb' | 'starRocks' | 'clickHouse'
    endpoint                 TEXT,                   -- HTTP(S) or mysql:// URL; NULL for embedded engines
    database_path            TEXT,                   -- DuckDB file path; NULL for all other engines
    auth_type                TEXT,                   -- 'basic' | 'bearer' | NULL
    auth_username            TEXT,
    auth_password            TEXT,
    auth_token               TEXT,
    tls_insecure_skip_verify BOOLEAN     NOT NULL DEFAULT false,
    enabled                  BOOLEAN     NOT NULL DEFAULT true,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Persisted cluster group configurations.
CREATE TABLE IF NOT EXISTS cluster_group_configs (
    name                TEXT        PRIMARY KEY,
    enabled             BOOLEAN     NOT NULL DEFAULT true,
    members             TEXT[]      NOT NULL DEFAULT '{}',
    max_running_queries BIGINT      NOT NULL DEFAULT 10,
    max_queued_queries  BIGINT,
    strategy            JSONB,                       -- serialised StrategyConfig (null = RoundRobin)
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
