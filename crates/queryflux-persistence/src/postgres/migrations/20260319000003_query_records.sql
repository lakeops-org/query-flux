CREATE TABLE IF NOT EXISTS query_records (
    id                    BIGSERIAL    PRIMARY KEY,
    proxy_query_id        TEXT         NOT NULL,
    cluster_group         TEXT         NOT NULL,
    cluster_name          TEXT         NOT NULL,
    engine_type           TEXT         NOT NULL,
    frontend_protocol     TEXT         NOT NULL,
    source_dialect        TEXT         NOT NULL,
    target_dialect        TEXT         NOT NULL,
    was_translated        BOOLEAN      NOT NULL DEFAULT false,
    username              TEXT,
    catalog               TEXT,
    db_name               TEXT,
    sql_preview           TEXT         NOT NULL DEFAULT '',
    status                TEXT         NOT NULL,
    routing_trace         JSONB,
    queue_duration_ms     BIGINT       NOT NULL DEFAULT 0,
    execution_duration_ms BIGINT       NOT NULL DEFAULT 0,
    rows_returned         BIGINT,
    error_message         TEXT,
    created_at            TIMESTAMPTZ  NOT NULL
);

CREATE INDEX IF NOT EXISTS query_records_created_at    ON query_records (created_at DESC);
CREATE INDEX IF NOT EXISTS query_records_cluster_group ON query_records (cluster_group, created_at DESC);
CREATE INDEX IF NOT EXISTS query_records_status        ON query_records (status, created_at DESC);
CREATE INDEX IF NOT EXISTS query_records_proxy_id      ON query_records (proxy_query_id);
