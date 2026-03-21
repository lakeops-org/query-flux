CREATE TABLE IF NOT EXISTS executing_queries (
    id         TEXT        PRIMARY KEY,
    data       JSONB       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS executing_queries_created_at ON executing_queries (created_at);
