CREATE TABLE IF NOT EXISTS queued_queries (
    id         TEXT        PRIMARY KEY,
    data       JSONB       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS queued_queries_created_at ON queued_queries (created_at);
