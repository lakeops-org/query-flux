CREATE TABLE IF NOT EXISTS cluster_snapshots (
    id                  BIGSERIAL    PRIMARY KEY,
    cluster_name        TEXT         NOT NULL,
    group_name          TEXT         NOT NULL,
    engine_type         TEXT         NOT NULL,
    running_queries     INT          NOT NULL,
    queued_queries      INT          NOT NULL,
    max_running_queries INT          NOT NULL,
    recorded_at         TIMESTAMPTZ  NOT NULL
);

CREATE INDEX IF NOT EXISTS cluster_snapshots_recorded_at ON cluster_snapshots (recorded_at DESC);
CREATE INDEX IF NOT EXISTS cluster_snapshots_group       ON cluster_snapshots (group_name, recorded_at DESC);
