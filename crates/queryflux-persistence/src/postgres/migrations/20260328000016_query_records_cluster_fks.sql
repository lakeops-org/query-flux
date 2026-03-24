-- Add FK references from query_records to cluster_group_configs and cluster_configs.
-- ON DELETE SET NULL so that deleting a cluster/group doesn't wipe history rows.
ALTER TABLE query_records
    ADD COLUMN IF NOT EXISTS cluster_group_id BIGINT
        REFERENCES cluster_group_configs(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS cluster_id BIGINT
        REFERENCES cluster_configs(id) ON DELETE SET NULL;

-- Backfill from the existing denormalised name columns.
UPDATE query_records qr
SET cluster_group_id = g.id
FROM cluster_group_configs g
WHERE g.name = qr.cluster_group;

UPDATE query_records qr
SET cluster_id = c.id
FROM cluster_configs c
WHERE c.name = qr.cluster_name;

CREATE INDEX IF NOT EXISTS query_records_cluster_group_id
    ON query_records (cluster_group_id, created_at DESC);
CREATE INDEX IF NOT EXISTS query_records_cluster_id
    ON query_records (cluster_id, created_at DESC);
