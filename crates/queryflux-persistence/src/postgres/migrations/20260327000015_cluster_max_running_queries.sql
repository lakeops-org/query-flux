-- Per-cluster concurrency cap. NULL = inherit from the cluster group's max_running_queries.
ALTER TABLE cluster_configs
    ADD COLUMN IF NOT EXISTS max_running_queries BIGINT;
