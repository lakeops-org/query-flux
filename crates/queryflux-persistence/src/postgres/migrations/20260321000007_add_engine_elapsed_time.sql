ALTER TABLE query_records
    ADD COLUMN IF NOT EXISTS engine_elapsed_time_ms BIGINT;
