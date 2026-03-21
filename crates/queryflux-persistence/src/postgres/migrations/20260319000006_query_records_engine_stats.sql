ALTER TABLE query_records
    ADD COLUMN IF NOT EXISTS cpu_time_ms         BIGINT,
    ADD COLUMN IF NOT EXISTS processed_rows      BIGINT,
    ADD COLUMN IF NOT EXISTS processed_bytes     BIGINT,
    ADD COLUMN IF NOT EXISTS physical_input_bytes BIGINT,
    ADD COLUMN IF NOT EXISTS peak_memory_bytes   BIGINT,
    ADD COLUMN IF NOT EXISTS spilled_bytes       BIGINT,
    ADD COLUMN IF NOT EXISTS total_splits        INT;
