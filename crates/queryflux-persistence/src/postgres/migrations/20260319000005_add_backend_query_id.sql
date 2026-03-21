ALTER TABLE query_records ADD COLUMN IF NOT EXISTS backend_query_id TEXT;
CREATE INDEX IF NOT EXISTS query_records_backend_id ON query_records (backend_query_id) WHERE backend_query_id IS NOT NULL;
