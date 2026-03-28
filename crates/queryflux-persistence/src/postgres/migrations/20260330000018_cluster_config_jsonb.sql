-- Collapse all engine-specific flat columns into a single `config JSONB` column.
-- Keeps `enabled`, `max_running_queries`, `engine_key` as first-class columns
-- since they are operational/routing concerns shared by every engine type.

-- 1. Add the new column.
ALTER TABLE cluster_configs
    ADD COLUMN IF NOT EXISTS config JSONB NOT NULL DEFAULT '{}';

-- 2. Backfill from existing flat columns.
--    Uses CASE to omit tlsInsecureSkipVerify when it is false (so it doesn't
--    clutter the JSON for the vast majority of clusters that don't use it).
UPDATE cluster_configs
SET config = jsonb_strip_nulls(jsonb_build_object(
    'endpoint',              endpoint,
    'databasePath',          database_path,
    'authType',              auth_type,
    'authUsername',          auth_username,
    'authPassword',          auth_password,
    'authToken',             auth_token,
    'tlsInsecureSkipVerify', CASE WHEN tls_insecure_skip_verify THEN true ELSE NULL END,
    'region',                region,
    's3OutputLocation',      s3_output_location,
    'workgroup',             workgroup,
    'catalog',               catalog
));

-- 3. Drop the old flat columns.
ALTER TABLE cluster_configs
    DROP COLUMN IF EXISTS endpoint,
    DROP COLUMN IF EXISTS database_path,
    DROP COLUMN IF EXISTS auth_type,
    DROP COLUMN IF EXISTS auth_username,
    DROP COLUMN IF EXISTS auth_password,
    DROP COLUMN IF EXISTS auth_token,
    DROP COLUMN IF EXISTS tls_insecure_skip_verify,
    DROP COLUMN IF EXISTS region,
    DROP COLUMN IF EXISTS s3_output_location,
    DROP COLUMN IF EXISTS workgroup,
    DROP COLUMN IF EXISTS catalog;
