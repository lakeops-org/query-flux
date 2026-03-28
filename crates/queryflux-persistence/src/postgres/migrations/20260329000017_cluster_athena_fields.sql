-- Cloud/Athena fields for cluster_configs.
-- `region` is generic enough for any AWS backend.
-- `s3_output_location` and `workgroup` are Athena-specific.
-- `catalog` is the default Glue catalog (also Athena-specific).
ALTER TABLE cluster_configs
    ADD COLUMN IF NOT EXISTS region             TEXT,
    ADD COLUMN IF NOT EXISTS s3_output_location TEXT,
    ADD COLUMN IF NOT EXISTS workgroup          TEXT,
    ADD COLUMN IF NOT EXISTS catalog            TEXT;
