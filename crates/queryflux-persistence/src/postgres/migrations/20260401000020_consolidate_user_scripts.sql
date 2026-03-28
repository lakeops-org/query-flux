-- Consolidate group_translation_scripts into cluster_group_configs.
-- The ordered list of script ids is stored as BIGINT[] on the group row.
--
-- Safe if the join table never existed (e.g. only user_scripts was created, or a fresh DB).

ALTER TABLE cluster_group_configs
    ADD COLUMN IF NOT EXISTS translation_script_ids BIGINT[] NOT NULL DEFAULT '{}';

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'group_translation_scripts'
    ) THEN
        UPDATE cluster_group_configs g
        SET translation_script_ids = sub.ids
        FROM (
            SELECT group_id,
                   array_agg(script_id ORDER BY sort_order) AS ids
            FROM group_translation_scripts
            GROUP BY group_id
        ) sub
        WHERE g.id = sub.group_id;

        DROP TABLE group_translation_scripts;
    END IF;
END $$;
