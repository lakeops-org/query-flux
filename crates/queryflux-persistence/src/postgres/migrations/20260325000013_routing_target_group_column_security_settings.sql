-- Routing: store target cluster group on each row (target_group_id); definitions use internal
-- `_qf*` leg types or compound/pythonScript without embedded group names.
-- Drop junction table. Replace proxy_settings with security_settings (singleton JSON).

DROP TABLE IF EXISTS routing_rule_target_groups;

ALTER TABLE routing_rules
    ADD COLUMN IF NOT EXISTS router_logical_index INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS slice_index INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS target_group_id BIGINT REFERENCES cluster_group_configs (id) ON DELETE RESTRICT;

UPDATE routing_rules SET router_logical_index = sort_order;

CREATE INDEX IF NOT EXISTS routing_rules_target_group_id_idx ON routing_rules (target_group_id);

-- Security config: dedicated table (replaces proxy_settings key-value).
CREATE TABLE IF NOT EXISTS security_settings (
    singleton   BOOLEAN PRIMARY KEY DEFAULT TRUE,
    config      JSONB        NOT NULL DEFAULT '{}',
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT security_settings_singleton CHECK (singleton = TRUE)
);

INSERT INTO security_settings (singleton, config)
SELECT TRUE, value
FROM proxy_settings
WHERE key = 'security_config'
ON CONFLICT (singleton) DO UPDATE SET
    config = EXCLUDED.config,
    updated_at = now();

INSERT INTO security_settings (singleton, config)
SELECT TRUE, '{}'::jsonb
WHERE NOT EXISTS (SELECT 1 FROM security_settings);

DROP TABLE IF EXISTS proxy_settings;
