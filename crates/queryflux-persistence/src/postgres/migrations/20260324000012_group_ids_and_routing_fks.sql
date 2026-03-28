-- Stable numeric IDs for cluster groups; members reference clusters; routing rules reference groups.

ALTER TABLE cluster_group_configs ADD COLUMN IF NOT EXISTS id BIGSERIAL;

ALTER TABLE cluster_group_configs DROP CONSTRAINT IF EXISTS cluster_group_configs_pkey;
ALTER TABLE cluster_group_configs ADD PRIMARY KEY (id);
CREATE UNIQUE INDEX IF NOT EXISTS cluster_group_configs_name_key ON cluster_group_configs (name);

CREATE TABLE IF NOT EXISTS cluster_group_members (
    group_id      BIGINT NOT NULL REFERENCES cluster_group_configs (id) ON DELETE CASCADE,
    cluster_name  TEXT   NOT NULL REFERENCES cluster_configs (name) ON DELETE CASCADE,
    PRIMARY KEY (group_id, cluster_name)
);

INSERT INTO cluster_group_members (group_id, cluster_name)
SELECT g.id, m.member
FROM cluster_group_configs g
CROSS JOIN LATERAL unnest(g.members) AS m(member)
INNER JOIN cluster_configs c ON c.name = m.member
ON CONFLICT DO NOTHING;

ALTER TABLE routing_settings
    ADD COLUMN IF NOT EXISTS fallback_group_id BIGINT REFERENCES cluster_group_configs (id) ON DELETE SET NULL;

UPDATE routing_settings rs
SET fallback_group_id = g.id
FROM cluster_group_configs g
WHERE rs.routing_fallback IS NOT NULL
  AND btrim(rs.routing_fallback) <> ''
  AND g.name = rs.routing_fallback
  AND rs.fallback_group_id IS NULL;

CREATE TABLE IF NOT EXISTS routing_rule_target_groups (
    routing_rule_id BIGINT NOT NULL REFERENCES routing_rules (id) ON DELETE CASCADE,
    group_id        BIGINT NOT NULL REFERENCES cluster_group_configs (id) ON DELETE RESTRICT,
    PRIMARY KEY (routing_rule_id, group_id)
);

CREATE INDEX IF NOT EXISTS routing_rule_target_groups_group_id_idx ON routing_rule_target_groups (group_id);
