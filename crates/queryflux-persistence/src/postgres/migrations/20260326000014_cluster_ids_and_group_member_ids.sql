-- Stable numeric ids for clusters. Group `members` is BIGINT[] of cluster_configs.id.
-- Names are resolved with a join when loading groups. Drops redundant cluster_group_members.

ALTER TABLE cluster_configs ADD COLUMN id BIGSERIAL;
CREATE UNIQUE INDEX cluster_configs_id_key ON cluster_configs (id);

ALTER TABLE cluster_group_configs ADD COLUMN members_new BIGINT[] NOT NULL DEFAULT '{}';

UPDATE cluster_group_configs g
SET members_new = COALESCE(
    (
        SELECT array_agg(c.id ORDER BY u.ord)
        FROM unnest(g.members) WITH ORDINALITY AS u(mname, ord)
        JOIN cluster_configs c ON c.name = u.mname
    ),
    '{}'
);

ALTER TABLE cluster_group_configs DROP COLUMN members;
ALTER TABLE cluster_group_configs RENAME COLUMN members_new TO members;

DROP TABLE IF EXISTS cluster_group_members;
