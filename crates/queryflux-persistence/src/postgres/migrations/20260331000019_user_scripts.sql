-- Reusable Python snippets: translation AST fixups (post-sqlglot) and routing helpers.
-- Groups reference translation scripts via group_translation_scripts (ordered).

CREATE TABLE IF NOT EXISTS user_scripts (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT        NOT NULL UNIQUE,
    description TEXT        NOT NULL DEFAULT '',
    kind        TEXT        NOT NULL CHECK (kind IN ('translation_fixup', 'routing')),
    body        TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS user_scripts_kind_idx ON user_scripts (kind);

CREATE TABLE IF NOT EXISTS group_translation_scripts (
    group_id   BIGINT NOT NULL REFERENCES cluster_group_configs (id) ON DELETE CASCADE,
    script_id  BIGINT NOT NULL REFERENCES user_scripts (id) ON DELETE CASCADE,
    sort_order INT    NOT NULL,
    PRIMARY KEY (group_id, sort_order),
    UNIQUE (group_id, script_id)
);

CREATE INDEX IF NOT EXISTS group_translation_scripts_script_id_idx ON group_translation_scripts (script_id);
