-- Add allow_groups / allow_users to cluster group configs.
-- These drive the SimpleAuthorizationPolicy when authorization.provider = none.
ALTER TABLE cluster_group_configs
  ADD COLUMN IF NOT EXISTS allow_groups TEXT[] NOT NULL DEFAULT '{}',
  ADD COLUMN IF NOT EXISTS allow_users  TEXT[] NOT NULL DEFAULT '{}';

-- Generic proxy-level settings store.
-- Keys used: 'auth_config', 'authz_config', 'routers_config'
-- Each value is the full JSON blob for that config section.
-- QueryFlux reads these at startup (after Postgres connect) to override YAML config.
CREATE TABLE IF NOT EXISTS proxy_settings (
    key        TEXT        PRIMARY KEY,
    value      JSONB       NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
