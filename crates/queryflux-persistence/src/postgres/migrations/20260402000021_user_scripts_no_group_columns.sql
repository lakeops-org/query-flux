-- Scripts are group-agnostic: attachment and order live on cluster_group_configs.translation_script_ids.
ALTER TABLE user_scripts DROP COLUMN IF EXISTS group_name;
ALTER TABLE user_scripts DROP COLUMN IF EXISTS sort_order;
