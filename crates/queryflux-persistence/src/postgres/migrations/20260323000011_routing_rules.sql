-- Normalized routing: one row per router in `routing_rules`, fallback in `routing_settings`.
-- Replaces the monolithic `proxy_settings` key `routing_config` (migrated on first apply).

CREATE TABLE IF NOT EXISTS routing_settings (
    singleton              BOOLEAN PRIMARY KEY DEFAULT TRUE,
    routing_fallback       TEXT        NOT NULL DEFAULT '',
    routing_persist_active BOOLEAN     NOT NULL DEFAULT FALSE,
    CONSTRAINT routing_settings_singleton CHECK (singleton = TRUE)
);

INSERT INTO routing_settings (singleton, routing_fallback, routing_persist_active)
SELECT TRUE, '', FALSE
WHERE NOT EXISTS (SELECT 1 FROM routing_settings);

CREATE TABLE IF NOT EXISTS routing_rules (
    id          BIGSERIAL PRIMARY KEY,
    sort_order  INT         NOT NULL,
    definition  JSONB       NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS routing_rules_sort_order_idx ON routing_rules (sort_order);

-- Migrate legacy `proxy_settings.routing_config` JSON blob into rows (once).
DO $$
DECLARE
    blob jsonb;
    r    jsonb;
    i    int := 0;
    fb   text;
BEGIN
    SELECT value INTO blob FROM proxy_settings WHERE key = 'routing_config';
    IF blob IS NULL THEN
        RETURN;
    END IF;
    IF EXISTS (SELECT 1 FROM routing_rules LIMIT 1) THEN
        RETURN;
    END IF;

    fb := COALESCE(blob->>'routingFallback', '');
    UPDATE routing_settings
    SET routing_fallback = fb,
        routing_persist_active = TRUE
    WHERE singleton = TRUE;

    FOR r IN SELECT * FROM jsonb_array_elements(COALESCE(blob->'routers', '[]'::jsonb))
    LOOP
        INSERT INTO routing_rules (sort_order, definition) VALUES (i, r);
        i := i + 1;
    END LOOP;

    DELETE FROM proxy_settings WHERE key = 'routing_config';
END $$;
