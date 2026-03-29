-- Standalone migration: UNIQUE keys required for ON CONFLICT upserts (PostgreSQL 42P10).
--
-- Older DBs may have route_* tables created with only a surrogate PK (e.g. id) or no
-- composite key. The previous migration only added constraints when the table had *no*
-- primary key at all, so id-only tables still failed ON CONFLICT on (feed_id, ...).
--
-- Run as a role that can ALTER these tables (database owner or superuser), e.g.:
--   docker compose exec postgis psql -U user -d israel_gtfs -f /backend/db_migration_route_cache_pk.sql
-- Or on host:
--   psql -h localhost -U postgres -d israel_gtfs -f backend/db_migration_route_cache_pk.sql

DO $upgrade_route_cache_pk$
BEGIN
    -- route_signatures: ON CONFLICT (feed_id, route_id, direction_id)
    IF to_regclass('public.route_signatures') IS NOT NULL
       AND NOT EXISTS (
           SELECT 1
           FROM pg_constraint c
           JOIN pg_class t ON c.conrelid = t.oid
           JOIN pg_namespace n ON t.relnamespace = n.oid
           WHERE n.nspname = 'public'
             AND t.relname = 'route_signatures'
             AND c.contype IN ('p', 'u')
             AND (
                 SELECT coalesce(array_agg(a.attname ORDER BY a.attname), ARRAY[]::name[])
                 FROM unnest(c.conkey) AS ck(attnum)
                 JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ck.attnum
             ) = ARRAY['direction_id', 'feed_id', 'route_id']::name[]
       )
    THEN
        DELETE FROM route_signatures a
        USING route_signatures b
        WHERE a.ctid < b.ctid
          AND a.feed_id = b.feed_id
          AND a.route_id = b.route_id
          AND COALESCE(a.direction_id, -1) = COALESCE(b.direction_id, -1);
        ALTER TABLE route_signatures
            ADD CONSTRAINT route_signatures_upsert_uq
            UNIQUE (feed_id, route_id, direction_id);
    END IF;

    -- route_graph_cache: ON CONFLICT (feed_id, route_id, direction_id, pretty_osm)
    IF to_regclass('public.route_graph_cache') IS NOT NULL
       AND NOT EXISTS (
           SELECT 1
           FROM pg_constraint c
           JOIN pg_class t ON c.conrelid = t.oid
           JOIN pg_namespace n ON t.relnamespace = n.oid
           WHERE n.nspname = 'public'
             AND t.relname = 'route_graph_cache'
             AND c.contype IN ('p', 'u')
             AND (
                 SELECT coalesce(array_agg(a.attname ORDER BY a.attname), ARRAY[]::name[])
                 FROM unnest(c.conkey) AS ck(attnum)
                 JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ck.attnum
             ) = ARRAY['direction_id', 'feed_id', 'pretty_osm', 'route_id']::name[]
       )
    THEN
        DELETE FROM route_graph_cache a
        USING route_graph_cache b
        WHERE a.ctid < b.ctid
          AND a.feed_id = b.feed_id
          AND a.route_id = b.route_id
          AND COALESCE(a.direction_id, -1) = COALESCE(b.direction_id, -1)
          AND a.pretty_osm = b.pretty_osm;
        ALTER TABLE route_graph_cache
            ADD CONSTRAINT route_graph_cache_upsert_uq
            UNIQUE (feed_id, route_id, direction_id, pretty_osm);
    END IF;

    -- route_preview_cache: ON CONFLICT (feed_id, route_id, direction_id, profile_key, pretty_osm)
    IF to_regclass('public.route_preview_cache') IS NOT NULL
       AND NOT EXISTS (
           SELECT 1
           FROM pg_constraint c
           JOIN pg_class t ON c.conrelid = t.oid
           JOIN pg_namespace n ON t.relnamespace = n.oid
           WHERE n.nspname = 'public'
             AND t.relname = 'route_preview_cache'
             AND c.contype IN ('p', 'u')
             AND (
                 SELECT coalesce(array_agg(a.attname ORDER BY a.attname), ARRAY[]::name[])
                 FROM unnest(c.conkey) AS ck(attnum)
                 JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ck.attnum
             ) = ARRAY['direction_id', 'feed_id', 'pretty_osm', 'profile_key', 'route_id']::name[]
       )
    THEN
        DELETE FROM route_preview_cache a
        USING route_preview_cache b
        WHERE a.ctid < b.ctid
          AND a.feed_id = b.feed_id
          AND a.route_id = b.route_id
          AND COALESCE(a.direction_id, -1) = COALESCE(b.direction_id, -1)
          AND a.profile_key = b.profile_key
          AND a.pretty_osm = b.pretty_osm;
        ALTER TABLE route_preview_cache
            ADD CONSTRAINT route_preview_cache_upsert_uq
            UNIQUE (feed_id, route_id, direction_id, profile_key, pretty_osm);
    END IF;
EXCEPTION
    WHEN duplicate_object THEN
        NULL;
    WHEN unique_violation THEN
        RAISE NOTICE
            'route cache upsert key upgrade: unique_violation (duplicates remain?). TRUNCATE route_graph_cache, route_preview_cache, route_signatures and retry.';
END
$upgrade_route_cache_pk$;
