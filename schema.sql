-- ============================================================================
-- Valorant Esports Match Analytics — PostgreSQL Schema
-- ============================================================================
-- Three normalized tables for match data ingestion from HenrikDev API payloads.
--
-- Usage:
--   psql -U <user> -d <database> -f schema.sql
-- ============================================================================

-- Drop existing tables (safe for dev — remove in production)
DROP TABLE IF EXISTS kill_events CASCADE;
DROP TABLE IF EXISTS player_match_stats CASCADE;
DROP TABLE IF EXISTS matches CASCADE;

-- --------------------------------------------------------------------------
-- 1. matches — One row per match
-- --------------------------------------------------------------------------
CREATE TABLE matches (
    match_id        UUID PRIMARY KEY,
    map_name        VARCHAR(50)  NOT NULL,
    game_mode       VARCHAR(50)  NOT NULL,
    game_start      TIMESTAMPTZ  NOT NULL,
    game_length_ms  INTEGER      NOT NULL,
    rounds_played   SMALLINT     NOT NULL,
    winning_team    VARCHAR(10)  NOT NULL,
    region          VARCHAR(10),
    cluster         VARCHAR(30),
    game_version    VARCHAR(80),
    season_id       UUID,
    blue_rounds_won SMALLINT,
    red_rounds_won  SMALLINT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE matches IS 'Core match metadata — one row per Valorant match.';

-- --------------------------------------------------------------------------
-- 2. player_match_stats — One row per player per match (10 rows / match)
-- --------------------------------------------------------------------------
CREATE TABLE player_match_stats (
    id              SERIAL PRIMARY KEY,
    match_id        UUID         NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    puuid           VARCHAR(128) NOT NULL,
    game_name       VARCHAR(50)  NOT NULL,
    tag_line        VARCHAR(20)  NOT NULL,
    team            VARCHAR(10)  NOT NULL,
    agent           VARCHAR(30)  NOT NULL,
    -- Core combat stats
    kills           SMALLINT     NOT NULL DEFAULT 0,
    deaths          SMALLINT     NOT NULL DEFAULT 0,
    assists         SMALLINT     NOT NULL DEFAULT 0,
    score           INTEGER      NOT NULL DEFAULT 0,
    -- Shot distribution
    headshots       SMALLINT     NOT NULL DEFAULT 0,
    bodyshots       SMALLINT     NOT NULL DEFAULT 0,
    legshots        SMALLINT     NOT NULL DEFAULT 0,
    -- Damage
    damage_made     INTEGER      NOT NULL DEFAULT 0,
    damage_received INTEGER      NOT NULL DEFAULT 0,
    -- Nested objects stored as JSONB for flexible querying
    ability_casts   JSONB,
    economy         JSONB,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE player_match_stats IS 'Per-player stats within a match (10 rows per match).';

-- Index for common query patterns
CREATE INDEX idx_pms_match_id ON player_match_stats(match_id);
CREATE INDEX idx_pms_puuid    ON player_match_stats(puuid);
CREATE INDEX idx_pms_agent    ON player_match_stats(agent);

-- --------------------------------------------------------------------------
-- 3. kill_events — One row per kill (variable per match)
-- --------------------------------------------------------------------------
CREATE TABLE kill_events (
    id                  SERIAL PRIMARY KEY,
    match_id            UUID         NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    round               SMALLINT     NOT NULL,
    kill_time_in_round  INTEGER      NOT NULL,  -- milliseconds
    kill_time_in_match  INTEGER      NOT NULL,  -- milliseconds
    killer_puuid        VARCHAR(128) NOT NULL,
    victim_puuid        VARCHAR(128) NOT NULL,
    weapon              VARCHAR(50),
    damage_type         VARCHAR(20)  NOT NULL,
    -- Granular spatial data for heatmap / positional analysis
    killer_x            FLOAT,
    killer_y            FLOAT,
    victim_x            FLOAT        NOT NULL,
    victim_y            FLOAT        NOT NULL,
    -- Assistants stored as JSONB array of PUUIDs
    assistants          JSONB,
    finishing_damage     JSONB,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE kill_events IS 'Individual kill events with spatial coordinates for positional analytics.';

-- Index for common query patterns
CREATE INDEX idx_ke_match_id     ON kill_events(match_id);
CREATE INDEX idx_ke_killer_puuid ON kill_events(killer_puuid);
CREATE INDEX idx_ke_victim_puuid ON kill_events(victim_puuid);
CREATE INDEX idx_ke_weapon       ON kill_events(weapon);
CREATE INDEX idx_ke_round        ON kill_events(match_id, round);
