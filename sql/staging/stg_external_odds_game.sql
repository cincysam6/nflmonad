-- =============================================================================
-- STAGING: stg_external_odds_game
-- Source:  raw_external_odds (placeholder — empty until odds ingestion runs)
-- Grain:   game_id + market_timestamp + sportsbook
-- =============================================================================

CREATE OR REPLACE VIEW stg_external_odds_game AS
SELECT
  CAST(NULL AS VARCHAR)   AS game_id,
  CAST(NULL AS INTEGER)   AS season,
  CAST(NULL AS INTEGER)   AS week,
  CAST(NULL AS DATE)      AS game_date,
  CAST(NULL AS VARCHAR)   AS home_team,
  CAST(NULL AS VARCHAR)   AS away_team,
  CAST(NULL AS TIMESTAMP) AS market_timestamp,
  CAST(NULL AS VARCHAR)   AS sportsbook,
  CAST(NULL AS VARCHAR)   AS market_type,
  CAST(NULL AS DOUBLE)    AS home_spread,
  CAST(NULL AS DOUBLE)    AS away_spread,
  CAST(NULL AS DOUBLE)    AS total_line,
  CAST(NULL AS DOUBLE)    AS opening_spread,
  CAST(NULL AS DOUBLE)    AS opening_total,
  CAST(NULL AS DOUBLE)    AS closing_spread,
  CAST(NULL AS DOUBLE)    AS closing_total,
  CAST(NULL AS DOUBLE)    AS home_ml,
  CAST(NULL AS DOUBLE)    AS away_ml,
  CAST(NULL AS TIMESTAMP) AS ingestion_ts
WHERE FALSE;
;
