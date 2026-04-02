-- =============================================================================
-- STAGING: stg_external_odds_game
-- Source:  raw_external_odds (placeholder — empty until odds ingestion runs)
-- Grain:   game_id + market_timestamp + sportsbook
-- =============================================================================

CREATE OR REPLACE VIEW stg_external_odds_game AS
SELECT
  game_id,
  CAST(season           AS INTEGER)  AS season,
  CAST(week             AS INTEGER)  AS week,
  CAST(game_date        AS DATE)     AS game_date,
  home_team,
  away_team,
  CAST(market_timestamp AS TIMESTAMP) AS market_timestamp,
  sportsbook,
  market_type,
  CAST(home_spread      AS DOUBLE)   AS home_spread,
  CAST(away_spread      AS DOUBLE)   AS away_spread,
  TRY_CAST(spread_juice_home AS DOUBLE) AS spread_juice_home,
  TRY_CAST(spread_juice_away AS DOUBLE) AS spread_juice_away,
  CAST(total_line       AS DOUBLE)   AS total_line,
  TRY_CAST(over_juice   AS DOUBLE)   AS over_juice,
  TRY_CAST(under_juice  AS DOUBLE)   AS under_juice,
  TRY_CAST(home_ml      AS INTEGER)  AS home_ml,
  TRY_CAST(away_ml      AS INTEGER)  AS away_ml,
  TRY_CAST(opening_spread AS DOUBLE) AS opening_spread,
  TRY_CAST(opening_total  AS DOUBLE) AS opening_total,
  TRY_CAST(closing_spread AS DOUBLE) AS closing_spread,
  TRY_CAST(closing_total  AS DOUBLE) AS closing_total,
  ingestion_ts
FROM raw_external_odds
;
