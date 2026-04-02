-- =============================================================================
-- STAGING: stg_games
-- Source:  raw_schedules
-- Grain:   game_id  (one row per game, non-preseason)
-- Verified against actual nflreadr::load_schedules() column names.
-- Key fixes:
--   conf_game   -> not in source; derived from schedule metadata
--   pfr_game_id -> actual col is "pfr"
--   pff_game_id -> actual col is "pff"
--   espn_game_id-> actual col is "espn"
--   rest days   -> away_rest / home_rest already in schedules
-- =============================================================================

CREATE OR REPLACE VIEW stg_games AS
SELECT
  -- Keys
  game_id,
  old_game_id,
  CAST(season AS INTEGER)                      AS season,
  CAST(week   AS INTEGER)                      AS week,

  -- Game type
  game_type,
  CASE
    WHEN game_type = 'REG'                     THEN 'regular'
    WHEN game_type IN ('WC','DIV','CON','SB')  THEN 'postseason'
    WHEN game_type = 'PRE'                     THEN 'preseason'
    ELSE 'unknown'
  END                                          AS season_type,

  -- Dates
  CAST(gameday  AS DATE)                       AS game_date,
  CAST(gametime AS VARCHAR)                    AS kickoff_time_local,
  weekday,

  -- Teams (standardise legacy abbreviations)
  CASE home_team WHEN 'OAK' THEN 'LV' WHEN 'SD' THEN 'LAC' WHEN 'STL' THEN 'LA' ELSE home_team END AS home_team,
  CASE away_team WHEN 'OAK' THEN 'LV' WHEN 'SD' THEN 'LAC' WHEN 'STL' THEN 'LA' ELSE away_team END AS away_team,

  -- Scores
  TRY_CAST(home_score AS INTEGER)              AS home_score,
  TRY_CAST(away_score AS INTEGER)              AS away_score,
  TRY_CAST(home_score AS INTEGER) + TRY_CAST(away_score AS INTEGER) AS total_points,
  TRY_CAST(home_score AS INTEGER) - TRY_CAST(away_score AS INTEGER) AS home_margin,
  CASE
    WHEN TRY_CAST(home_score AS INTEGER) IS NOT NULL
    THEN CASE
           WHEN TRY_CAST(home_score AS INTEGER) > TRY_CAST(away_score AS INTEGER) THEN 1
           WHEN TRY_CAST(home_score AS INTEGER) < TRY_CAST(away_score AS INTEGER) THEN 0
           ELSE NULL
         END
  END                                          AS home_win_flag,

  -- Venue
  location,
  stadium,
  stadium_id,
  UPPER(COALESCE(surface, 'unknown'))          AS surface,
  UPPER(COALESCE(roof,    'unknown'))          AS roof,

  -- Context flags
  CAST(COALESCE(div_game, 0) AS INTEGER)       AS div_game,
  -- conf_game not available in source; use div_game as conservative proxy
  CAST(COALESCE(div_game, 0) AS INTEGER)       AS conf_game,

  -- Primetime (Mon/Thu/late Sun)
  CASE
    WHEN UPPER(weekday) = 'MONDAY'   THEN 1
    WHEN UPPER(weekday) = 'THURSDAY' THEN 1
    WHEN UPPER(weekday) = 'SUNDAY'
     AND TRY_CAST(SPLIT_PART(COALESCE(gametime, '13:00'), ':', 1) AS INTEGER) >= 20 THEN 1
    ELSE 0
  END                                          AS primetime_flag,

  -- Rest days (already pre-computed in nflreadr schedules)
  CAST(COALESCE(home_rest, 7) AS INTEGER)      AS home_rest_days,
  CAST(COALESCE(away_rest, 7) AS INTEGER)      AS away_rest_days,

  -- Built-in weather from schedule (rough game-time conditions)
  TRY_CAST(temp AS DOUBLE)                     AS schedule_temp_f,
  TRY_CAST(wind AS DOUBLE)                     AS schedule_wind_mph,

  -- Built-in market lines from schedule
  TRY_CAST(spread_line     AS DOUBLE)          AS schedule_spread_line,
  TRY_CAST(total_line      AS DOUBLE)          AS schedule_total_line,
  TRY_CAST(away_moneyline  AS INTEGER)         AS schedule_away_ml,
  TRY_CAST(home_moneyline  AS INTEGER)         AS schedule_home_ml,

  -- Completion flag
  CASE WHEN TRY_CAST(home_score AS INTEGER) IS NOT NULL THEN 1 ELSE 0 END AS game_completed_flag,

  -- Reference IDs (actual column names in nflreadr)
  pfr                                          AS pfr_game_id,
  pff                                          AS pff_game_id,
  espn                                         AS espn_game_id,
  nfl_detail_id,

  ingestion_ts

FROM raw_schedules
WHERE game_type != 'PRE'
;
