-- =============================================================================
-- STAGING: stg_games
-- Source:  raw_schedules
-- Grain:   game_id (one row per game)
-- Keys:    game_id (PK)
-- Purpose: Canonical game-level schedule table with cleaned team abbrs,
--          parsed dates, season/week types, home/away normalisation.
-- =============================================================================

CREATE OR REPLACE VIEW stg_games AS
WITH base AS (
  SELECT
    -- Canonical identifiers
    game_id,
    old_game_id,
    CAST(season          AS INTEGER) AS season,
    CAST(week            AS INTEGER) AS week,

    -- Game metadata
    game_type,                              -- REG | POST | SB | PRE
    CASE
      WHEN game_type = 'REG'  THEN 'regular'
      WHEN game_type IN ('WC','DIV','CON','SB') THEN 'postseason'
      WHEN game_type = 'PRE'  THEN 'preseason'
      ELSE 'unknown'
    END AS season_type,

    -- Dates
    CAST(gameday    AS DATE)                AS game_date,
    CAST(gametime   AS VARCHAR)             AS kickoff_time_local,
    weekday,

    -- Teams — standardise legacy abbreviations
    CASE home_team
      WHEN 'OAK' THEN 'LV'
      WHEN 'SD'  THEN 'LAC'
      WHEN 'STL' THEN 'LA'
      ELSE home_team
    END AS home_team,
    CASE away_team
      WHEN 'OAK' THEN 'LV'
      WHEN 'SD'  THEN 'LAC'
      WHEN 'STL' THEN 'LA'
      ELSE away_team
    END AS away_team,

    -- Scores (NULL until game is final)
    TRY_CAST(home_score  AS INTEGER)        AS home_score,
    TRY_CAST(away_score  AS INTEGER)        AS away_score,
    TRY_CAST(home_score  AS INTEGER) + TRY_CAST(away_score AS INTEGER) AS total_points,
    TRY_CAST(home_score  AS INTEGER) - TRY_CAST(away_score AS INTEGER) AS home_margin,

    -- Outcome flags (NULL if not yet played)
    CASE
      WHEN TRY_CAST(home_score AS INTEGER) IS NOT NULL THEN
        CASE WHEN home_score > away_score THEN 1
             WHEN home_score < away_score THEN 0
             ELSE NULL END  -- OT tie safety
    END AS home_win_flag,

    -- Venue
    location,                               -- Home | Neutral
    stadium,
    stadium_id,
    UPPER(surface)                          AS surface,
    UPPER(roof)                             AS roof,

    -- Divisional / conference context
    div_game,
    conf_game,

    -- Rest (computed downstream in int_game_base using lag on team schedule)
    -- Primetime flag
    CASE
      WHEN UPPER(weekday) IN ('MONDAY','THURSDAY') THEN 1
      WHEN UPPER(weekday) = 'SUNDAY' AND kickoff_time_local >= '20:00' THEN 1
      ELSE 0
    END AS primetime_flag,

    -- Completeness flag
    CASE WHEN home_score IS NOT NULL THEN 1 ELSE 0 END AS game_completed_flag,

    -- Game reference IDs
    pfr_game_id,
    pff_game_id,
    espn_game_id,
    nfl_detail_id,

    -- Pipeline metadata
    ingestion_ts

  FROM raw_schedules
  WHERE game_type != 'PRE'               -- drop preseason by default; adjust if needed
)

SELECT
  *,
  -- Composite result string for quick reference
  CASE WHEN game_completed_flag = 1
       THEN home_team || ' ' || COALESCE(CAST(home_score AS VARCHAR),'?') ||
            ' - ' || COALESCE(CAST(away_score AS VARCHAR),'?') || ' ' || away_team
  END AS result_label
FROM base
;
