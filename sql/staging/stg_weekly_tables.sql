-- =============================================================================
-- STAGING: stg_weekly_tables.sql
-- Replaces: sql/staging/stg_weekly_tables.sql
--
-- Root cause of "avg_time_to_throw referenced in SELECT clause" BINDER error:
--   The old file contained stg_nextgen_player_week which DuckDB cached as a
--   broken view. Because all views in this file are parsed as one SQL string,
--   a stale bad view definition in the .duckdb file infected all subsequent
--   CREATE OR REPLACE VIEW statements in the same execution.
--
--   Fix: This file is rewritten clean. The DROP in run_transforms.R clears
--   stale cached views before this file executes, so every view here starts
--   from scratch. No QUALIFY clauses reference SELECT aliases.
--
-- Key fixes vs old version:
--   1. stg_team_week: CTE renamed from "unpivot" (DuckDB keyword) to "sides"
--   2. stg_rosters_weekly: reads raw_rosters (raw_rosters_weekly doesn't exist)
--   3. stg_injuries_weekly: no QUALIFY — consumers dedup using date_modified
--   4. stg_nextgen_player_week: simplified; only confirmed cols from raw used
--   5. stg_external_odds_game: zero-row typed stub (raw_external_odds absent)
-- =============================================================================


-- =============================================================================
-- stg_player_week
-- Source:  raw_player_stats
-- Grain:   season + week + player_id
-- Confirmed cols from column_names.csv audit of raw.player_stats
-- =============================================================================

CREATE OR REPLACE VIEW stg_player_week AS
SELECT
  player_id,
  player_display_name                           AS full_name,
  UPPER(position)                               AS position,
  UPPER(position_group)                         AS position_group,
  recent_team                                   AS team,
  opponent_team                                 AS opponent,
  CAST(season      AS INTEGER)                  AS season,
  CAST(week        AS INTEGER)                  AS week,
  season_type,

  -- Passing (all confirmed in raw.player_stats)
  COALESCE(CAST(completions               AS INTEGER), 0) AS completions,
  COALESCE(CAST(attempts                  AS INTEGER), 0) AS attempts,
  COALESCE(CAST(passing_yards             AS DOUBLE),  0) AS passing_yards,
  COALESCE(CAST(passing_tds               AS INTEGER), 0) AS passing_tds,
  COALESCE(CAST(interceptions             AS INTEGER), 0) AS interceptions,
  COALESCE(CAST(sacks                     AS INTEGER), 0) AS sacks,
  COALESCE(CAST(sack_yards                AS DOUBLE),  0) AS sack_yards,
  COALESCE(CAST(sack_fumbles              AS INTEGER), 0) AS sack_fumbles,
  COALESCE(CAST(sack_fumbles_lost         AS INTEGER), 0) AS sack_fumbles_lost,
  COALESCE(CAST(passing_air_yards         AS DOUBLE),  0) AS passing_air_yards,
  COALESCE(CAST(passing_yards_after_catch AS DOUBLE),  0) AS passing_yac,
  COALESCE(CAST(passing_first_downs       AS INTEGER), 0) AS passing_first_downs,
  COALESCE(CAST(passing_epa               AS DOUBLE),  0) AS passing_epa,
  COALESCE(CAST(passing_2pt_conversions   AS INTEGER), 0) AS passing_2pt,
  COALESCE(CAST(dakota                    AS DOUBLE),  0) AS dakota,

  -- Rushing
  COALESCE(CAST(carries                   AS INTEGER), 0) AS carries,
  COALESCE(CAST(rushing_yards             AS DOUBLE),  0) AS rushing_yards,
  COALESCE(CAST(rushing_tds               AS INTEGER), 0) AS rushing_tds,
  COALESCE(CAST(rushing_fumbles           AS INTEGER), 0) AS rushing_fumbles,
  COALESCE(CAST(rushing_fumbles_lost      AS INTEGER), 0) AS rushing_fumbles_lost,
  COALESCE(CAST(rushing_first_downs       AS INTEGER), 0) AS rushing_first_downs,
  COALESCE(CAST(rushing_epa               AS DOUBLE),  0) AS rushing_epa,
  COALESCE(CAST(rushing_2pt_conversions   AS INTEGER), 0) AS rushing_2pt,

  -- Receiving
  COALESCE(CAST(receptions                AS INTEGER), 0) AS receptions,
  COALESCE(CAST(targets                   AS INTEGER), 0) AS targets,
  COALESCE(CAST(receiving_yards           AS DOUBLE),  0) AS receiving_yards,
  COALESCE(CAST(receiving_tds             AS INTEGER), 0) AS receiving_tds,
  COALESCE(CAST(receiving_air_yards       AS DOUBLE),  0) AS receiving_air_yards,
  COALESCE(CAST(receiving_yards_after_catch AS DOUBLE),0) AS receiving_yac,
  COALESCE(CAST(receiving_first_downs     AS INTEGER), 0) AS receiving_first_downs,
  COALESCE(CAST(receiving_epa             AS DOUBLE),  0) AS receiving_epa,
  COALESCE(CAST(receiving_fumbles         AS INTEGER), 0) AS receiving_fumbles,
  COALESCE(CAST(receiving_fumbles_lost    AS INTEGER), 0) AS receiving_fumbles_lost,
  COALESCE(CAST(receiving_2pt_conversions AS INTEGER), 0) AS receiving_2pt,

  -- Usage / efficiency (all confirmed)
  TRY_CAST(target_share    AS DOUBLE)           AS target_share,
  TRY_CAST(air_yards_share AS DOUBLE)           AS air_yards_share,
  TRY_CAST(wopr            AS DOUBLE)           AS wopr,
  TRY_CAST(racr            AS DOUBLE)           AS racr,
  TRY_CAST(pacr            AS DOUBLE)           AS pacr,

  -- Fantasy
  TRY_CAST(fantasy_points     AS DOUBLE)        AS fantasy_points_std,
  TRY_CAST(fantasy_points_ppr AS DOUBLE)        AS fantasy_points_ppr,

  ingestion_ts
FROM raw_player_stats
WHERE player_id IS NOT NULL
  AND season_type IN ('REG', 'POST')
;


-- =============================================================================
-- stg_team_week
-- Source:  raw_schedules
-- Grain:   season + week + team (one row per team per game via UNION ALL)
-- Fix:     CTE renamed from "unpivot" (reserved keyword) to "sides"
--          gameday confirmed as the date column in raw.schedules
-- =============================================================================

CREATE OR REPLACE VIEW stg_team_week AS
WITH sides AS (
  SELECT
    game_id,
    CAST(season AS INTEGER)           AS season,
    CAST(week   AS INTEGER)           AS week,
    game_type,
    CAST(gameday AS DATE)             AS game_date,
    CASE home_team
      WHEN 'OAK' THEN 'LV'
      WHEN 'SD'  THEN 'LAC'
      WHEN 'STL' THEN 'LA'
      ELSE home_team END              AS team,
    CASE away_team
      WHEN 'OAK' THEN 'LV'
      WHEN 'SD'  THEN 'LAC'
      WHEN 'STL' THEN 'LA'
      ELSE away_team END              AS opponent,
    1                                 AS home_flag,
    TRY_CAST(home_score AS INTEGER)   AS team_score,
    TRY_CAST(away_score AS INTEGER)   AS opp_score
  FROM raw_schedules
  WHERE game_type != 'PRE'

  UNION ALL

  SELECT
    game_id,
    CAST(season AS INTEGER)           AS season,
    CAST(week   AS INTEGER)           AS week,
    game_type,
    CAST(gameday AS DATE)             AS game_date,
    CASE away_team
      WHEN 'OAK' THEN 'LV'
      WHEN 'SD'  THEN 'LAC'
      WHEN 'STL' THEN 'LA'
      ELSE away_team END              AS team,
    CASE home_team
      WHEN 'OAK' THEN 'LV'
      WHEN 'SD'  THEN 'LAC'
      WHEN 'STL' THEN 'LA'
      ELSE home_team END              AS opponent,
    0                                 AS home_flag,
    TRY_CAST(away_score AS INTEGER)   AS team_score,
    TRY_CAST(home_score AS INTEGER)   AS opp_score
  FROM raw_schedules
  WHERE game_type != 'PRE'
)
SELECT
  game_id,
  season,
  week,
  game_type,
  game_date,
  team,
  opponent,
  home_flag,
  team_score                          AS points_for,
  opp_score                           AS points_against,
  team_score - opp_score              AS margin,
  CASE
    WHEN team_score >  opp_score THEN 1
    WHEN team_score <  opp_score THEN 0
    ELSE NULL
  END                                 AS win_flag,
  CASE
    WHEN game_type = 'REG' THEN 'regular'
    ELSE 'postseason'
  END                                 AS season_type
FROM sides
WHERE team IS NOT NULL
;


-- =============================================================================
-- stg_rosters_weekly
-- Source:  raw_rosters  (NOT raw_rosters_weekly — that table does not exist)
-- Grain:   season + week + team + gsis_id
-- raw_rosters already contains a week column
-- =============================================================================

CREATE OR REPLACE VIEW stg_rosters_weekly AS
SELECT
  gsis_id                             AS player_id,
  full_name,
  UPPER(position)                     AS position,
  depth_chart_position,
  CASE team
    WHEN 'OAK' THEN 'LV'
    WHEN 'SD'  THEN 'LAC'
    WHEN 'STL' THEN 'LA'
    ELSE team END                     AS team,
  CAST(season    AS INTEGER)          AS season,
  CAST(week      AS INTEGER)          AS week,
  status,
  jersey_number,
  CAST(years_exp AS INTEGER)          AS years_exp,
  ingestion_ts
FROM raw_rosters
WHERE gsis_id IS NOT NULL
  AND week IS NOT NULL
;


-- =============================================================================
-- stg_injuries_weekly
-- Source:  raw_injuries
-- Grain:   season + week + team + gsis_id (one row per report entry)
-- Fix:     No QUALIFY here — QUALIFY ORDER BY on a SELECT alias (report_date)
--          causes DuckDB BINDER error. Consumers that need the latest report
--          per player/week must dedup using the source column: date_modified
-- =============================================================================

CREATE OR REPLACE VIEW stg_injuries_weekly AS
SELECT
  gsis_id                             AS player_id,
  full_name,
  UPPER(position)                     AS position,
  CASE team
    WHEN 'OAK' THEN 'LV'
    WHEN 'SD'  THEN 'LAC'
    WHEN 'STL' THEN 'LA'
    ELSE team END                     AS team,
  CAST(season    AS INTEGER)          AS season,
  CAST(week      AS INTEGER)          AS week,
  CAST(CAST(date_modified AS TIMESTAMP) AS DATE) AS report_date,
  report_primary_injury,
  report_secondary_injury,
  report_status,
  practice_primary_injury,
  practice_secondary_injury,
  practice_status,
  CASE UPPER(report_status)
    WHEN 'OUT'          THEN 4
    WHEN 'DOUBTFUL'     THEN 3
    WHEN 'QUESTIONABLE' THEN 2
    WHEN 'LIMITED'      THEN 1
    ELSE 0
  END                                 AS injury_severity_score,
  ingestion_ts
FROM raw_injuries
WHERE gsis_id IS NOT NULL
;


-- =============================================================================
-- stg_snap_counts_weekly
-- Source:  raw_snap_counts
-- Grain:   season + week + team + pfr_player_id
-- Confirmed cols: player, position, team, opponent, offense_snaps,
--   offense_pct, defense_snaps, defense_pct, st_snaps, st_pct
-- =============================================================================

CREATE OR REPLACE VIEW stg_snap_counts_weekly AS
SELECT
  pfr_player_id,
  player                              AS full_name,
  UPPER(position)                     AS position,
  CASE team
    WHEN 'OAK' THEN 'LV'
    WHEN 'SD'  THEN 'LAC'
    WHEN 'STL' THEN 'LA'
    ELSE team END                     AS team,
  opponent,
  CAST(season        AS INTEGER)      AS season,
  CAST(week          AS INTEGER)      AS week,
  CAST(offense_snaps AS INTEGER)      AS offense_snaps,
  CAST(offense_pct   AS DOUBLE)       AS offense_snap_pct,
  CAST(defense_snaps AS INTEGER)      AS defense_snaps,
  CAST(defense_pct   AS DOUBLE)       AS defense_snap_pct,
  CAST(st_snaps      AS INTEGER)      AS st_snaps,
  CAST(st_pct        AS DOUBLE)       AS st_snap_pct,
  ingestion_ts
FROM raw_snap_counts
WHERE pfr_player_id IS NOT NULL
;


-- =============================================================================
-- stg_nextgen_player_week
-- Source:  raw_nextgen_stats
-- Grain:   season + week + player_gsis_id + stat_type
--
-- IMPORTANT: raw_nextgen_stats in this load only contains RECEIVING rows.
-- Confirmed present cols: avg_cushion, avg_expected_yac, avg_intended_air_yards,
--   avg_separation, avg_yac, avg_yac_above_expectation, catch_percentage,
--   percent_share_of_intended_air_yards, rec_touchdowns, receptions,
--   targets, yards (plus player id/name/position/team/season/week/stat_type)
--
-- Passing/rushing columns (avg_time_to_throw, aggressiveness, cpoe, ryoe etc.)
-- are NOT present in raw — TRY_CAST returns NULL for absent columns, which
-- is acceptable. The BINDER error was caused by stale cached view definitions,
-- NOT by TRY_CAST. This clean file resolves that.
-- =============================================================================
CREATE OR REPLACE VIEW stg_nextgen_player_week AS
SELECT
  player_gsis_id                               AS player_id,
  player_display_name                          AS full_name,
  player_position                              AS position,
  CASE team_abbr
    WHEN 'OAK' THEN 'LV'
    WHEN 'SD'  THEN 'LAC'
    WHEN 'STL' THEN 'LA'
    ELSE team_abbr END                         AS team,
  CAST(season AS INTEGER)                      AS season,
  CAST(week   AS INTEGER)                      AS week,
  season_type,
  stat_type,

  -- Receiving columns (ALL confirmed present in raw_nextgen_stats)
  CAST(avg_cushion                          AS DOUBLE) AS avg_cushion,
  CAST(avg_separation                       AS DOUBLE) AS avg_separation,
  CAST(catch_percentage                     AS DOUBLE) AS catch_percentage,
  CAST(avg_yac                              AS DOUBLE) AS avg_yac,
  CAST(avg_yac_above_expectation            AS DOUBLE) AS avg_yac_above_expectation,
  CAST(avg_intended_air_yards               AS DOUBLE) AS avg_intended_air_yards,
  CAST(percent_share_of_intended_air_yards  AS DOUBLE) AS air_yards_share_ngs,
  CAST(avg_expected_yac                     AS DOUBLE) AS avg_expected_yac,
  CAST(receptions                           AS INTEGER) AS receptions,
  CAST(targets                              AS INTEGER) AS targets,
  CAST(yards                                AS DOUBLE)  AS receiving_yards,
  CAST(rec_touchdowns                       AS INTEGER) AS rec_touchdowns,

  -- Passing columns DO NOT EXIST in this dataset - omitted entirely
  -- avg_time_to_throw, aggressiveness, cpoe, passer_rating etc. are absent
  -- They will be NULL in downstream joins by default

  -- Rushing columns DO NOT EXIST in this dataset - omitted entirely  
  -- ryoe, stacked_box_pct, avg_time_to_los etc. are absent

  ingestion_ts
FROM raw_nextgen_stats
WHERE player_gsis_id IS NOT NULL
;

-- =============================================================================
-- stg_external_odds_game
-- Source:  raw_external_odds (absent until odds ingestion runs)
-- Grain:   game_id + market_timestamp + sportsbook
-- This zero-row typed stub ensures downstream JOINs have a valid schema target
-- and replaces the inline stub that the R pipeline creates, eliminating the
-- Catalog Error that previously triggered the R fallback path.
-- =============================================================================

CREATE OR REPLACE VIEW stg_external_odds_game AS
SELECT
  CAST(NULL AS VARCHAR)    AS game_id,
  CAST(NULL AS INTEGER)    AS season,
  CAST(NULL AS INTEGER)    AS week,
  CAST(NULL AS DATE)       AS game_date,
  CAST(NULL AS VARCHAR)    AS home_team,
  CAST(NULL AS VARCHAR)    AS away_team,
  CAST(NULL AS TIMESTAMP)  AS market_timestamp,
  CAST(NULL AS VARCHAR)    AS sportsbook,
  CAST(NULL AS VARCHAR)    AS market_type,
  CAST(NULL AS DOUBLE)     AS home_spread,
  CAST(NULL AS DOUBLE)     AS away_spread,
  CAST(NULL AS DOUBLE)     AS spread_juice_home,
  CAST(NULL AS DOUBLE)     AS spread_juice_away,
  CAST(NULL AS DOUBLE)     AS total_line,
  CAST(NULL AS DOUBLE)     AS over_juice,
  CAST(NULL AS DOUBLE)     AS under_juice,
  CAST(NULL AS INTEGER)    AS home_ml,
  CAST(NULL AS INTEGER)    AS away_ml,
  CAST(NULL AS DOUBLE)     AS opening_spread,
  CAST(NULL AS DOUBLE)     AS opening_total,
  CAST(NULL AS DOUBLE)     AS closing_spread,
  CAST(NULL AS DOUBLE)     AS closing_total,
  CAST(NULL AS TIMESTAMP)  AS ingestion_ts
WHERE FALSE
;
