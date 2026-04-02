-- =============================================================================
-- STAGING: stg_player_week
-- Source:  raw_player_stats
-- Grain:   season + week + player_id
-- Verified column names. Note: pacr exists but racr does not; using pacr
-- =============================================================================

CREATE OR REPLACE VIEW stg_player_week AS
SELECT
  player_id,
  player_display_name                          AS full_name,
  UPPER(position)                              AS position,
  UPPER(position_group)                        AS position_group,
  recent_team                                  AS team,
  opponent_team                                AS opponent,
  CAST(season      AS INTEGER)                 AS season,
  CAST(week        AS INTEGER)                 AS week,
  season_type,

  COALESCE(CAST(completions              AS INTEGER), 0) AS completions,
  COALESCE(CAST(attempts                 AS INTEGER), 0) AS attempts,
  COALESCE(CAST(passing_yards            AS DOUBLE),  0) AS passing_yards,
  COALESCE(CAST(passing_tds              AS INTEGER), 0) AS passing_tds,
  COALESCE(CAST(interceptions            AS INTEGER), 0) AS interceptions,
  COALESCE(CAST(sacks                    AS INTEGER), 0) AS sacks,
  COALESCE(CAST(sack_yards               AS DOUBLE),  0) AS sack_yards,
  COALESCE(CAST(sack_fumbles             AS INTEGER), 0) AS sack_fumbles,
  COALESCE(CAST(sack_fumbles_lost        AS INTEGER), 0) AS sack_fumbles_lost,
  COALESCE(CAST(passing_air_yards        AS DOUBLE),  0) AS passing_air_yards,
  COALESCE(CAST(passing_yards_after_catch AS DOUBLE), 0) AS passing_yac,
  COALESCE(CAST(passing_first_downs      AS INTEGER), 0) AS passing_first_downs,
  COALESCE(CAST(passing_epa              AS DOUBLE),  0) AS passing_epa,
  COALESCE(CAST(passing_2pt_conversions  AS INTEGER), 0) AS passing_2pt,
  COALESCE(CAST(dakota                   AS DOUBLE),  0) AS dakota,

  COALESCE(CAST(carries                  AS INTEGER), 0) AS carries,
  COALESCE(CAST(rushing_yards            AS DOUBLE),  0) AS rushing_yards,
  COALESCE(CAST(rushing_tds              AS INTEGER), 0) AS rushing_tds,
  COALESCE(CAST(rushing_fumbles          AS INTEGER), 0) AS rushing_fumbles,
  COALESCE(CAST(rushing_fumbles_lost     AS INTEGER), 0) AS rushing_fumbles_lost,
  COALESCE(CAST(rushing_first_downs      AS INTEGER), 0) AS rushing_first_downs,
  COALESCE(CAST(rushing_epa              AS DOUBLE),  0) AS rushing_epa,
  COALESCE(CAST(rushing_2pt_conversions  AS INTEGER), 0) AS rushing_2pt,

  COALESCE(CAST(receptions               AS INTEGER), 0) AS receptions,
  COALESCE(CAST(targets                  AS INTEGER), 0) AS targets,
  COALESCE(CAST(receiving_yards          AS DOUBLE),  0) AS receiving_yards,
  COALESCE(CAST(receiving_tds            AS INTEGER), 0) AS receiving_tds,
  COALESCE(CAST(receiving_fumbles        AS INTEGER), 0) AS receiving_fumbles,
  COALESCE(CAST(receiving_fumbles_lost   AS INTEGER), 0) AS receiving_fumbles_lost,
  COALESCE(CAST(receiving_air_yards      AS DOUBLE),  0) AS receiving_air_yards,
  COALESCE(CAST(receiving_yards_after_catch AS DOUBLE),0) AS receiving_yac,
  COALESCE(CAST(receiving_first_downs    AS INTEGER), 0) AS receiving_first_downs,
  COALESCE(CAST(receiving_epa            AS DOUBLE),  0) AS receiving_epa,
  COALESCE(CAST(receiving_2pt_conversions AS INTEGER), 0) AS receiving_2pt,
  COALESCE(CAST(pacr                     AS DOUBLE),  0) AS racr,   -- pacr is the actual col
  COALESCE(CAST(target_share             AS DOUBLE),  0) AS target_share,
  COALESCE(CAST(air_yards_share          AS DOUBLE),  0) AS air_yards_share,
  COALESCE(CAST(wopr                     AS DOUBLE),  0) AS wopr,

  COALESCE(CAST(special_teams_tds        AS INTEGER), 0) AS special_teams_tds,
  COALESCE(CAST(fantasy_points           AS DOUBLE),  0) AS fantasy_points_std,
  COALESCE(CAST(fantasy_points_ppr       AS DOUBLE),  0) AS fantasy_points_ppr,

  ingestion_ts

FROM raw_player_stats
WHERE player_id IS NOT NULL
  AND season_type IN ('REG', 'POST')
;


-- =============================================================================
-- STAGING: stg_team_week
-- Source:  raw_schedules (unpivoted to one row per team per game)
-- Grain:   season + week + team
-- =============================================================================

CREATE OR REPLACE VIEW stg_team_week AS
WITH team_games AS (
  SELECT
    game_id,
    CAST(season AS INTEGER)          AS season,
    CAST(week   AS INTEGER)          AS week,
    game_type,
    CAST(gameday AS DATE)            AS game_date,
    CASE home_team WHEN 'OAK' THEN 'LV' WHEN 'SD' THEN 'LAC' WHEN 'STL' THEN 'LA' ELSE home_team END AS team,
    CASE away_team WHEN 'OAK' THEN 'LV' WHEN 'SD' THEN 'LAC' WHEN 'STL' THEN 'LA' ELSE away_team END AS opponent,
    1                                AS home_flag,
    TRY_CAST(home_score AS INTEGER)  AS team_score,
    TRY_CAST(away_score AS INTEGER)  AS opp_score
  FROM raw_schedules
  WHERE game_type != 'PRE'

  UNION ALL

  SELECT
    game_id,
    CAST(season AS INTEGER)          AS season,
    CAST(week   AS INTEGER)          AS week,
    game_type,
    CAST(gameday AS DATE)            AS game_date,
    CASE away_team WHEN 'OAK' THEN 'LV' WHEN 'SD' THEN 'LAC' WHEN 'STL' THEN 'LA' ELSE away_team END AS team,
    CASE home_team WHEN 'OAK' THEN 'LV' WHEN 'SD' THEN 'LAC' WHEN 'STL' THEN 'LA' ELSE home_team END AS opponent,
    0                                AS home_flag,
    TRY_CAST(away_score AS INTEGER)  AS team_score,
    TRY_CAST(home_score AS INTEGER)  AS opp_score
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
  team_score                         AS points_for,
  opp_score                          AS points_against,
  team_score - opp_score             AS margin,
  CASE
    WHEN team_score > opp_score THEN 1
    WHEN team_score < opp_score THEN 0
    ELSE NULL
  END                                AS win_flag,
  CASE WHEN game_type = 'REG' THEN 'regular' ELSE 'postseason' END AS season_type
FROM team_games
WHERE team IS NOT NULL
;


-- =============================================================================
-- STAGING: stg_rosters_weekly
-- Source:  raw_rosters (already contains week column — nflreadr loads all weeks)
-- Grain:   season + week + team + gsis_id
-- Note:    raw_rosters has "week" col confirming it is already weekly-level data
-- =============================================================================

CREATE OR REPLACE VIEW stg_rosters_weekly AS
SELECT
  gsis_id                            AS player_id,
  full_name,
  UPPER(position)                    AS position,
  depth_chart_position,
  CASE team WHEN 'OAK' THEN 'LV' WHEN 'SD' THEN 'LAC' WHEN 'STL' THEN 'LA' ELSE team END AS team,
  CAST(season      AS INTEGER)       AS season,
  CAST(week        AS INTEGER)       AS week,
  status,
  jersey_number,
  CAST(years_exp   AS INTEGER)       AS years_exp,
  ingestion_ts
FROM raw_rosters
WHERE gsis_id IS NOT NULL
  AND week IS NOT NULL
;


-- =============================================================================
-- STAGING: stg_injuries_weekly
-- Source:  raw_injuries
-- Grain:   season + week + team + gsis_id
-- Fix:     report_date -> date_modified (actual column name in nflreadr injuries)
-- =============================================================================

CREATE OR REPLACE VIEW stg_injuries_weekly AS
SELECT
  gsis_id                            AS player_id,
  full_name,
  UPPER(position)                    AS position,
  CASE team WHEN 'OAK' THEN 'LV' WHEN 'SD' THEN 'LAC' WHEN 'STL' THEN 'LA' ELSE team END AS team,
  CAST(season      AS INTEGER)       AS season,
  CAST(week        AS INTEGER)       AS week,
  CAST(date_modified AS DATE)        AS report_date,   -- actual col: date_modified
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
  END                                AS injury_severity_score,
  ingestion_ts
FROM raw_injuries
WHERE gsis_id IS NOT NULL
;


-- =============================================================================
-- STAGING: stg_snap_counts_weekly
-- Source:  raw_snap_counts
-- Grain:   season + week + team + pfr_player_id
-- Fix:     pos -> position (actual column name)
-- =============================================================================

CREATE OR REPLACE VIEW stg_snap_counts_weekly AS
SELECT
  pfr_player_id,
  player                             AS full_name,
  UPPER(position)                    AS position,   -- actual col: position (not pos)
  CASE team WHEN 'OAK' THEN 'LV' WHEN 'SD' THEN 'LAC' WHEN 'STL' THEN 'LA' ELSE team END AS team,
  opponent,
  CAST(season        AS INTEGER)     AS season,
  CAST(week          AS INTEGER)     AS week,
  CAST(offense_snaps AS INTEGER)     AS offense_snaps,
  CAST(offense_pct   AS DOUBLE)      AS offense_snap_pct,
  CAST(defense_snaps AS INTEGER)     AS defense_snaps,
  CAST(defense_pct   AS DOUBLE)      AS defense_snap_pct,
  CAST(st_snaps      AS INTEGER)     AS st_snaps,
  CAST(st_pct        AS DOUBLE)      AS st_snap_pct,
  ingestion_ts
FROM raw_snap_counts
WHERE pfr_player_id IS NOT NULL
;


-- =============================================================================
-- STAGING: stg_nextgen_player_week
-- Source:  raw_nextgen_stats
-- Grain:   season + week + player_gsis_id + stat_type
-- Note:    Receiving stats confirmed. Passing/rushing cols differ per stat_type.
--          All columns use TRY_CAST so missing cols for other stat types = NULL.
-- =============================================================================

CREATE OR REPLACE VIEW stg_nextgen_player_week AS
WITH base AS (
  SELECT *
  FROM raw_nextgen_stats
  WHERE player_gsis_id IS NOT NULL
)
SELECT
  player_gsis_id                     AS player_id,
  player_display_name                AS full_name,
  player_position                    AS position,
  CASE team_abbr WHEN 'OAK' THEN 'LV' WHEN 'SD' THEN 'LAC' WHEN 'STL' THEN 'LA' ELSE team_abbr END AS team,
  CAST(season    AS INTEGER)         AS season,
  CAST(week      AS INTEGER)         AS week,
  season_type,
  stat_type,

  -- Passing NGS columns
  TRY_CAST(avg_time_to_throw                  AS DOUBLE) AS avg_time_to_throw,
  TRY_CAST(avg_completed_air_yards            AS DOUBLE) AS avg_completed_air_yards,
  TRY_CAST(avg_intended_air_yards             AS DOUBLE) AS avg_intended_air_yards,
  TRY_CAST(aggressiveness                     AS DOUBLE) AS aggressiveness,
  TRY_CAST(completion_percentage_above_expectation AS DOUBLE) AS cpoe,
  TRY_CAST(passer_rating                      AS DOUBLE) AS passer_rating,
  TRY_CAST(expected_completion_percentage     AS DOUBLE) AS xcomp_pct,

  -- Rushing NGS columns
  TRY_CAST(efficiency                         AS DOUBLE) AS rush_efficiency,
  TRY_CAST(percent_attempts_gte_eight_defenders AS DOUBLE) AS stacked_box_pct,
  TRY_CAST(avg_time_to_los                    AS DOUBLE) AS avg_time_to_los,
  TRY_CAST(rush_yards_over_expected           AS DOUBLE) AS ryoe,
  TRY_CAST(rush_yards_over_expected_per_att   AS DOUBLE) AS ryoe_per_att,

  -- Receiving NGS columns (confirmed present)
  TRY_CAST(avg_cushion                        AS DOUBLE) AS avg_cushion,
  TRY_CAST(avg_separation                     AS DOUBLE) AS avg_separation,
  TRY_CAST(catch_percentage                   AS DOUBLE) AS catch_percentage,
  TRY_CAST(avg_yac                            AS DOUBLE) AS avg_yac,
  TRY_CAST(avg_yac_above_expectation          AS DOUBLE) AS avg_yac_above_expectation,

  ingestion_ts
FROM base
;


-- stg_external_odds_game moved to sql/staging/stg_external_odds_game.sql
