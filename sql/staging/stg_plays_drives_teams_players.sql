-- =============================================================================
-- STAGING: stg_plays
-- Source:  raw_pbp
-- Grain:   game_id + play_id
-- Keys:    game_id (FK -> stg_games), play_id
-- Purpose: Cleaned, typed play-level rows with derived context flags.
-- =============================================================================

CREATE OR REPLACE VIEW stg_plays AS
SELECT
  -- Canonical keys
  game_id,
  CAST(play_id          AS INTEGER)     AS play_id,
  CAST(season           AS INTEGER)     AS season,
  CAST(week             AS INTEGER)     AS week,
  game_date,
  CAST(qtr              AS INTEGER)     AS quarter,
  CAST(down             AS INTEGER)     AS down,

  -- Teams (standardised)
  CASE posteam  WHEN 'OAK' THEN 'LV' WHEN 'SD' THEN 'LAC' WHEN 'STL' THEN 'LA' ELSE posteam  END AS posteam,
  CASE defteam  WHEN 'OAK' THEN 'LV' WHEN 'SD' THEN 'LAC' WHEN 'STL' THEN 'LA' ELSE defteam  END AS defteam,
  CASE home_team WHEN 'OAK' THEN 'LV' WHEN 'SD' THEN 'LAC' WHEN 'STL' THEN 'LA' ELSE home_team END AS home_team,
  CASE away_team WHEN 'OAK' THEN 'LV' WHEN 'SD' THEN 'LAC' WHEN 'STL' THEN 'LA' ELSE away_team END AS away_team,

  -- Situational
  CAST(ydstogo          AS INTEGER)     AS ydstogo,
  CAST(yardline_100     AS INTEGER)     AS yardline_100,
  CAST(score_differential AS DOUBLE)   AS score_differential,
  CAST(posteam_score    AS INTEGER)     AS posteam_score,
  CAST(defteam_score    AS INTEGER)     AS defteam_score,
  CAST(game_seconds_remaining  AS DOUBLE) AS game_seconds_remaining,
  CAST(half_seconds_remaining  AS DOUBLE) AS half_seconds_remaining,
  CAST(qtr_seconds_remaining   AS DOUBLE) AS qtr_seconds_remaining,
  CAST(posteam_timeouts_remaining AS INTEGER) AS posteam_timeouts,
  CAST(defteam_timeouts_remaining AS INTEGER) AS defteam_timeouts,

  -- Play type flags
  play_type,
  CAST(pass_attempt     AS INTEGER)     AS pass_attempt,
  CAST(rush_attempt     AS INTEGER)     AS rush_attempt,
  CAST(sack             AS INTEGER)     AS sack,
  CAST(scramble         AS INTEGER)     AS scramble,
  CAST(qb_spike         AS INTEGER)     AS qb_spike,
  CAST(qb_kneel         AS INTEGER)     AS qb_kneel,
  CAST(penalty          AS INTEGER)     AS penalty,
  CAST(punt_attempt     AS INTEGER)     AS punt_attempt,
  CAST(field_goal_attempt AS INTEGER)   AS field_goal_attempt,
  CAST(two_point_attempt AS INTEGER)    AS two_point_attempt,
  CAST(extra_point_attempt AS INTEGER)  AS extra_point_attempt,

  -- Formation / pre-snap
  CAST(shotgun          AS INTEGER)     AS shotgun,
  CAST(no_huddle        AS INTEGER)     AS no_huddle,

  -- Air yards / YAC
  CAST(air_yards        AS DOUBLE)      AS air_yards,
  CAST(yards_after_catch AS DOUBLE)     AS yards_after_catch,
  CAST(yards_gained     AS DOUBLE)      AS yards_gained,

  -- EPA / WPA
  CAST(epa              AS DOUBLE)      AS epa,
  CAST(wpa              AS DOUBLE)      AS wpa,
  CAST(air_epa          AS DOUBLE)      AS air_epa,
  CAST(yac_epa          AS DOUBLE)      AS yac_epa,
  CAST(comp_air_epa     AS DOUBLE)      AS comp_air_epa,
  CAST(comp_yac_epa     AS DOUBLE)      AS comp_yac_epa,
  CAST(success          AS INTEGER)     AS success,      -- 1 if EPA >= 0

  -- Completion / target
  CAST(complete_pass    AS INTEGER)     AS complete_pass,
  CAST(incomplete_pass  AS INTEGER)     AS incomplete_pass,
  CAST(interception     AS INTEGER)     AS interception,
  CAST(touchdown        AS INTEGER)     AS touchdown,
  CAST(fumble           AS INTEGER)     AS fumble,
  CAST(fumble_lost      AS INTEGER)     AS fumble_lost,

  -- Player IDs
  passer_id,
  passer,
  rusher_id,
  rusher,
  receiver_id,
  receiver,

  -- Drive reference
  CAST(drive            AS INTEGER)     AS drive_id,

  -- Special play context
  CAST(special_teams_play AS INTEGER)   AS special_teams_play,
  CAST(st_play_type     AS VARCHAR)     AS st_play_type,

  -- Red zone / goal line
  CAST(CASE WHEN yardline_100 <= 20 THEN 1 ELSE 0 END AS INTEGER) AS red_zone_flag,
  CAST(CASE WHEN yardline_100 <= 5  THEN 1 ELSE 0 END AS INTEGER) AS goal_line_flag,

  -- Two-minute drill
  CAST(CASE WHEN half_seconds_remaining <= 120 THEN 1 ELSE 0 END AS INTEGER) AS two_minute_flag,

  -- Game type
  CASE
    WHEN season_type = 'REG' THEN 'regular'
    WHEN season_type = 'POST' THEN 'postseason'
    ELSE season_type
  END AS season_type,

  -- Metadata
  ingestion_ts

FROM raw_pbp
WHERE play_type NOT IN ('no_play', 'kickoff', 'extra_point')
   OR play_type IS NULL
;


-- =============================================================================
-- STAGING: stg_drives
-- Source:  raw_pbp (aggregated)
-- Grain:   game_id + drive_id
-- =============================================================================

CREATE OR REPLACE VIEW stg_drives AS
SELECT
  game_id,
  CAST(drive                AS INTEGER) AS drive_id,
  CAST(season               AS INTEGER) AS season,
  CASE posteam WHEN 'OAK' THEN 'LV' WHEN 'SD' THEN 'LAC' WHEN 'STL' THEN 'LA' ELSE posteam END AS posteam,
  CASE defteam WHEN 'OAK' THEN 'LV' WHEN 'SD' THEN 'LAC' WHEN 'STL' THEN 'LA' ELSE defteam END AS defteam,

  MIN(yardline_100)                       AS end_yardline_100,
  MAX(yardline_100)                       AS start_yardline_100,
  COUNT(*)                                AS plays,
  SUM(CAST(yards_gained AS DOUBLE))       AS yards_gained,
  MAX(qtr_seconds_remaining)              AS drive_start_qtr_secs,
  MIN(qtr_seconds_remaining)              AS drive_end_qtr_secs,
  SUM(CAST(pass_attempt AS INTEGER))      AS pass_attempts,
  SUM(CAST(rush_attempt AS INTEGER))      AS rush_attempts,
  SUM(CAST(touchdown    AS INTEGER))      AS touchdowns,
  SUM(CAST(penalty      AS INTEGER))      AS penalties,
  SUM(CAST(fumble_lost  AS INTEGER))      AS fumbles_lost,
  SUM(CAST(interception AS INTEGER))      AS interceptions,
  MAX(CAST(CASE WHEN yardline_100 <= 20 THEN 1 ELSE 0 END AS INTEGER)) AS reached_red_zone,

  -- Drive result derived from fixed_drive_result column if present
  MAX(fixed_drive_result)                 AS drive_result,

  MIN(ingestion_ts)                       AS ingestion_ts

FROM raw_pbp
WHERE drive IS NOT NULL
  AND play_type NOT IN ('no_play','kickoff','extra_point')
GROUP BY game_id, drive, season, posteam, defteam
;


-- =============================================================================
-- STAGING: stg_teams
-- Grain:   team_abbr (canonical, all-time)
-- Purpose: Canonical team dimension with current / historical abbrs.
-- =============================================================================

CREATE OR REPLACE VIEW stg_teams AS
SELECT DISTINCT
  CASE team_abbr
    WHEN 'OAK' THEN 'LV'
    WHEN 'SD'  THEN 'LAC'
    WHEN 'STL' THEN 'LA'
    ELSE team_abbr
  END AS team_abbr,
  team_name,
  team_conf,
  team_division
FROM (
  SELECT DISTINCT
    home_team AS team_abbr,
    NULL      AS team_name,
    NULL      AS team_conf,
    NULL      AS team_division
  FROM raw_schedules

  UNION ALL

  SELECT DISTINCT
    team_abbr,
    team_name,
    team_conf,
    team_division
  FROM raw_rosters
  WHERE team_abbr IS NOT NULL
) teams
;


-- =============================================================================
-- STAGING: stg_players
-- Source:  raw_players
-- Grain:   gsis_id
-- Purpose: Canonical player registry with cleaned position groups.
-- =============================================================================

CREATE OR REPLACE VIEW stg_players AS
SELECT
  gsis_id                                 AS player_id,
  display_name                            AS full_name,
  first_name,
  last_name,
  UPPER(position)                         AS position,
  CASE UPPER(position)
    WHEN 'QB'  THEN 'QB'
    WHEN 'HB'  THEN 'RB'  WHEN 'RB' THEN 'RB'  WHEN 'FB' THEN 'RB'
    WHEN 'WR'  THEN 'WR'
    WHEN 'TE'  THEN 'TE'
    WHEN 'C'   THEN 'OL'  WHEN 'G'  THEN 'OL'  WHEN 'T'  THEN 'OL'
    WHEN 'DE'  THEN 'DL'  WHEN 'DT' THEN 'DL'  WHEN 'NT' THEN 'DL'
    WHEN 'ILB' THEN 'LB'  WHEN 'OLB' THEN 'LB' WHEN 'MLB' THEN 'LB'
    WHEN 'CB'  THEN 'CB'
    WHEN 'DB'  THEN 'S'   WHEN 'FS' THEN 'S'   WHEN 'SS' THEN 'S'
    WHEN 'K'   THEN 'K'
    WHEN 'P'   THEN 'P'
    ELSE 'OTHER'
  END AS position_group,
  birth_date,
  college,
  height,
  weight,
  entry_year,
  rookie_year,
  draft_club,
  draft_number,
  CASE WHEN status = 'ACT' THEN 1 ELSE 0 END AS active_flag,
  headshot_url,
  short_name,
  ingestion_ts

FROM raw_players
WHERE gsis_id IS NOT NULL
;
