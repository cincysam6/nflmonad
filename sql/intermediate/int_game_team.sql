-- =============================================================================
-- INTERMEDIATE: int_game_base
-- Grain:   game_id (one row per game)
-- Purpose: Full game context with rest days, venue, flags, score outcomes.
--          All features are based only on pre-game available information
--          (rest, travel, surface) PLUS post-game outcomes for training.
-- =============================================================================

CREATE OR REPLACE VIEW int_game_base AS
WITH game_sequence AS (
  -- For each team build a lagged schedule to compute rest days
  SELECT
    tw.game_id,
    tw.season,
    tw.week,
    tw.team,
    tw.game_date,
    tw.home_flag,
    LAG(tw.game_date) OVER (
      PARTITION BY tw.team, tw.season ORDER BY tw.week
    ) AS prev_game_date,
    LAG(tw.week) OVER (
      PARTITION BY tw.team, tw.season ORDER BY tw.week
    ) AS prev_week
  FROM stg_team_week tw
  WHERE tw.season_type = 'regular'
),
rest_days AS (
  SELECT
    game_id,
    team,
    season,
    week,
    home_flag,
    CASE
      WHEN prev_game_date IS NULL THEN NULL  -- first game of season (bye before)
      ELSE DATEDIFF('day', prev_game_date, game_date)
    END AS rest_days,
    CASE WHEN prev_game_date IS NULL THEN 1 ELSE 0 END AS bye_week_prior_flag,
    CASE
      WHEN prev_game_date IS NOT NULL
       AND DATEDIFF('day', prev_game_date, game_date) <= 6 THEN 1
      ELSE 0
    END AS short_week_flag
  FROM game_sequence
),
home_rest AS (
  SELECT game_id, rest_days AS home_rest_days,
         bye_week_prior_flag AS home_bye_prior, short_week_flag AS home_short_week
  FROM rest_days WHERE home_flag = 1
),
away_rest AS (
  SELECT game_id, rest_days AS away_rest_days,
         bye_week_prior_flag AS away_bye_prior, short_week_flag AS away_short_week
  FROM rest_days WHERE home_flag = 0
)

SELECT
  g.game_id,
  g.season,
  g.week,
  g.season_type,
  g.game_date,
  g.kickoff_time_local,
  g.weekday,
  g.home_team,
  g.away_team,
  g.home_score,
  g.away_score,
  g.total_points,
  g.home_margin,
  g.home_win_flag,
  g.game_completed_flag,

  -- Venue
  g.stadium,
  g.surface,
  g.roof,
  g.location,

  -- Game context flags
  g.div_game,
  g.conf_game,
  g.primetime_flag,

  -- Rest
  hr.home_rest_days,
  hr.home_bye_prior,
  hr.home_short_week,
  ar.away_rest_days,
  ar.away_bye_prior,
  ar.away_short_week,
  COALESCE(hr.home_rest_days, 7) - COALESCE(ar.away_rest_days, 7) AS rest_days_advantage_home,

  -- Reference IDs
  g.pfr_game_id,
  g.espn_game_id,
  g.nfl_detail_id

FROM stg_games g
LEFT JOIN home_rest hr ON g.game_id = hr.game_id
LEFT JOIN away_rest ar ON g.game_id = ar.game_id
;


-- =============================================================================
-- INTERMEDIATE: int_team_game
-- Grain:   game_id + team (two rows per game)
-- Purpose: Team-level offensive/defensive game metrics from PBP.
--          Join this to int_game_base for full game context.
-- =============================================================================

CREATE OR REPLACE VIEW int_team_game AS
WITH pbp_offense AS (
  SELECT
    game_id,
    posteam                       AS team,
    COUNT(*)                      AS offensive_plays,
    SUM(pass_attempt)             AS pass_attempts,
    SUM(rush_attempt)             AS rush_attempts,
    SUM(yards_gained)             AS total_yards,
    -- EPA
    AVG(epa)                      AS epa_per_play,
    SUM(epa)                      AS total_epa,
    AVG(CASE WHEN pass_attempt=1 THEN epa END) AS pass_epa_per_dropback,
    AVG(CASE WHEN rush_attempt=1 THEN epa END) AS rush_epa_per_carry,
    -- Success rate (EPA >= 0)
    AVG(CAST(success AS DOUBLE))  AS success_rate,
    AVG(CASE WHEN pass_attempt=1 THEN CAST(success AS DOUBLE) END) AS pass_success_rate,
    AVG(CASE WHEN rush_attempt=1 THEN CAST(success AS DOUBLE) END) AS rush_success_rate,
    -- Explosive plays
    AVG(CASE WHEN pass_attempt=1 AND yards_gained >= 20 THEN 1.0 ELSE 0.0 END) AS explosive_pass_rate,
    AVG(CASE WHEN rush_attempt=1 AND yards_gained >= 10 THEN 1.0 ELSE 0.0 END) AS explosive_rush_rate,
    -- Air yards
    AVG(CASE WHEN pass_attempt=1 THEN air_yards END) AS avg_air_yards,
    -- Pace (avg seconds per play)
    AVG(CASE WHEN play_type IN ('pass','run') THEN
      COALESCE(qtr_seconds_remaining, 0) END)        AS avg_qtr_secs_remaining,
    -- Red zone
    SUM(CASE WHEN red_zone_flag=1 THEN 1 ELSE 0 END) AS red_zone_plays,
    SUM(CASE WHEN red_zone_flag=1 AND touchdown=1 THEN 1 ELSE 0 END) AS red_zone_tds,
    -- Turnovers
    SUM(interception)             AS interceptions_thrown,
    SUM(fumble_lost)              AS fumbles_lost,
    SUM(COALESCE(interception,0) + COALESCE(fumble_lost,0)) AS turnovers,
    -- Sacks taken
    SUM(sack)                     AS sacks_allowed,
    -- Pass rate
    CASE WHEN SUM(pass_attempt) + SUM(rush_attempt) > 0
         THEN CAST(SUM(pass_attempt) AS DOUBLE) /
              (SUM(pass_attempt) + SUM(rush_attempt))
    END AS pass_rate

  FROM stg_plays
  WHERE play_type IN ('pass','run')
    AND season_type = 'regular'
  GROUP BY game_id, posteam
),
pbp_defense AS (
  SELECT
    game_id,
    defteam                       AS team,
    AVG(epa)                      AS def_epa_per_play,
    SUM(epa)                      AS def_total_epa,
    AVG(CAST(success AS DOUBLE))  AS def_success_rate_allowed,
    AVG(CASE WHEN pass_attempt=1 THEN epa END) AS def_pass_epa_per_dropback,
    AVG(CASE WHEN rush_attempt=1 THEN epa END) AS def_rush_epa_per_carry,
    SUM(COALESCE(interception,0) + COALESCE(fumble_lost,0)) AS def_turnovers_forced,
    SUM(sack)                     AS def_sacks,
    AVG(CASE WHEN pass_attempt=1 AND yards_gained >= 20 THEN 1.0 ELSE 0.0 END) AS def_explosive_pass_rate_allowed,
    SUM(CASE WHEN red_zone_flag=1 AND touchdown=1 THEN 1 ELSE 0 END) AS def_red_zone_tds_allowed

  FROM stg_plays
  WHERE play_type IN ('pass','run')
    AND season_type = 'regular'
  GROUP BY game_id, defteam
),
team_scores AS (
  SELECT
    game_id,
    team,
    opponent,
    home_flag,
    season,
    week,
    season_type,
    game_date,
    points_for,
    points_against,
    margin,
    win_flag
  FROM stg_team_week
)

SELECT
  ts.game_id,
  ts.team,
  ts.opponent,
  ts.home_flag,
  ts.season,
  ts.week,
  ts.season_type,
  ts.game_date,
  ts.points_for,
  ts.points_against,
  ts.margin,
  ts.win_flag,

  -- Offense metrics
  o.offensive_plays,
  o.pass_attempts,
  o.rush_attempts,
  o.total_yards,
  o.epa_per_play,
  o.total_epa,
  o.pass_epa_per_dropback,
  o.rush_epa_per_carry,
  o.success_rate,
  o.pass_success_rate,
  o.rush_success_rate,
  o.explosive_pass_rate,
  o.explosive_rush_rate,
  o.avg_air_yards,
  o.red_zone_plays,
  o.red_zone_tds,
  CASE WHEN o.red_zone_plays > 0
       THEN CAST(o.red_zone_tds AS DOUBLE) / o.red_zone_plays END AS red_zone_td_rate,
  o.interceptions_thrown,
  o.fumbles_lost,
  o.turnovers,
  o.sacks_allowed,
  o.pass_rate,

  -- Defense metrics
  d.def_epa_per_play,
  d.def_total_epa,
  d.def_success_rate_allowed,
  d.def_pass_epa_per_dropback,
  d.def_rush_epa_per_carry,
  d.def_turnovers_forced,
  d.def_sacks,
  d.def_explosive_pass_rate_allowed,
  d.def_red_zone_tds_allowed

FROM team_scores ts
LEFT JOIN pbp_offense o ON ts.game_id = o.game_id AND ts.team = o.team
LEFT JOIN pbp_defense d ON ts.game_id = d.game_id AND ts.team = d.team
;
