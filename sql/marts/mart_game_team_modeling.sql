-- =============================================================================
-- MART: mart_game_team_modeling.sql
-- Replaces: sql/marts/mart_game_team_modeling.sql
--
-- Fix: mart_game_modeling was referencing g.schedule_spread_line,
--   g.schedule_total_line, g.schedule_home_ml from int_game_base (alias "g").
--   int_game_base does NOT select those columns — they live in stg_games.
--   Fix: added a direct LEFT JOIN to stg_games (alias "sg") to pull odds/
--   weather columns that int_game_base intentionally excludes.
--
-- Also: mart_backtest_game referenced home_games_played and away_games_played
--   which don't exist in mart_game_modeling output. Changed to home_games_played
--   / away_games_played derived from the form tables (games_played_std).
--   Filter loosened to games_played_std >= 1 for both teams.
-- =============================================================================


-- =============================================================================
-- MART: mart_game_modeling
-- Grain:   game_id
-- =============================================================================

CREATE OR REPLACE VIEW mart_game_modeling AS
SELECT
  -- Identifiers
  g.game_id,
  g.season,
  g.week,
  g.season_type,
  g.game_date,
  g.home_team,
  g.away_team,
  g.game_completed_flag,

  -- Schedule context (pre-game)
  g.surface,
  g.roof,
  g.location,
  g.stadium,
  g.div_game,
  g.conf_game,
  g.primetime_flag,
  g.weekday,
  g.kickoff_time_local,

  -- Rest / bye (computed in int_game_base)
  g.home_rest_days,
  g.home_bye_prior,
  g.home_short_week,
  g.away_rest_days,
  g.away_bye_prior,
  g.away_short_week,
  g.rest_days_advantage_home,

  -- Home team form (all lagged — from int_team_form)
  h_form.epa_pp_blended               AS home_epa_pp_blended,
  h_form.def_epa_pp_blended           AS home_def_epa_pp_blended,
  h_form.epa_pp_std                   AS home_epa_pp_std,
  h_form.pass_epa_std                 AS home_pass_epa_std,
  h_form.rush_epa_std                 AS home_rush_epa_std,
  h_form.def_epa_pp_std               AS home_def_epa_pp_std,
  h_form.def_pass_epa_std             AS home_def_pass_epa_std,
  h_form.def_rush_epa_std             AS home_def_rush_epa_std,
  h_form.success_rate_std             AS home_success_rate_std,
  h_form.def_success_rate_allowed_std AS home_def_success_rate_allowed_std,
  h_form.explosive_pass_rate_std      AS home_explosive_pass_rate_std,
  h_form.pass_rate_std                AS home_pass_rate_std,
  h_form.turnovers_per_game_std       AS home_turnovers_pg_std,
  h_form.def_to_forced_std            AS home_def_to_forced_std,
  h_form.rz_td_rate_std               AS home_rz_td_rate_std,
  h_form.pts_for_std                  AS home_pts_for_std,
  h_form.pts_against_std              AS home_pts_against_std,
  h_form.def_sacks_std                AS home_def_sacks_std,
  h_form.sacks_allowed_std            AS home_sacks_allowed_std,
  h_form.games_played_std             AS home_games_played,
  h_form.epa_pp_l5                    AS home_epa_pp_l5,
  h_form.pass_epa_l5                  AS home_pass_epa_l5,
  h_form.def_epa_pp_l5                AS home_def_epa_pp_l5,
  h_form.pts_for_l5                   AS home_pts_for_l5,
  h_form.pts_against_l5              AS home_pts_against_l5,
  h_form.epa_pp_l3                    AS home_epa_pp_l3,
  h_form.prior_epa_pp                 AS home_prior_epa_pp,
  h_form.prior_def_epa_pp             AS home_prior_def_epa_pp,

  -- Away team form (all lagged)
  a_form.epa_pp_blended               AS away_epa_pp_blended,
  a_form.def_epa_pp_blended           AS away_def_epa_pp_blended,
  a_form.epa_pp_std                   AS away_epa_pp_std,
  a_form.pass_epa_std                 AS away_pass_epa_std,
  a_form.rush_epa_std                 AS away_rush_epa_std,
  a_form.def_epa_pp_std               AS away_def_epa_pp_std,
  a_form.def_pass_epa_std             AS away_def_pass_epa_std,
  a_form.def_rush_epa_std             AS away_def_rush_epa_std,
  a_form.success_rate_std             AS away_success_rate_std,
  a_form.def_success_rate_allowed_std AS away_def_success_rate_allowed_std,
  a_form.explosive_pass_rate_std      AS away_explosive_pass_rate_std,
  a_form.pass_rate_std                AS away_pass_rate_std,
  a_form.turnovers_per_game_std       AS away_turnovers_pg_std,
  a_form.def_to_forced_std            AS away_def_to_forced_std,
  a_form.rz_td_rate_std               AS away_rz_td_rate_std,
  a_form.pts_for_std                  AS away_pts_for_std,
  a_form.pts_against_std              AS away_pts_against_std,
  a_form.def_sacks_std                AS away_def_sacks_std,
  a_form.sacks_allowed_std            AS away_sacks_allowed_std,
  a_form.games_played_std             AS away_games_played,
  a_form.epa_pp_l5                    AS away_epa_pp_l5,
  a_form.pass_epa_l5                  AS away_pass_epa_l5,
  a_form.def_epa_pp_l5                AS away_def_epa_pp_l5,
  a_form.pts_for_l5                   AS away_pts_for_l5,
  a_form.pts_against_l5               AS away_pts_against_l5,
  a_form.epa_pp_l3                    AS away_epa_pp_l3,
  a_form.prior_epa_pp                 AS away_prior_epa_pp,
  a_form.prior_def_epa_pp             AS away_prior_def_epa_pp,

  -- Injury context (from int_injury_team_impact)
  h_inj.total_injury_burden           AS home_injury_burden,
  h_inj.qb_out                        AS home_qb_out,
  h_inj.ol_burden                     AS home_ol_burden,
  h_inj.wr_burden                     AS home_wr_burden,
  h_inj.te_burden                     AS home_te_burden,
  h_inj.rb_out                        AS home_rb_out,
  h_inj.dl_burden                     AS home_dl_burden,
  h_inj.cb_burden                     AS home_cb_burden,
  a_inj.total_injury_burden           AS away_injury_burden,
  a_inj.qb_out                        AS away_qb_out,
  a_inj.ol_burden                     AS away_ol_burden,
  a_inj.wr_burden                     AS away_wr_burden,
  a_inj.te_burden                     AS away_te_burden,
  a_inj.rb_out                        AS away_rb_out,
  a_inj.dl_burden                     AS away_dl_burden,
  a_inj.cb_burden                     AS away_cb_burden,

  -- Market context from stg_games (nflverse ESPN lines)
  -- FIX: these cols are in stg_games but NOT in int_game_base;
  --      pull directly from stg_games via separate join alias "sg"
  sg.schedule_spread_line             AS spread_line_nflverse,
  sg.schedule_total_line              AS total_line_nflverse,
  sg.schedule_home_ml                 AS home_ml_nflverse,
  sg.schedule_away_ml                 AS away_ml_nflverse,

  -- Targets (post-game only — NULL for unplayed games)
  g.home_score,
  g.away_score,
  g.total_points,
  g.home_margin,
  g.home_win_flag,

  -- Cover / over flags
  CASE
    WHEN g.home_margin IS NOT NULL AND sg.schedule_spread_line IS NOT NULL
    THEN CASE WHEN g.home_margin > (-1 * sg.schedule_spread_line) THEN 1 ELSE 0 END
  END AS home_covered_flag,
  CASE
    WHEN g.total_points IS NOT NULL AND sg.schedule_total_line IS NOT NULL
    THEN CASE WHEN g.total_points > sg.schedule_total_line THEN 1 ELSE 0 END
  END AS over_flag

FROM int_game_base g

-- Separate join to stg_games for market/weather cols not in int_game_base
LEFT JOIN stg_games sg
  ON g.game_id = sg.game_id

LEFT JOIN int_team_form h_form
  ON g.home_team = h_form.team
 AND g.season    = h_form.season
 AND g.week      = h_form.week

LEFT JOIN int_team_form a_form
  ON g.away_team = a_form.team
 AND g.season    = a_form.season
 AND g.week      = a_form.week

LEFT JOIN int_injury_team_impact h_inj
  ON g.home_team = h_inj.team
 AND g.season    = h_inj.season
 AND g.week      = h_inj.week

LEFT JOIN int_injury_team_impact a_inj
  ON g.away_team = a_inj.team
 AND g.season    = a_inj.season
 AND g.week      = a_inj.week
;


-- =============================================================================
-- MART: mart_team_week_modeling
-- Grain:   season + week + team
-- Fix:     Added def_sacks_std and sacks_allowed_std (both in int_team_form)
--          which are needed by mart_player_week_projection via opp_tmf join
-- =============================================================================

CREATE OR REPLACE VIEW mart_team_week_modeling AS
SELECT
  tf.team,
  tf.season,
  tf.week,
  tf.game_id,
  tf.game_date,

  -- Offensive form (lagged)
  tf.epa_pp_blended,
  tf.epa_pp_std,
  tf.pass_epa_std,
  tf.rush_epa_std,
  tf.success_rate_std,
  tf.explosive_pass_rate_std,
  tf.pass_rate_std,
  tf.turnovers_per_game_std,
  tf.rz_td_rate_std,
  tf.pts_for_std,
  tf.pts_against_std,
  tf.sacks_allowed_std,

  -- Defensive form (lagged)
  tf.def_epa_pp_blended,
  tf.def_epa_pp_std,
  tf.def_pass_epa_std,
  tf.def_rush_epa_std,
  tf.def_success_rate_allowed_std,
  tf.def_to_forced_std,
  tf.def_sacks_std,             -- required by mart_player_week_projection opp join

  -- Rolling window summaries
  tf.games_played_std,
  tf.epa_pp_l5,
  tf.epa_pp_l3,
  tf.prior_epa_pp,
  tf.prior_def_epa_pp,
  tf.wins_l5,
  tf.sacks_allowed_std,
  tf.def_sacks_std,

  -- Injury context
  inj.total_injury_burden,
  inj.qb_out,
  inj.ol_burden,
  inj.wr_burden,
  inj.te_burden,
  inj.rb_out,
  inj.dl_burden,
  inj.cb_burden

FROM int_team_form tf
LEFT JOIN int_injury_team_impact inj
  ON tf.team   = inj.team
 AND tf.season = inj.season
 AND tf.week   = inj.week
;


-- =============================================================================
-- MART: mart_backtest_game
-- Grain:   game_id (completed games only)
-- Fix:     home_games_played / away_games_played come from mart_game_modeling
--          (aliased from h_form.games_played_std / a_form.games_played_std)
-- =============================================================================

CREATE OR REPLACE VIEW mart_backtest_game AS
SELECT
  *,
  CASE
    WHEN season < 2018                THEN 'train'
    WHEN season BETWEEN 2018 AND 2021 THEN 'validation'
    ELSE 'test'
  END AS cv_split
FROM mart_game_modeling
WHERE game_completed_flag = 1
  AND season_type         = 'regular'
  AND COALESCE(home_games_played, 0) >= 1
  AND COALESCE(away_games_played, 0) >= 1
;
