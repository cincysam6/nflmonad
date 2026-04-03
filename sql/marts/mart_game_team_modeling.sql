-- =============================================================================
-- MART: mart_game_modeling
-- Grain:   game_id (one row per completed or scheduled game)
-- Purpose: Master model training table for game-level prediction.
--          Win/spread/total prediction features + targets.
-- Leakage rule: All features derived from data BEFORE kickoff.
--               home_score / away_score / total_points are TARGETS only.
-- =============================================================================

CREATE OR REPLACE VIEW mart_game_modeling AS
SELECT
  -- ===== IDENTIFIERS =====
  g.game_id,
  g.season,
  g.week,
  g.season_type,
  g.game_date,
  g.home_team,
  g.away_team,

  -- ===== SCHEDULE CONTEXT (pre-game available) =====
  g.surface,
  g.roof,
  g.location,
  g.stadium,
  g.div_game,
  g.conf_game,
  g.primetime_flag,
  g.weekday,
  g.kickoff_time_local,

  -- ===== REST / BYE (pre-game available) =====
  g.home_rest_days,
  g.home_bye_prior,
  g.home_short_week,
  g.away_rest_days,
  g.away_bye_prior,
  g.away_short_week,
  g.rest_days_advantage_home,

  -- ===== HOME TEAM FORM (all lagged) =====
  h_form.epa_pp_blended          AS home_epa_pp_blended,
  h_form.def_epa_pp_blended      AS home_def_epa_pp_blended,
  h_form.epa_pp_std              AS home_epa_pp_std,
  h_form.pass_epa_std            AS home_pass_epa_std,
  h_form.rush_epa_std            AS home_rush_epa_std,
  h_form.def_epa_pp_std          AS home_def_epa_pp_std,
  h_form.def_pass_epa_std        AS home_def_pass_epa_std,
  h_form.def_rush_epa_std        AS home_def_rush_epa_std,
  h_form.success_rate_std        AS home_success_rate_std,
  h_form.def_success_rate_allowed_std AS home_def_success_rate_allowed_std,
  h_form.explosive_pass_rate_std AS home_explosive_pass_rate_std,
  h_form.pass_rate_std           AS home_pass_rate_std,
  h_form.turnovers_per_game_std  AS home_turnovers_pg_std,
  h_form.def_to_forced_std       AS home_def_to_forced_std,
  h_form.rz_td_rate_std          AS home_rz_td_rate_std,
  h_form.pts_for_std             AS home_pts_for_std,
  h_form.pts_against_std         AS home_pts_against_std,
  h_form.games_played_std        AS home_games_played,
  h_form.epa_pp_l5               AS home_epa_pp_l5,
  h_form.pass_epa_l5             AS home_pass_epa_l5,
  h_form.def_epa_pp_l5           AS home_def_epa_pp_l5,
  h_form.pts_for_l5              AS home_pts_for_l5,
  h_form.pts_against_l5          AS home_pts_against_l5,
  h_form.epa_pp_l3               AS home_epa_pp_l3,
  h_form.prior_epa_pp            AS home_prior_epa_pp,
  h_form.prior_def_epa_pp        AS home_prior_def_epa_pp,

  -- ===== AWAY TEAM FORM (all lagged) =====
  a_form.epa_pp_blended          AS away_epa_pp_blended,
  a_form.def_epa_pp_blended      AS away_def_epa_pp_blended,
  a_form.epa_pp_std              AS away_epa_pp_std,
  a_form.pass_epa_std            AS away_pass_epa_std,
  a_form.rush_epa_std            AS away_rush_epa_std,
  a_form.def_epa_pp_std          AS away_def_epa_pp_std,
  a_form.def_pass_epa_std        AS away_def_pass_epa_std,
  a_form.def_rush_epa_std        AS away_def_rush_epa_std,
  a_form.success_rate_std        AS away_success_rate_std,
  a_form.def_success_rate_allowed_std AS away_def_success_rate_allowed_std,
  a_form.explosive_pass_rate_std AS away_explosive_pass_rate_std,
  a_form.pass_rate_std           AS away_pass_rate_std,
  a_form.turnovers_per_game_std  AS away_turnovers_pg_std,
  a_form.def_to_forced_std       AS away_def_to_forced_std,
  a_form.rz_td_rate_std          AS away_rz_td_rate_std,
  a_form.pts_for_std             AS away_pts_for_std,
  a_form.pts_against_std         AS away_pts_against_std,
  a_form.games_played_std        AS away_games_played,
  a_form.epa_pp_l5               AS away_epa_pp_l5,
  a_form.pass_epa_l5             AS away_pass_epa_l5,
  a_form.def_epa_pp_l5           AS away_def_epa_pp_l5,
  a_form.pts_for_l5              AS away_pts_for_l5,
  a_form.pts_against_l5          AS away_pts_against_l5,
  a_form.epa_pp_l3               AS away_epa_pp_l3,
  a_form.prior_epa_pp            AS away_prior_epa_pp,
  a_form.prior_def_epa_pp        AS away_prior_def_epa_pp,

  -- ===== DIFFERENTIAL FEATURES =====
  h_form.epa_pp_blended - a_form.epa_pp_blended AS epa_diff_home_minus_away,
  (h_form.epa_pp_blended + a_form.def_epa_pp_blended) -
  (a_form.epa_pp_blended + h_form.def_epa_pp_blended) AS composite_strength_diff,

  -- ===== INJURY BURDEN (pre-game) =====
  h_inj.total_injury_burden      AS home_injury_burden,
  h_inj.qb_out                   AS home_qb_out,
  h_inj.ol_burden                AS home_ol_burden,
  h_inj.wr_burden                AS home_wr_burden,
  a_inj.total_injury_burden      AS away_injury_burden,
  a_inj.qb_out                   AS away_qb_out,
  a_inj.ol_burden                AS away_ol_burden,
  a_inj.wr_burden                AS away_wr_burden,

  -- ===== MARKET FEATURES (optional, joined when available) =====
  odds.home_spread                AS opening_home_spread,
  odds.total_line                 AS opening_total,
  odds.home_ml                    AS opening_home_ml,
  odds_close.closing_spread       AS closing_home_spread,
  odds_close.closing_total        AS closing_total,

  -- ===== TARGET VARIABLES (post-game; NULL for future games) =====
  g.home_score,
  g.away_score,
  g.total_points,
  g.home_margin,
  g.home_win_flag,
  g.game_completed_flag,

  -- Derived targets from odds (if available)
  CASE
    WHEN g.home_margin IS NOT NULL AND odds_close.closing_spread IS NOT NULL
    THEN CASE WHEN g.home_margin > (-1 * odds_close.closing_spread) THEN 1 ELSE 0 END
  END AS home_covered_flag,
  CASE
    WHEN g.total_points IS NOT NULL AND odds_close.closing_total IS NOT NULL
    THEN CASE WHEN g.total_points > odds_close.closing_total THEN 1 ELSE 0 END
  END AS over_flag

FROM int_game_base g

-- Home team form: use week-of-game row (contains only prior-week data)
LEFT JOIN int_team_form h_form
  ON g.home_team = h_form.team
  AND g.season   = h_form.season
  AND g.week     = h_form.week

-- Away team form
LEFT JOIN int_team_form a_form
  ON g.away_team = a_form.team
  AND g.season   = a_form.season
  AND g.week     = a_form.week

-- Injury reports filed before this game week
LEFT JOIN int_injury_team_impact h_inj
  ON g.home_team = h_inj.team
  AND g.season   = h_inj.season
  AND g.week     = h_inj.week

LEFT JOIN int_injury_team_impact a_inj
  ON g.away_team = a_inj.team
  AND g.season   = a_inj.season
  AND g.week     = a_inj.week

-- Opening odds (earliest line per game)
LEFT JOIN (
  SELECT game_id, home_spread, total_line, home_ml
  FROM stg_external_odds_game
  QUALIFY ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY market_timestamp) = 1
) odds ON g.game_id = odds.game_id

-- Closing odds
LEFT JOIN (
  SELECT game_id, closing_spread, closing_total
  FROM stg_external_odds_game
  QUALIFY ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY market_timestamp DESC) = 1
) odds_close ON g.game_id = odds_close.game_id
;


-- =============================================================================
-- MART: mart_team_week_modeling
-- Grain:   season + week + team
-- Purpose: Team priors / rolling features for downstream model joining.
-- =============================================================================

CREATE OR REPLACE VIEW mart_team_week_modeling AS
SELECT
  tf.team,
  tf.season,
  tf.week,
  tf.game_id,
  tf.game_date,

  -- Form features (all lagged)
  tf.epa_pp_blended,
  tf.def_epa_pp_blended,
  tf.epa_pp_std,
  tf.pass_epa_std,
  tf.rush_epa_std,
  tf.def_epa_pp_std,
  tf.def_pass_epa_std,
  tf.def_rush_epa_std,
  tf.success_rate_std,
  tf.def_success_rate_allowed_std,
  tf.explosive_pass_rate_std,
  tf.pass_rate_std,
  tf.turnovers_per_game_std,
  tf.def_to_forced_std,
  tf.rz_td_rate_std,
  tf.pts_for_std,
  tf.pts_against_std,
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
