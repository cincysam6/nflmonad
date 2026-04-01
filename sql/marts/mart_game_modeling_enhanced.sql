-- =============================================================================
-- MART: mart_game_modeling (ENHANCED)
-- Replaces the original version. Now includes market, weather, travel,
-- schedule spot, Elo/power ratings, and venue context.
-- Grain:   game_id
--
-- TWO FLAVORS built as separate queries in the mart runner:
--   mart_game_modeling_no_market  → pure football signal (market-agnostic)
--   mart_game_modeling            → full feature set including market
--
-- LEAKAGE CONTRACT:
--   Football features: pre-game safe (lagged rolling metrics)
--   opening_* market: safe for pregame (early_week horizon)
--   forecast weather: safe when forecast_run_ts < prediction_cutoff_ts
--   closing_* market: NOT safe for pregame prediction
--   historical weather: NOT safe for pregame; use for retrospective only
-- =============================================================================

CREATE OR REPLACE VIEW mart_game_modeling AS
WITH spread_open AS (
  SELECT game_id,
         opening_line AS opening_spread, opening_implied_prob AS opening_home_win_prob,
         closing_line AS closing_spread, move_open_to_close AS spread_move,
         crossed_key_3_flag, crossed_key_7_flag, crossed_key_10_flag,
         line_volatility_score AS spread_volatility,
         home_fav_opening, home_dog_opening, road_fav_opening,
         close_consensus_dispersion AS spread_close_dispersion
  FROM int_market_open_close
  WHERE market_type = 'spread' AND selection = 'home'
),
total_open AS (
  SELECT game_id,
         opening_line AS opening_total, closing_line AS closing_total,
         move_open_to_close AS total_move
  FROM int_market_open_close
  WHERE market_type = 'total' AND selection = 'over'
),
ml_open AS (
  SELECT game_id,
         opening_line AS opening_home_ml, closing_line AS closing_home_ml
  FROM int_market_open_close
  WHERE market_type = 'moneyline' AND selection = 'home'
)

SELECT
  -- ===== IDENTIFIERS =====
  g.game_id, g.season, g.week, g.season_type, g.game_date,
  g.home_team, g.away_team, g.game_completed_flag,

  -- ===== SCHEDULE CONTEXT =====
  g.surface, g.roof, g.location, g.stadium,
  g.div_game, g.conf_game, g.primetime_flag, g.weekday, g.kickoff_time_local,

  -- ===== VENUE =====
  st.roof_type AS stadium_roof_type, st.surface_type AS stadium_surface_type,
  st.elevation_m, st.altitude_flag, st.international_game_flag,
  st.venue_category,

  -- ===== REST (pre-game safe) =====
  g.home_rest_days, g.home_bye_prior, g.home_short_week,
  g.away_rest_days, g.away_bye_prior, g.away_short_week,
  g.rest_days_advantage_home,
  h_ss.third_road_in_four_flag AS home_third_road_four,
  a_ss.third_road_in_four_flag AS away_third_road_four,
  h_ss.consecutive_road_games_prior AS home_consec_road,
  a_ss.consecutive_road_games_prior AS away_consec_road,
  h_ss.long_rest_flag AS home_long_rest, a_ss.long_rest_flag AS away_long_rest,

  -- ===== TRAVEL (pre-game safe) =====
  a_trvl.travel_km, a_trvl.travel_miles,
  a_trvl.timezone_shift_hours AS away_tz_shift,
  a_trvl.east_to_west_flag AS away_east_to_west,
  a_trvl.west_to_east_flag AS away_west_to_east,
  a_trvl.west_east_early_flag AS away_west_east_early,
  a_trvl.travel_distance_bucket,
  a_trvl.intl_travel_flag AS away_intl_travel,

  -- ===== HOME TEAM FORM (pre-game safe) =====
  h.epa_pp_blended AS home_epa_blended, h.epa_pp_std AS home_epa_std,
  h.epa_pp_l5 AS home_epa_l5, h.epa_pp_l3 AS home_epa_l3,
  h.pass_epa_std AS home_pass_epa, h.rush_epa_std AS home_rush_epa,
  h.def_epa_pp_blended AS home_def_epa, h.def_pass_epa_std AS home_def_pass_epa,
  h.success_rate_std AS home_success_rate, h.pass_rate_std AS home_pass_rate,
  h.explosive_pass_rate_std AS home_explosive_pass, h.rz_td_rate_std AS home_rz_td_rate,
  h.turnovers_per_game_std AS home_to_pg, h.def_to_forced_std AS home_def_to_forced,
  h.pts_for_std AS home_pts_for_pg, h.pts_against_std AS home_pts_against_pg,
  h.games_played_std AS home_games_played, h.wins_l5 AS home_wins_l5,

  -- ===== AWAY TEAM FORM (pre-game safe) =====
  a.epa_pp_blended AS away_epa_blended, a.epa_pp_std AS away_epa_std,
  a.epa_pp_l5 AS away_epa_l5, a.epa_pp_l3 AS away_epa_l3,
  a.pass_epa_std AS away_pass_epa, a.rush_epa_std AS away_rush_epa,
  a.def_epa_pp_blended AS away_def_epa, a.def_pass_epa_std AS away_def_pass_epa,
  a.success_rate_std AS away_success_rate, a.pass_rate_std AS away_pass_rate,
  a.explosive_pass_rate_std AS away_explosive_pass, a.rz_td_rate_std AS away_rz_td_rate,
  a.turnovers_per_game_std AS away_to_pg, a.def_to_forced_std AS away_def_to_forced,
  a.pts_for_std AS away_pts_for_pg, a.pts_against_std AS away_pts_against_pg,
  a.games_played_std AS away_games_played, a.wins_l5 AS away_wins_l5,

  -- ===== COMPOSITE DIFFERENTIALS =====
  h.epa_pp_blended - a.epa_pp_blended AS epa_diff,
  (h.epa_pp_blended + a.def_epa_pp_blended) -
  (a.epa_pp_blended + h.def_epa_pp_blended) AS composite_diff,

  -- ===== ELO / POWER RATINGS (pre-game safe) =====
  hpr.elo_rating_pregame AS home_elo, apr.elo_rating_pregame AS away_elo,
  hpr.elo_rating_pregame - apr.elo_rating_pregame AS elo_diff,
  hpr.offensive_power AS home_off_power, apr.offensive_power AS away_off_power,
  hpr.defensive_power AS home_def_power, apr.defensive_power AS away_def_power,
  hpr.total_environment_rating + apr.total_environment_rating AS total_env_combined,

  -- ===== INJURY BURDEN (pre-game safe) =====
  h_inj.total_injury_burden AS home_injury_burden, a_inj.total_injury_burden AS away_injury_burden,
  h_inj.qb_out AS home_qb_out, a_inj.qb_out AS away_qb_out,
  h_inj.ol_burden AS home_ol_burden, a_inj.ol_burden AS away_ol_burden,
  h_inj.wr_burden AS home_wr_burden, a_inj.wr_burden AS away_wr_burden,

  -- ===== WEATHER (see LEAKAGE CONTRACT above) =====
  wg.eff_temp_f, wg.eff_wind_mph, wg.eff_precip_in,
  wg.wind_bucket, wg.temp_bucket, wg.weather_condition,
  wg.adverse_weather_score, wg.passing_condition_score, wg.total_suppression_score,
  wg.cold_flag, wg.wind_20_flag, wg.wind_15_flag, wg.precip_flag, wg.snow_flag,
  wg.roof_closed_flag, wg.open_air_weather_applicable, wg.cold_wind_interaction,
  wg.weather_source_type,  -- 'historical' | 'forecast' — check this before using!
  wg.forecast_run_ts,

  -- ===== MARKET FEATURES — OPENING (pre-game safe) =====
  so.opening_spread, so.opening_home_win_prob, so.opening_home_ml,
  to_.opening_total,
  CASE WHEN so.opening_spread IS NOT NULL AND to_.opening_total IS NOT NULL
    THEN to_.opening_total / 2.0 - so.opening_spread / 2.0 END AS opening_home_team_total,
  CASE WHEN so.opening_spread IS NOT NULL AND to_.opening_total IS NOT NULL
    THEN to_.opening_total / 2.0 + so.opening_spread / 2.0 END AS opening_away_team_total,
  so.home_fav_opening, so.home_dog_opening, so.road_fav_opening,

  -- ===== MARKET FEATURES — CLOSING (NOT safe for pregame prediction) =====
  so.closing_spread, to_.closing_total, ml_open.closing_home_ml,
  so.spread_move, to_.total_move,
  so.crossed_key_3_flag, so.crossed_key_7_flag, so.crossed_key_10_flag,
  so.spread_volatility, so.spread_close_dispersion,

  -- ===== MODEL VS MARKET COMPARISON PLACEHOLDERS =====
  -- Populated externally after model generates predictions
  NULL::DOUBLE AS model_home_win_prob,
  NULL::DOUBLE AS model_spread_prediction,
  NULL::DOUBLE AS model_total_prediction,
  NULL::DOUBLE AS model_edge_spread,
  NULL::DOUBLE AS model_edge_total,

  -- ===== DATA AVAILABILITY FLAGS (null-safe for downstream) =====
  CASE WHEN so.opening_spread  IS NOT NULL THEN 1 ELSE 0 END AS odds_available,
  CASE WHEN wg.eff_temp_f      IS NOT NULL THEN 1 ELSE 0 END AS weather_available,
  CASE WHEN hpr.elo_rating_pregame IS NOT NULL THEN 1 ELSE 0 END AS elo_available,

  -- ===== TARGETS =====
  g.home_score, g.away_score, g.total_points, g.home_margin, g.home_win_flag,
  CASE WHEN g.home_margin IS NOT NULL AND so.closing_spread IS NOT NULL
    THEN CASE WHEN g.home_margin > (-1 * so.closing_spread) THEN 1 ELSE 0 END END AS home_covered_close,
  CASE WHEN g.total_points IS NOT NULL AND to_.closing_total IS NOT NULL
    THEN CASE WHEN g.total_points > to_.closing_total THEN 1 ELSE 0 END END AS over_close_flag

FROM int_game_base g
LEFT JOIN mart_team_week_modeling h    ON g.home_team=h.team AND g.season=h.season AND g.week=h.week
LEFT JOIN mart_team_week_modeling a    ON g.away_team=a.team AND g.season=a.season AND g.week=a.week
LEFT JOIN int_team_power_rating hpr    ON g.home_team=hpr.team AND g.game_id=hpr.game_id
LEFT JOIN int_team_power_rating apr    ON g.away_team=apr.team AND g.game_id=apr.game_id
LEFT JOIN int_team_schedule_spot h_ss  ON g.home_team=h_ss.team AND g.game_id=h_ss.game_id
LEFT JOIN int_team_schedule_spot a_ss  ON g.away_team=a_ss.team AND g.game_id=a_ss.game_id
LEFT JOIN int_team_travel_game a_trvl  ON g.away_team=a_trvl.team AND g.game_id=a_trvl.game_id AND a_trvl.is_away_team=1
LEFT JOIN int_injury_team_impact h_inj ON g.home_team=h_inj.team AND g.season=h_inj.season AND g.week=h_inj.week
LEFT JOIN int_injury_team_impact a_inj ON g.away_team=a_inj.team AND g.season=a_inj.season AND g.week=a_inj.week
LEFT JOIN int_weather_game wg          ON g.game_id=wg.game_id
LEFT JOIN stg_stadium st               ON g.home_team=st.team AND g.season BETWEEN st.season_start AND st.season_end
LEFT JOIN spread_open so               ON g.game_id=so.game_id
LEFT JOIN total_open to_               ON g.game_id=to_.game_id
LEFT JOIN ml_open                      ON g.game_id=ml_open.game_id
;

-- ---- Football-only variant (no market columns) ----------------------------
CREATE OR REPLACE VIEW mart_game_modeling_no_market AS
SELECT
  game_id, season, week, season_type, game_date, home_team, away_team,
  game_completed_flag, surface, roof, location, stadium,
  div_game, conf_game, primetime_flag, weekday,
  venue_category, elevation_m, altitude_flag, international_game_flag,
  home_rest_days, away_rest_days, rest_days_advantage_home,
  home_short_week, away_short_week, home_bye_prior, away_bye_prior,
  home_long_rest, away_long_rest,
  home_third_road_four, away_third_road_four,
  travel_km, travel_miles, away_tz_shift, away_west_east_early, away_intl_travel,
  home_epa_blended, home_epa_l5, home_pass_epa, home_rush_epa,
  home_def_epa, home_success_rate, home_pass_rate, home_rz_td_rate,
  home_games_played, home_wins_l5,
  away_epa_blended, away_epa_l5, away_pass_epa, away_rush_epa,
  away_def_epa, away_success_rate, away_pass_rate, away_rz_td_rate,
  away_games_played, away_wins_l5,
  epa_diff, composite_diff,
  home_elo, away_elo, elo_diff,
  home_off_power, away_off_power, home_def_power, away_def_power,
  total_env_combined,
  home_injury_burden, away_injury_burden, home_qb_out, away_qb_out,
  home_wr_burden, away_wr_burden,
  eff_temp_f, eff_wind_mph, eff_precip_in, wind_bucket, temp_bucket,
  adverse_weather_score, passing_condition_score, total_suppression_score,
  roof_closed_flag, cold_wind_interaction,
  weather_source_type, forecast_run_ts,
  weather_available,
  home_score, away_score, total_points, home_margin, home_win_flag

FROM mart_game_modeling
;
