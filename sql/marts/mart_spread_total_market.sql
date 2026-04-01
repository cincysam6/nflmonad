-- =============================================================================
-- MART: mart_spread_modeling
-- Grain:   game_id
-- Purpose: Dedicated spread/side prediction model table.
-- Versions: _no_market (pure football signal) and _with_market
-- LEAKAGE:
--   All football features: pre-game safe (lagged rolling metrics)
--   opening_spread: pre-game safe
--   closing_spread: NOT predictive — CLV research only
-- =============================================================================

CREATE OR REPLACE VIEW mart_spread_modeling AS
WITH spread_open AS (
  SELECT game_id, opening_line AS opening_spread, opening_implied_prob AS opening_home_win_prob,
         home_fav_opening, home_dog_opening, road_fav_opening,
         crossed_key_3_flag AS spread_crossed_3, crossed_key_7_flag AS spread_crossed_7,
         line_volatility_score AS spread_line_volatility, move_open_to_close AS spread_move_otc,
         closing_line AS closing_spread
  FROM int_market_open_close
  WHERE market_type = 'spread' AND selection = 'home'
),
spread_bucket AS (
  -- 24h-before snapshot for mid-week modeling
  SELECT game_id, bucket_line AS spread_24h, bucket_implied_prob AS win_prob_24h
  FROM int_market_time_buckets
  WHERE market_type = 'spread' AND selection = 'home' AND time_bucket = '24h'
)

SELECT
  -- ===== IDENTIFIERS =====
  g.game_id, g.season, g.week, g.season_type,
  g.game_date, g.home_team, g.away_team,
  g.primetime_flag, g.div_game, g.conf_game,
  g.surface, g.roof, g.location,

  -- ===== TEAM STRENGTH DELTAS (pre-game safe) =====
  h.epa_pp_blended - a.epa_pp_blended             AS home_epa_advantage,
  h.def_epa_pp_blended - a.def_epa_pp_blended     AS home_def_epa_advantage,
  (h.epa_pp_blended + a.def_epa_pp_blended) -
  (a.epa_pp_blended + h.def_epa_pp_blended)       AS composite_strength_diff,
  h.pass_epa_std - a.pass_epa_std                 AS home_pass_epa_delta,
  h.rush_epa_std - a.rush_epa_std                 AS home_rush_epa_delta,
  h.success_rate_std - a.success_rate_std          AS home_success_rate_delta,

  -- Individual team form
  h.epa_pp_blended AS home_epa_blended,
  h.epa_pp_l5      AS home_epa_l5,
  h.epa_pp_l3      AS home_epa_l3,
  a.epa_pp_blended AS away_epa_blended,
  a.epa_pp_l5      AS away_epa_l5,
  a.epa_pp_l3      AS away_epa_l3,

  h.def_epa_pp_blended AS home_def_epa_blended,
  a.def_epa_pp_blended AS away_def_epa_blended,
  h.games_played_std   AS home_games_played,
  a.games_played_std   AS away_games_played,

  -- ===== ELO RATINGS =====
  hpr.elo_rating_pregame AS home_elo,
  apr.elo_rating_pregame AS away_elo,
  hpr.elo_rating_pregame - apr.elo_rating_pregame AS elo_diff,
  hpr.offensive_power    AS home_offensive_power,
  apr.offensive_power    AS away_offensive_power,
  hpr.defensive_power    AS home_defensive_power,
  apr.defensive_power    AS away_defensive_power,

  -- ===== REST / SCHEDULING CONTEXT =====
  h_ss.rest_days          AS home_rest_days,
  a_ss.rest_days          AS away_rest_days,
  h_ss.rest_days - COALESCE(a_ss.rest_days, 7) AS rest_advantage_home,
  h_ss.short_week_flag    AS home_short_week,
  a_ss.short_week_flag    AS away_short_week,
  h_ss.bye_week_prior_flag AS home_bye_prior,
  a_ss.bye_week_prior_flag AS away_bye_prior,
  h_ss.long_rest_flag     AS home_long_rest,
  a_ss.long_rest_flag     AS away_long_rest,

  -- ===== TRAVEL (away team) =====
  a_trvl.travel_km        AS away_travel_km,
  a_trvl.timezone_shift_hours AS away_tz_shift,
  a_trvl.west_east_early_flag AS away_west_east_early,
  a_trvl.third_road_in_four_flag AS away_third_road_four,
  a_trvl.intl_travel_flag AS away_intl_travel,

  -- ===== INJURY BURDEN DELTAS =====
  h_inj.total_injury_burden - a_inj.total_injury_burden AS injury_burden_diff_home,
  h_inj.qb_out             AS home_qb_out,
  a_inj.qb_out             AS away_qb_out,
  h_inj.ol_burden          AS home_ol_burden,
  a_inj.ol_burden          AS away_ol_burden,
  h_inj.wr_burden + COALESCE(h_inj.te_burden,0) AS home_skill_burden,
  a_inj.wr_burden + COALESCE(a_inj.te_burden,0) AS away_skill_burden,

  -- ===== WEATHER (pre-game safe with forecast; post-game with historical) =====
  wg.eff_wind_mph, wg.eff_temp_f, wg.eff_precip_in,
  wg.wind_bucket, wg.temp_bucket,
  wg.adverse_weather_score,
  wg.passing_condition_score,
  wg.roof_closed_flag,
  wg.open_air_weather_applicable,
  wg.cold_wind_interaction,
  wg.weather_source_type,

  -- ===== MARKET FEATURES — OPENING (safe for pregame) =====
  so.opening_spread,
  so.opening_home_win_prob,
  so.home_fav_opening,
  so.home_dog_opening,
  so.road_fav_opening,
  sb.spread_24h,
  sb.win_prob_24h,

  -- ===== MARKET FEATURES — CLOSING (NOT safe for pregame; CLV only) =====
  so.closing_spread,
  so.spread_crossed_3,
  so.spread_crossed_7,
  so.spread_line_volatility,
  so.spread_move_otc,

  -- ===== TARGETS =====
  g.home_score,
  g.away_score,
  g.home_margin,
  g.home_win_flag,
  g.game_completed_flag,
  -- Cover vs closing spread (CLV research only)
  CASE
    WHEN g.home_margin IS NOT NULL AND so.closing_spread IS NOT NULL
    THEN CASE WHEN g.home_margin > (-1 * so.closing_spread) THEN 1 ELSE 0 END
  END AS home_covered_close,
  -- CLV: open spread vs closing spread alignment
  CASE
    WHEN g.home_win_flag = 1 AND so.home_fav_opening = 1 THEN 1
    WHEN g.home_win_flag = 0 AND so.home_dog_opening = 1 THEN 1
    ELSE 0
  END AS bet_side_won,

  -- ===== SPLIT VERSIONS =====
  -- football_only_flag: use when training market-agnostic model
  1                                  AS football_only_available,
  CASE WHEN so.opening_spread IS NOT NULL THEN 1 ELSE 0 END AS market_available

FROM int_game_base g

LEFT JOIN mart_team_week_modeling h    ON g.home_team = h.team AND g.season = h.season AND g.week = h.week
LEFT JOIN mart_team_week_modeling a    ON g.away_team = a.team AND g.season = a.season AND g.week = a.week
LEFT JOIN int_team_power_rating hpr    ON g.home_team = hpr.team AND g.game_id = hpr.game_id
LEFT JOIN int_team_power_rating apr    ON g.away_team = apr.team AND g.game_id = apr.game_id
LEFT JOIN int_team_schedule_spot h_ss  ON g.home_team = h_ss.team AND g.game_id = h_ss.game_id
LEFT JOIN int_team_schedule_spot a_ss  ON g.away_team = a_ss.team AND g.game_id = a_ss.game_id
LEFT JOIN int_team_travel_game a_trvl  ON g.away_team = a_trvl.team AND g.game_id = a_trvl.game_id AND a_trvl.is_away_team = 1
LEFT JOIN int_injury_team_impact h_inj ON g.home_team = h_inj.team AND g.season = h_inj.season AND g.week = h_inj.week
LEFT JOIN int_injury_team_impact a_inj ON g.away_team = a_inj.team AND g.season = a_inj.season AND g.week = a_inj.week
LEFT JOIN int_weather_game wg          ON g.game_id = wg.game_id
LEFT JOIN spread_open so               ON g.game_id = so.game_id
LEFT JOIN spread_bucket sb             ON g.game_id = sb.game_id
;


-- =============================================================================
-- MART: mart_total_modeling
-- Grain:   game_id
-- Purpose: Over-under / totals model table.
-- LEAKAGE: Same rules as mart_spread_modeling.
-- =============================================================================

CREATE OR REPLACE VIEW mart_total_modeling AS
WITH total_open AS (
  SELECT game_id, opening_line AS opening_total, opening_implied_prob AS opening_over_prob,
         closing_line AS closing_total, move_open_to_close AS total_move_otc,
         line_volatility_score AS total_line_volatility
  FROM int_market_open_close
  WHERE market_type = 'total' AND selection = 'over'
),
total_24h AS (
  SELECT game_id, bucket_line AS total_24h
  FROM int_market_time_buckets
  WHERE market_type = 'total' AND selection = 'over' AND time_bucket = '24h'
)

SELECT
  g.game_id, g.season, g.week, g.season_type, g.game_date,
  g.home_team, g.away_team,
  g.surface, g.roof, g.location, g.div_game,

  -- ===== TOTAL ENVIRONMENT FEATURES =====
  h.epa_pp_blended + a.epa_pp_blended         AS combined_off_epa,
  h.def_epa_pp_blended + a.def_epa_pp_blended AS combined_def_epa,
  h.pts_for_std + a.pts_for_std               AS combined_pts_for_pg,
  h.pts_against_std + a.pts_against_std       AS combined_pts_against_pg,
  h.pass_rate_std + a.pass_rate_std           AS combined_pass_rate,
  h.explosive_pass_rate_std + a.explosive_pass_rate_std AS combined_explosive_rate,
  h.rz_td_rate_std + a.rz_td_rate_std         AS combined_rz_td_rate,
  h.turnovers_per_game_std + a.turnovers_per_game_std AS combined_turnovers_pg,

  -- Total environment rating (offense strength vs defense strength)
  hpr.total_environment_rating AS home_total_env,
  apr.total_environment_rating AS away_total_env,
  hpr.total_environment_rating + apr.total_environment_rating AS combined_total_env,

  -- ===== PACE =====
  h.pass_rate_std AS home_pass_rate,
  a.pass_rate_std AS away_pass_rate,

  -- ===== WEATHER (total suppression features) =====
  wg.eff_wind_mph, wg.eff_temp_f, wg.eff_precip_in,
  wg.wind_bucket, wg.temp_bucket,
  wg.adverse_weather_score,
  wg.total_suppression_score,
  wg.passing_condition_score,
  wg.roof_closed_flag, wg.open_air_weather_applicable,
  wg.cold_wind_interaction,
  wg.snow_flag, wg.precip_flag, wg.wind_20_flag,
  wg.weather_source_type,

  -- ===== VENUE =====
  st.elevation_m,
  st.altitude_flag,
  st.international_game_flag,

  -- ===== SCHEDULE / INJURY =====
  h_ss.rest_days AS home_rest, a_ss.rest_days AS away_rest,
  h_inj.total_injury_burden AS home_injury_burden,
  a_inj.total_injury_burden AS away_injury_burden,

  -- ===== MARKET — OPENING (pre-game safe) =====
  to_.opening_total,
  to_.opening_over_prob,
  t24.total_24h,

  -- ===== MARKET — CLOSING (NOT safe for pregame) =====
  to_.closing_total,
  to_.total_move_otc,
  to_.total_line_volatility,

  -- Implied team totals (from spread+total at open)
  CASE
    WHEN so.opening_spread IS NOT NULL AND to_.opening_total IS NOT NULL
    THEN to_.opening_total / 2.0 - so.opening_spread / 2.0
  END AS implied_home_team_total_open,
  CASE
    WHEN so.opening_spread IS NOT NULL AND to_.opening_total IS NOT NULL
    THEN to_.opening_total / 2.0 + so.opening_spread / 2.0
  END AS implied_away_team_total_open,

  -- ===== TARGETS =====
  g.home_score, g.away_score, g.total_points, g.game_completed_flag,
  CASE
    WHEN g.total_points IS NOT NULL AND to_.closing_total IS NOT NULL
    THEN CASE WHEN g.total_points > to_.closing_total THEN 1 ELSE 0 END
  END AS over_close_flag,
  CASE
    WHEN g.total_points IS NOT NULL AND to_.opening_total IS NOT NULL
    THEN CASE WHEN g.total_points > to_.opening_total THEN 1 ELSE 0 END
  END AS over_open_flag,
  CASE WHEN to_.opening_total IS NOT NULL THEN 1 ELSE 0 END AS market_available

FROM int_game_base g
LEFT JOIN mart_team_week_modeling h    ON g.home_team = h.team AND g.season = h.season AND g.week = h.week
LEFT JOIN mart_team_week_modeling a    ON g.away_team = a.team AND g.season = a.season AND g.week = a.week
LEFT JOIN int_team_power_rating hpr    ON g.home_team = hpr.team AND g.game_id = hpr.game_id
LEFT JOIN int_team_power_rating apr    ON g.away_team = apr.team AND g.game_id = apr.game_id
LEFT JOIN int_team_schedule_spot h_ss  ON g.home_team = h_ss.team AND g.game_id = h_ss.game_id
LEFT JOIN int_team_schedule_spot a_ss  ON g.away_team = a_ss.team AND g.game_id = a_ss.game_id
LEFT JOIN int_injury_team_impact h_inj ON g.home_team = h_inj.team AND g.season = h_inj.season AND g.week = h_inj.week
LEFT JOIN int_injury_team_impact a_inj ON g.away_team = a_inj.team AND g.season = a_inj.season AND g.week = a_inj.week
LEFT JOIN int_weather_game wg          ON g.game_id = wg.game_id
LEFT JOIN stg_stadium st               ON g.home_team = st.team AND g.season BETWEEN st.season_start AND st.season_end
LEFT JOIN total_open to_               ON g.game_id = to_.game_id
LEFT JOIN total_24h t24                ON g.game_id = t24.game_id
LEFT JOIN (SELECT game_id, opening_line AS opening_spread FROM int_market_open_close
           WHERE market_type = 'spread' AND selection = 'home') so
  ON g.game_id = so.game_id
;


-- =============================================================================
-- MART: mart_market_research_game
-- Grain:   game_id + market_type + selection
-- Purpose: CLV analysis, line movement research, market efficiency studies.
-- WARNING: This mart CONTAINS closing lines. It is NOT for predictive modeling.
--          It is for postgame research and model evaluation ONLY.
-- =============================================================================

CREATE OR REPLACE VIEW mart_market_research_game AS
SELECT
  oc.game_id,
  oc.season,
  oc.week,
  oc.home_team,
  oc.away_team,
  oc.market_type,
  oc.selection,

  -- Line timeline
  oc.opening_line,
  oc.opening_implied_prob,
  tb72.bucket_line      AS line_72h,
  tb24.bucket_line      AS line_24h,
  tb6.bucket_line       AS line_6h,
  tb1.bucket_line       AS line_1h,
  oc.closing_line,
  oc.closing_implied_prob,

  -- Movement summaries
  oc.move_open_to_close,
  oc.abs_move_open_to_close,
  oc.favorite_flip_flag,
  oc.crossed_key_3_flag,
  oc.crossed_key_7_flag,
  oc.crossed_key_10_flag,
  oc.line_volatility_score,
  oc.close_consensus_dispersion,
  oc.close_book_count,

  -- Book count at open
  sc_open.book_count AS open_book_count,

  -- Closing implied no-vig probability (pre-computed for two-way markets)
  -- For spread: assumes standard -110 each side → 52.38% each
  CASE
    WHEN oc.market_type IN ('spread','total')
    THEN 0.5238  -- standard no-vig for -110 both sides
    ELSE oc.closing_implied_prob  -- use raw for ML (would need opponent side to de-vig)
  END AS closing_novig_prob,

  -- Market-implied fair value at open
  CASE
    WHEN oc.market_type IN ('spread','total')
    THEN 0.5238
    ELSE oc.opening_implied_prob
  END AS opening_novig_prob,

  -- Game outcomes (targets for CLV analysis)
  g.home_score,
  g.away_score,
  g.home_margin,
  g.total_points,
  g.home_win_flag,

  -- Did the opening side win?
  CASE
    WHEN oc.market_type = 'spread' AND oc.selection = 'home'
     AND g.home_margin IS NOT NULL AND oc.opening_line IS NOT NULL
    THEN CASE WHEN g.home_margin > (-1 * oc.opening_line) THEN 1 ELSE 0 END
  END AS covered_open_flag,

  -- Did the closing side win? (CLV measurement)
  CASE
    WHEN oc.market_type = 'spread' AND oc.selection = 'home'
     AND g.home_margin IS NOT NULL AND oc.closing_line IS NOT NULL
    THEN CASE WHEN g.home_margin > (-1 * oc.closing_line) THEN 1 ELSE 0 END
  END AS covered_close_flag,

  -- CLV: did you beat the closing line?
  -- Positive CLV = you got a better number than close
  CASE
    WHEN oc.market_type = 'spread' AND oc.selection = 'home'
    THEN oc.opening_line - oc.closing_line  -- positive = open was better for home side
  END AS open_vs_close_clv,

  -- Over/under outcome
  CASE
    WHEN oc.market_type = 'total' AND oc.selection = 'over'
     AND g.total_points IS NOT NULL AND oc.opening_line IS NOT NULL
    THEN CASE WHEN g.total_points > oc.opening_line THEN 1 ELSE 0 END
  END AS over_open_flag,
  CASE
    WHEN oc.market_type = 'total' AND oc.selection = 'over'
     AND g.total_points IS NOT NULL AND oc.closing_line IS NOT NULL
    THEN CASE WHEN g.total_points > oc.closing_line THEN 1 ELSE 0 END
  END AS over_close_flag,

  -- Weather context for research
  wg.eff_wind_mph, wg.eff_temp_f, wg.wind_bucket, wg.temp_bucket,
  wg.adverse_weather_score, wg.roof_closed_flag,

  oc.open_timestamp,
  oc.close_timestamp

FROM int_market_open_close oc
LEFT JOIN int_game_base g ON oc.game_id = g.game_id
LEFT JOIN int_weather_game wg ON oc.game_id = wg.game_id
LEFT JOIN (
  SELECT game_id, market_type, selection, book_count
  FROM stg_market_consensus_game
  WHERE snapshot_type = 'opening'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY game_id, market_type, selection ORDER BY market_timestamp
  ) = 1
) sc_open ON oc.game_id = sc_open.game_id
         AND oc.market_type = sc_open.market_type
         AND oc.selection = sc_open.selection
LEFT JOIN (SELECT game_id, market_type, selection, bucket_line FROM int_market_time_buckets WHERE time_bucket = '72h') tb72
  ON oc.game_id = tb72.game_id AND oc.market_type = tb72.market_type AND oc.selection = tb72.selection
LEFT JOIN (SELECT game_id, market_type, selection, bucket_line FROM int_market_time_buckets WHERE time_bucket = '24h') tb24
  ON oc.game_id = tb24.game_id AND oc.market_type = tb24.market_type AND oc.selection = tb24.selection
LEFT JOIN (SELECT game_id, market_type, selection, bucket_line FROM int_market_time_buckets WHERE time_bucket = '6h') tb6
  ON oc.game_id = tb6.game_id AND oc.market_type = tb6.market_type AND oc.selection = tb6.selection
LEFT JOIN (SELECT game_id, market_type, selection, bucket_line FROM int_market_time_buckets WHERE time_bucket = '1h') tb1
  ON oc.game_id = tb1.game_id AND oc.market_type = tb1.market_type AND oc.selection = tb1.selection
;
