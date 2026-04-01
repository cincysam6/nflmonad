-- =============================================================================
-- MART: mart_backtest_game (ENHANCED)
-- Grain:   game_id + prediction_horizon
-- Purpose: Leakage-safe historical training set with explicit horizon tracking.
--
-- Three rows per game (one per prediction_horizon):
--   early_week  → features as of 120h+ before kickoff
--   mid_week    → features as of 72h before kickoff
--   pre_kickoff → features as of 1h before kickoff
--
-- LEAKAGE RULES by horizon:
--   early_week:  opening spread/total, initial injury report, extended forecast
--   mid_week:    72h-bucket spread/total, Thursday injury report, 3-day forecast
--   pre_kickoff: 1h-bucket spread/total, final injury status, latest forecast
--
-- All closing market features are always NULL in this table by design.
-- =============================================================================

CREATE OR REPLACE VIEW mart_backtest_game AS
WITH horizons AS (
  SELECT 'early_week'  AS prediction_horizon, 120 AS hours_before_kickoff UNION ALL
  SELECT 'mid_week',                           72                          UNION ALL
  SELECT 'pre_kickoff',                         1
),

-- Spread at each horizon
spread_by_horizon AS (
  SELECT h.prediction_horizon,
         tb.game_id,
         tb.bucket_line  AS spread_at_horizon,
         tb.bucket_implied_prob AS win_prob_at_horizon
  FROM int_market_time_buckets tb
  JOIN horizons h
    ON (tb.time_bucket = 'open' AND h.prediction_horizon = 'early_week')
    OR (tb.time_bucket = '72h'  AND h.prediction_horizon = 'mid_week')
    OR (tb.time_bucket = '1h'   AND h.prediction_horizon = 'pre_kickoff')
  WHERE tb.market_type = 'spread' AND tb.selection = 'home'
),
total_by_horizon AS (
  SELECT h.prediction_horizon, tb.game_id, tb.bucket_line AS total_at_horizon
  FROM int_market_time_buckets tb
  JOIN horizons h
    ON (tb.time_bucket = 'open' AND h.prediction_horizon = 'early_week')
    OR (tb.time_bucket = '72h'  AND h.prediction_horizon = 'mid_week')
    OR (tb.time_bucket = '1h'   AND h.prediction_horizon = 'pre_kickoff')
  WHERE tb.market_type = 'total' AND tb.selection = 'over'
),

-- Injury burden is effectively the same at all horizons for now
-- (in a real system you'd snapshot injuries at the prediction_cutoff_ts)
-- Placeholder — enhanceable with timestamped injury data
inj_h AS (SELECT * FROM int_injury_team_impact),
inj_a AS (SELECT * FROM int_injury_team_impact)

SELECT
  h.prediction_horizon,
  h.hours_before_kickoff,
  g.game_id,
  g.season,
  g.week,
  g.season_type,
  g.game_date,
  g.home_team,
  g.away_team,
  g.game_completed_flag,

  -- Football features (same at all horizons — lagged rolling)
  g.home_epa_blended, g.home_epa_l5, g.home_epa_l3,
  g.away_epa_blended, g.away_epa_l5, g.away_epa_l3,
  g.epa_diff, g.composite_diff,
  g.home_elo, g.away_elo, g.elo_diff,
  g.home_off_power, g.away_off_power, g.home_def_power, g.away_def_power,
  g.home_pass_epa, g.away_pass_epa, g.home_def_epa, g.away_def_epa,
  g.home_success_rate, g.away_success_rate,
  g.home_pass_rate, g.away_pass_rate,
  g.home_rz_td_rate, g.away_rz_td_rate,
  g.home_games_played, g.away_games_played,

  -- Rest/scheduling (pre-game safe at all horizons)
  g.home_rest_days, g.away_rest_days, g.rest_days_advantage_home,
  g.home_short_week, g.away_short_week, g.home_bye_prior, g.away_bye_prior,
  g.home_long_rest, g.away_long_rest,
  g.home_third_road_four, g.away_third_road_four,

  -- Travel (pre-game safe)
  g.travel_km, g.away_tz_shift, g.away_west_east_early, g.away_intl_travel,

  -- Injury (as-of horizon — currently week-level approximation)
  g.home_qb_out, g.away_qb_out,
  g.home_injury_burden, g.away_injury_burden,
  g.home_wr_burden, g.away_wr_burden,

  -- Venue
  g.venue_category, g.elevation_m, g.altitude_flag, g.international_game_flag,
  g.roof_closed_flag, g.div_game, g.conf_game, g.primetime_flag,

  -- Weather (horizon-specific: use forecast for early/mid, nearest obs for pre-kick)
  -- NOTE: In retrospective backtests on historical games, this will be 'historical'
  -- weather (realized). For forward-looking use, replace with forecast.
  g.eff_temp_f, g.eff_wind_mph, g.eff_precip_in,
  g.wind_bucket, g.temp_bucket, g.adverse_weather_score,
  g.passing_condition_score, g.total_suppression_score,
  g.cold_wind_interaction, g.snow_flag, g.precip_flag,
  g.weather_source_type,
  g.forecast_run_ts,

  -- Market features (horizon-appropriate — no closing lines allowed here)
  sbh.spread_at_horizon,
  sbh.win_prob_at_horizon,
  tbh.total_at_horizon,
  CASE WHEN sbh.spread_at_horizon IS NOT NULL AND tbh.total_at_horizon IS NOT NULL
    THEN tbh.total_at_horizon / 2.0 - sbh.spread_at_horizon / 2.0 END AS implied_home_total_at_horizon,

  -- Market availability flag (null-safe for football-only backtests)
  CASE WHEN sbh.spread_at_horizon IS NOT NULL THEN 1 ELSE 0 END AS market_available_at_horizon,

  -- CV split (by season year — never random-split across seasons)
  CASE
    WHEN g.season < 2015 THEN 'train'
    WHEN g.season BETWEEN 2015 AND 2019 THEN 'validation'
    ELSE 'test'
  END AS cv_split,

  -- ===== TARGETS =====
  g.home_score, g.away_score, g.total_points, g.home_margin, g.home_win_flag,
  -- Cover vs closing spread (CLV benchmark only — using closing as target, not feature)
  g.home_covered_close,
  g.over_close_flag

FROM mart_game_modeling g
CROSS JOIN horizons h
LEFT JOIN spread_by_horizon sbh ON g.game_id = sbh.game_id AND h.prediction_horizon = sbh.prediction_horizon
LEFT JOIN total_by_horizon  tbh ON g.game_id = tbh.game_id AND h.prediction_horizon = tbh.prediction_horizon

WHERE g.game_completed_flag = 1
  AND g.season_type = 'regular'
  AND g.home_games_played >= 1
  AND g.away_games_played >= 1
;


-- =============================================================================
-- MART: mart_backtest_player (ENHANCED)
-- Grain:   season + week + player_id + prediction_horizon
-- Same leakage-safe horizon design as mart_backtest_game.
-- =============================================================================

CREATE OR REPLACE VIEW mart_backtest_player AS
WITH horizons AS (
  SELECT 'early_week'  AS prediction_horizon UNION ALL
  SELECT 'mid_week'                           UNION ALL
  SELECT 'pre_kickoff'
)

SELECT
  h.prediction_horizon,
  p.player_id, p.full_name, p.position, p.position_group,
  p.team, p.opponent, p.season, p.week, p.game_id, p.home_flag,

  -- Usage features (lagged — safe at all horizons)
  p.targets_per_game_std, p.rec_per_game_std, p.rec_yds_per_game_std,
  p.target_share_std, p.air_yards_share_std, p.wopr_std, p.snap_pct_std,
  p.carries_per_game_std, p.rush_yds_per_game_std, p.rush_tds_per_game_std,
  p.attempts_per_game_std, p.pass_yds_per_game_std, p.pass_tds_per_game_std,
  p.games_played_std,
  p.targets_per_game_l4, p.target_share_l4, p.carries_per_game_l4,
  p.targets_per_game_l2, p.carries_per_game_l2,

  -- Team context (pre-game safe)
  p.team_epa_blended, p.team_pass_rate_std, p.team_pts_for_std,

  -- Opponent context (pre-game safe)
  p.opp_def_epa_blended, p.opp_def_pass_epa_std, p.opp_def_rush_epa_std,
  p.opp_cb_burden, p.opp_dl_burden,

  -- Injury (as-of final status; refine if timestamped injury data available)
  p.injury_status, p.injury_out_flag,

  -- Weather (team's game weather — see leakage contract)
  wg.eff_temp_f, wg.eff_wind_mph, wg.eff_precip_in,
  wg.wind_bucket, wg.adverse_weather_score,
  wg.passing_condition_score, wg.total_suppression_score,
  wg.roof_closed_flag, wg.weather_source_type,

  -- Team total from market (horizon-safe opening implied total only)
  CASE
    WHEN p.home_flag = 1 THEN mg.opening_home_team_total
    ELSE mg.opening_away_team_total
  END AS team_implied_total_opening,

  -- CV split
  CASE
    WHEN p.season < 2015 THEN 'train'
    WHEN p.season BETWEEN 2015 AND 2019 THEN 'validation'
    ELSE 'test'
  END AS cv_split,

  -- Targets
  p.actual_targets, p.actual_receptions, p.actual_rec_yards, p.actual_rec_tds,
  p.actual_carries, p.actual_rush_yards, p.actual_rush_tds,
  p.actual_pass_attempts, p.actual_pass_yards, p.actual_pass_tds,
  p.actual_fpts_std, p.actual_fpts_ppr

FROM mart_player_week_projection p
CROSS JOIN horizons h
LEFT JOIN int_weather_game wg ON p.game_id = wg.game_id
LEFT JOIN mart_game_modeling mg ON p.game_id = mg.game_id

WHERE (p.actual_targets IS NOT NULL OR p.actual_carries IS NOT NULL
       OR p.actual_pass_attempts IS NOT NULL)  -- game has been played
;
