-- =============================================================================
-- INTERMEDIATE: int_weather_game
-- Grain:   game_id
-- Purpose: Engineered weather features at kickoff, with roof-aware logic.
-- LEAKAGE:
--   weather_source_type = 'historical' → postgame retrospective only
--   weather_source_type = 'forecast'   → pregame safe when forecast_run_ts
--                                         is before prediction cutoff
-- =============================================================================

CREATE OR REPLACE VIEW int_weather_game AS
WITH cfg AS (
  -- Hard-code thresholds (mirror config_external.yml)
  SELECT
    10  AS wind_mild_mph,
    15  AS wind_moderate_mph,
    20  AS wind_strong_mph,
    25  AS wind_severe_mph,
    32  AS cold_temp_f,
    20  AS very_cold_temp_f,
    85  AS hot_temp_f,
    1.0 AS precip_flag_in,
    0.5 AS snow_flag_in,
    80  AS humidity_high_pct
)

SELECT
  w.game_id,
  w.season,
  w.week,
  w.stadium_id,
  w.roof_closed_flag,
  w.open_air_weather_applicable,
  w.weather_source_type,
  w.forecast_run_ts,

  -- ===== RAW WEATHER AT KICKOFF =====
  w.temp_f,
  w.apparent_temp_f,
  w.wind_mph,
  w.wind_gusts_mph,
  w.wind_dir_deg,
  w.precip_in,
  w.rain_in,
  w.snow_in,
  w.snow_depth_in,
  w.humidity_pct,
  w.dew_point_f,
  w.pressure_hpa,
  w.weather_condition,

  -- ===== ROOF-ADJUSTED EFFECTIVE WEATHER =====
  -- If roof is closed, weather has no effect; set effective values to neutral
  CASE WHEN w.roof_closed_flag = 1 THEN 72.0   ELSE w.temp_f     END AS eff_temp_f,
  CASE WHEN w.roof_closed_flag = 1 THEN 0.0    ELSE w.wind_mph   END AS eff_wind_mph,
  CASE WHEN w.roof_closed_flag = 1 THEN 0.0    ELSE w.precip_in  END AS eff_precip_in,
  CASE WHEN w.roof_closed_flag = 1 THEN 0.0    ELSE w.snow_in    END AS eff_snow_in,

  -- ===== CATEGORICAL FLAGS =====
  CAST(CASE WHEN w.roof_closed_flag = 0 AND w.temp_f   < cfg.cold_temp_f       THEN 1 ELSE 0 END AS INTEGER) AS cold_flag,
  CAST(CASE WHEN w.roof_closed_flag = 0 AND w.temp_f   < cfg.very_cold_temp_f  THEN 1 ELSE 0 END AS INTEGER) AS very_cold_flag,
  CAST(CASE WHEN w.roof_closed_flag = 0 AND w.temp_f   > cfg.hot_temp_f        THEN 1 ELSE 0 END AS INTEGER) AS hot_flag,
  CAST(CASE WHEN w.roof_closed_flag = 0 AND w.wind_mph >= cfg.wind_mild_mph     THEN 1 ELSE 0 END AS INTEGER) AS wind_10_flag,
  CAST(CASE WHEN w.roof_closed_flag = 0 AND w.wind_mph >= cfg.wind_moderate_mph THEN 1 ELSE 0 END AS INTEGER) AS wind_15_flag,
  CAST(CASE WHEN w.roof_closed_flag = 0 AND w.wind_mph >= cfg.wind_strong_mph   THEN 1 ELSE 0 END AS INTEGER) AS wind_20_flag,
  CAST(CASE WHEN w.roof_closed_flag = 0 AND w.wind_mph >= cfg.wind_severe_mph   THEN 1 ELSE 0 END AS INTEGER) AS wind_25_flag,
  CAST(CASE WHEN w.roof_closed_flag = 0 AND w.precip_in > cfg.precip_flag_in    THEN 1 ELSE 0 END AS INTEGER) AS precip_flag,
  CAST(CASE WHEN w.roof_closed_flag = 0 AND w.snow_in   > cfg.snow_flag_in      THEN 1 ELSE 0 END AS INTEGER) AS snow_flag,
  CAST(CASE WHEN w.roof_closed_flag = 0 AND w.humidity_pct > cfg.humidity_high_pct THEN 1 ELSE 0 END AS INTEGER) AS high_humidity_flag,

  -- ===== BUCKET ENCODINGS =====
  CASE
    WHEN w.roof_closed_flag = 1   THEN 'dome'
    WHEN w.wind_mph >= 25          THEN 'severe_wind'
    WHEN w.wind_mph >= 20          THEN 'strong_wind'
    WHEN w.wind_mph >= 15          THEN 'moderate_wind'
    WHEN w.wind_mph >= 10          THEN 'mild_wind'
    ELSE 'calm'
  END AS wind_bucket,

  CASE
    WHEN w.roof_closed_flag = 1   THEN 'dome'
    WHEN w.temp_f < 20             THEN 'extreme_cold'
    WHEN w.temp_f < 32             THEN 'freezing'
    WHEN w.temp_f < 45             THEN 'cold'
    WHEN w.temp_f < 55             THEN 'cool'
    WHEN w.temp_f < 70             THEN 'mild'
    WHEN w.temp_f < 85             THEN 'warm'
    ELSE 'hot'
  END AS temp_bucket,

  -- ===== COMPOSITE SCORES =====
  -- Adverse weather score (0-100 scale; higher = worse conditions)
  -- Only applies to open-air games
  CASE WHEN w.roof_closed_flag = 1 THEN 0
    ELSE LEAST(100,
      -- Wind component (0-40 pts)
      LEAST(40, GREATEST(0, (w.wind_mph - 5) * 2.5)) +
      -- Cold component (0-25 pts)
      LEAST(25, GREATEST(0, (32.0 - w.temp_f) * 1.0)) +
      -- Precipitation component (0-25 pts)
      LEAST(25, GREATEST(0, w.precip_in * 20)) +
      -- Snow component (0-10 pts)
      LEAST(10, GREATEST(0, w.snow_in * 20))
    )
  END AS adverse_weather_score,

  -- Passing condition score (higher = better for passing)
  -- Inverse of adverse score with wind weighted more
  CASE WHEN w.roof_closed_flag = 1 THEN 100
    ELSE GREATEST(0, 100 -
      LEAST(60, GREATEST(0, (w.wind_mph - 5) * 3.5)) -
      LEAST(20, GREATEST(0, (32.0 - w.temp_f) * 0.8)) -
      LEAST(20, GREATEST(0, w.precip_in * 15))
    )
  END AS passing_condition_score,

  -- Total suppression proxy (combines wind + cold + precip)
  CASE WHEN w.roof_closed_flag = 1 THEN 0
    ELSE
      (CASE WHEN w.wind_mph >= 20 THEN 1 ELSE 0 END) * 2 +
      (CASE WHEN w.temp_f  <  32 THEN 1 ELSE 0 END)     +
      (CASE WHEN w.precip_in > 1 THEN 1 ELSE 0 END)
  END AS total_suppression_score,

  -- Cold × wind interaction
  CASE WHEN w.roof_closed_flag = 0
        AND w.temp_f < 32
        AND w.wind_mph >= 10
       THEN 1 ELSE 0 END AS cold_wind_interaction

FROM stg_weather_game_kickoff w
CROSS JOIN cfg
;


-- =============================================================================
-- INTERMEDIATE: int_team_travel_game
-- Grain:   game_id + team (away team traveling; home team = 0 distance)
-- Purpose: Travel distance, timezone shift, body clock features.
-- LEAKAGE: All features are pre-game (schedule-derived). Safe for pregame use.
-- =============================================================================

CREATE OR REPLACE VIEW int_team_travel_game AS
WITH game_team AS (
  SELECT
    game_id, season, week, home_team, away_team
  FROM stg_games
  WHERE season_type IN ('regular','postseason')
)

SELECT
  gt.game_id,
  gt.season,
  gt.week,
  trvl.away_team                        AS team,
  trvl.home_team                        AS game_location_team,
  1                                     AS is_away_team,    -- always away here
  trvl.travel_km,
  trvl.travel_miles,
  trvl.timezone_shift_hours,
  trvl.east_to_west_flag,
  trvl.west_to_east_flag,
  trvl.international_travel_flag,

  -- Body clock bucket: convert game local time to away team's body clock time
  -- (kickoff_local_hour + timezone_shift)
  -- Example: 4pm EST game, team from LA (PST, +3hrs east): body clock = 1pm
  tz.home_timezone                      AS away_team_home_tz,
  ss.timezone                           AS game_venue_tz,

  -- Travel distance buckets
  CASE
    WHEN trvl.travel_km < 500    THEN 'short'
    WHEN trvl.travel_km < 1500   THEN 'medium'
    WHEN trvl.travel_km < 3000   THEN 'long'
    ELSE 'cross_country'
  END AS travel_distance_bucket,

  -- West-to-East early kickoff penalty
  CASE
    WHEN trvl.west_to_east_flag = 1
     AND trvl.timezone_shift_hours >= 2 THEN 1
    ELSE 0
  END AS west_east_early_flag,

  -- International travel flag already set in source
  trvl.international_travel_flag        AS intl_travel_flag

FROM game_team gt
LEFT JOIN stg_travel_context trvl
  ON gt.season    = trvl.season
 AND gt.home_team = trvl.home_team
 AND gt.away_team = trvl.away_team
LEFT JOIN (
  SELECT team, season, home_timezone
  FROM raw_team_timezone_reference
) tz ON gt.away_team = tz.team AND gt.season = tz.season
LEFT JOIN stg_stadium ss
  ON gt.home_team = ss.team
 AND gt.season   >= ss.season_start
 AND gt.season   <= ss.season_end

UNION ALL

-- Home team row (0 travel)
SELECT
  gt.game_id,
  gt.season,
  gt.week,
  gt.home_team                          AS team,
  gt.home_team                          AS game_location_team,
  0                                     AS is_away_team,
  0.0                                   AS travel_km,
  0.0                                   AS travel_miles,
  0.0                                   AS timezone_shift_hours,
  0                                     AS east_to_west_flag,
  0                                     AS west_to_east_flag,
  0                                     AS international_travel_flag,
  tz.home_timezone                      AS away_team_home_tz,
  ss.timezone                           AS game_venue_tz,
  'home'                                AS travel_distance_bucket,
  0                                     AS west_east_early_flag,
  0                                     AS intl_travel_flag

FROM game_team gt
LEFT JOIN (SELECT team, season, home_timezone FROM raw_team_timezone_reference) tz
  ON gt.home_team = tz.team AND gt.season = tz.season
LEFT JOIN stg_stadium ss
  ON gt.home_team = ss.team
 AND gt.season   >= ss.season_start
 AND gt.season   <= ss.season_end
;


-- =============================================================================
-- INTERMEDIATE: int_team_schedule_spot
-- Grain:   game_id + team
-- Purpose: Rest/scheduling context. All pre-game safe except days_until_next.
-- LEAKAGE:
--   days_until_next_game → NEVER use in predictive models (future leakage)
--   All other columns are pre-game safe.
-- =============================================================================

CREATE OR REPLACE VIEW int_team_schedule_spot AS
WITH team_games_ordered AS (
  SELECT
    tw.game_id,
    tw.team,
    tw.season,
    tw.week,
    tw.home_flag,
    tw.game_date,
    tw.win_flag,
    LAG(tw.game_date) OVER (PARTITION BY tw.team, tw.season ORDER BY tw.week) AS prev_game_date,
    LEAD(tw.game_date) OVER (PARTITION BY tw.team, tw.season ORDER BY tw.week) AS next_game_date,
    LAG(tw.home_flag) OVER (PARTITION BY tw.team, tw.season ORDER BY tw.week) AS prev_home_flag,
    -- Consecutive road/home game streaks
    SUM(1 - tw.home_flag) OVER (
      PARTITION BY tw.team, tw.season ORDER BY tw.week
      ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) AS road_games_last_4,
    SUM(tw.home_flag) OVER (
      PARTITION BY tw.team, tw.season ORDER BY tw.week
      ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) AS home_games_last_4
  FROM stg_team_week tw
  WHERE tw.season_type = 'regular'
)

SELECT
  game_id,
  team,
  season,
  week,
  home_flag,
  game_date,

  -- REST DAYS (pre-game safe)
  CASE
    WHEN prev_game_date IS NULL THEN NULL  -- first game; coming off bye or preseason
    ELSE DATEDIFF('day', prev_game_date, game_date)
  END AS rest_days,

  CASE WHEN prev_game_date IS NULL THEN 1 ELSE 0 END AS season_opener_flag,

  CASE
    WHEN prev_game_date IS NOT NULL
     AND DATEDIFF('day', prev_game_date, game_date) <= 6 THEN 1
    ELSE 0
  END AS short_week_flag,

  CASE
    WHEN prev_game_date IS NOT NULL
     AND DATEDIFF('day', prev_game_date, game_date) >= 10 THEN 1
    ELSE 0
  END AS long_rest_flag,   -- includes bye week effect

  CASE WHEN prev_game_date IS NULL THEN 1 ELSE 0 END AS bye_week_prior_flag,

  -- ROAD/HOME STREAKS (pre-game safe — based only on prior games)
  GREATEST(0, road_games_last_4 - 1)   AS consecutive_road_games_prior,
  GREATEST(0, home_games_last_4 - 1)   AS consecutive_home_games_prior,

  -- Third road game in four weeks (travel fatigue proxy)
  CASE
    WHEN home_flag = 0 AND road_games_last_4 >= 3 THEN 1
    ELSE 0
  END AS third_road_in_four_flag,

  -- Road favorite / home dog (join to market in mart layer)
  CASE WHEN home_flag = 0 THEN 1 ELSE 0 END AS road_game_flag,

  -- DESCRIPTIVE ONLY — future game lookups (NEVER use as predictive features)
  DATEDIFF('day', game_date, next_game_date) AS days_until_next_game_DESCRIPTIVE_ONLY

FROM team_games_ordered
;
