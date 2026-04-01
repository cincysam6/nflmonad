-- =============================================================================
-- STAGING: stg_weather_game_hourly
-- Source:  raw_weather_hourly_history + raw_weather_forecast_hourly
-- Grain:   stadium_id + game_date + observation_or_forecast_ts + weather_type
-- LEAKAGE:
--   weather_type = 'historical' → ONLY safe for postgame retrospective models
--   weather_type = 'forecast'   → Safe for pregame prediction when
--                                 forecast_run_ts < prediction_cutoff_ts
-- =============================================================================

CREATE OR REPLACE VIEW stg_weather_game_hourly AS
SELECT
  source_name,
  stadium_id,
  CAST(latitude          AS DOUBLE)          AS latitude,
  CAST(longitude         AS DOUBLE)          AS longitude,
  weather_type,                              -- 'historical' | 'forecast'
  CAST(observation_or_forecast_ts AS TIMESTAMPTZ) AS weather_ts,
  CAST(forecast_run_ts   AS TIMESTAMPTZ)     AS forecast_run_ts,
  CAST(game_date         AS DATE)            AS game_date,
  CAST(season            AS INTEGER)         AS season,

  -- Temperature (Fahrenheit — Open-Meteo returns in °F when unit=fahrenheit)
  CAST(temperature_2m       AS DOUBLE)       AS temp_f,
  CAST(apparent_temperature AS DOUBLE)       AS apparent_temp_f,

  -- Precipitation (inches — Open-Meteo returns in inches when unit=inch)
  CAST(precipitation        AS DOUBLE)       AS precip_in,
  CAST(rain                 AS DOUBLE)       AS rain_in,
  CAST(snowfall             AS DOUBLE)       AS snow_in,
  CAST(snow_depth           AS DOUBLE)       AS snow_depth_in,

  -- Wind (mph — Open-Meteo returns in mph when unit=mph)
  CAST(wind_speed_10m       AS DOUBLE)       AS wind_mph,
  CAST(wind_gusts_10m       AS DOUBLE)       AS wind_gusts_mph,
  CAST(wind_direction_10m   AS INTEGER)      AS wind_dir_deg,

  -- Humidity / pressure
  CAST(relative_humidity_2m AS DOUBLE)       AS humidity_pct,
  CAST(dew_point_2m         AS DOUBLE)       AS dew_point_f,
  CAST(surface_pressure     AS DOUBLE)       AS pressure_hpa,

  -- WMO weather code → condition category
  CAST(weather_code         AS INTEGER)      AS weather_code,
  CASE
    WHEN CAST(weather_code AS INTEGER) = 0          THEN 'Clear'
    WHEN CAST(weather_code AS INTEGER) BETWEEN 1  AND  3 THEN 'Partly Cloudy'
    WHEN CAST(weather_code AS INTEGER) BETWEEN 45 AND 48 THEN 'Fog'
    WHEN CAST(weather_code AS INTEGER) BETWEEN 51 AND 67 THEN 'Rain'
    WHEN CAST(weather_code AS INTEGER) BETWEEN 71 AND 77 THEN 'Snow'
    WHEN CAST(weather_code AS INTEGER) BETWEEN 80 AND 82 THEN 'Rain Showers'
    WHEN CAST(weather_code AS INTEGER) BETWEEN 85 AND 86 THEN 'Snow Showers'
    WHEN CAST(weather_code AS INTEGER) BETWEEN 95 AND 99 THEN 'Thunderstorm'
    ELSE 'Other'
  END AS weather_condition,

  ingestion_ts

FROM (
  SELECT * FROM raw_weather_hourly_history
  UNION ALL
  SELECT * FROM raw_weather_forecast_hourly
)
;


-- =============================================================================
-- STAGING: stg_weather_game_kickoff
-- Source:  stg_weather_game_hourly + stg_games
-- Grain:   game_id (one row per game)
-- Purpose: Single weather record at kickoff time.
-- LEAKAGE:
--   Use historical rows ONLY for postgame retrospective models.
--   For pregame prediction models, use forecasts where
--   forecast_run_ts < prediction_cutoff_ts.
-- =============================================================================

CREATE OR REPLACE VIEW stg_weather_game_kickoff AS
WITH game_kickoffs AS (
  SELECT
    g.game_id,
    g.season,
    g.week,
    g.game_date,
    g.home_team,
    -- Parse kickoff to UTC-ish datetime
    CAST(g.game_date AS TIMESTAMPTZ) +
      INTERVAL (COALESCE(
        TRY_CAST(SPLIT_PART(g.kickoff_time_local, ':', 1) AS INTEGER), 13
      ) * 60 + COALESCE(
        TRY_CAST(SPLIT_PART(g.kickoff_time_local, ':', 2) AS INTEGER), 0
      )) MINUTES                       AS kickoff_ts_approx,
    g.roof,
    g.surface
  FROM stg_games g
),
-- Map game to stadium
game_with_stadium AS (
  SELECT
    gk.*,
    sm.stadium_id,
    sm.roof_type       AS metadata_roof_type,
    sm.surface_type    AS metadata_surface_type
  FROM game_kickoffs gk
  LEFT JOIN raw_stadium_metadata sm
    ON gk.home_team  = sm.team
   AND gk.season    >= sm.season_start
   AND gk.season    <= sm.season_end
),
-- Select nearest historical weather to kickoff
historical_nearest AS (
  SELECT
    gs.game_id,
    w.*,
    ABS(EXTRACT(EPOCH FROM (w.weather_ts - gs.kickoff_ts_approx)) / 60) AS diff_min,
    ROW_NUMBER() OVER (
      PARTITION BY gs.game_id
      ORDER BY ABS(EXTRACT(EPOCH FROM (w.weather_ts - gs.kickoff_ts_approx)) / 60)
    ) AS rn
  FROM game_with_stadium gs
  JOIN stg_weather_game_hourly w
    ON gs.stadium_id = w.stadium_id
   AND CAST(gs.game_date AS DATE) = w.game_date
   AND w.weather_type = 'historical'
)

SELECT
  gs.game_id,
  gs.season,
  gs.week,
  gs.game_date,
  gs.kickoff_ts_approx,
  gs.stadium_id,
  gs.roof,
  gs.metadata_roof_type,

  -- Historical weather at kickoff
  hn.temp_f,
  hn.apparent_temp_f,
  hn.precip_in,
  hn.rain_in,
  hn.snow_in,
  hn.snow_depth_in,
  hn.wind_mph,
  hn.wind_gusts_mph,
  hn.wind_dir_deg,
  hn.humidity_pct,
  hn.dew_point_f,
  hn.pressure_hpa,
  hn.weather_code,
  hn.weather_condition,
  hn.diff_min              AS weather_obs_minutes_from_kickoff,
  hn.weather_type          AS weather_source_type,
  hn.forecast_run_ts,

  -- Roof-closed override: if dome or known-closed retractable, weather is moot
  CASE
    WHEN LOWER(gs.metadata_roof_type) = 'dome' THEN 1
    WHEN LOWER(gs.metadata_roof_type) = 'retractable'
     AND LOWER(gs.roof) IN ('closed','dome') THEN 1
    ELSE 0
  END AS roof_closed_flag,

  -- Open-air weather applicable (not dome, not confirmed closed)
  CASE
    WHEN LOWER(gs.metadata_roof_type) = 'dome' THEN 0
    WHEN LOWER(gs.metadata_roof_type) = 'retractable'
     AND LOWER(gs.roof) IN ('closed','dome') THEN 0
    ELSE 1
  END AS open_air_weather_applicable

FROM game_with_stadium gs
LEFT JOIN historical_nearest hn
  ON gs.game_id = hn.game_id
  AND hn.rn = 1
;


-- =============================================================================
-- STAGING: stg_stadium
-- Source:  raw_stadium_metadata
-- Grain:   stadium_id + season (season_start to season_end range)
-- =============================================================================

CREATE OR REPLACE VIEW stg_stadium AS
SELECT
  stadium_id,
  stadium_name,
  CASE team
    WHEN 'OAK' THEN 'LV'
    WHEN 'SD'  THEN 'LAC'
    WHEN 'STL' THEN 'LA'
    ELSE team
  END AS team,
  CAST(season_start AS INTEGER) AS season_start,
  CAST(season_end   AS INTEGER) AS season_end,
  CAST(latitude     AS DOUBLE)  AS latitude,
  CAST(longitude    AS DOUBLE)  AS longitude,
  CAST(elevation_m  AS DOUBLE)  AS elevation_m,
  LOWER(roof_type)              AS roof_type,     -- open | dome | retractable
  LOWER(surface_type)           AS surface_type,  -- grass | turf
  timezone,
  CAST(capacity     AS INTEGER) AS capacity,
  CAST(is_neutral_site_default  AS INTEGER) AS is_neutral_site,
  CAST(international_game_flag  AS INTEGER) AS international_game_flag,
  CAST(altitude_flag AS INTEGER)            AS altitude_flag,
  CASE
    WHEN LOWER(roof_type) = 'dome'         THEN 'indoor'
    WHEN LOWER(roof_type) = 'retractable'  THEN 'retractable'
    ELSE 'outdoor'
  END AS venue_category,
  ingestion_ts

FROM raw_stadium_metadata
;


-- =============================================================================
-- STAGING: stg_travel_context
-- Source:  computed by R (compute_travel_matrix) → raw layer
-- Grain:   season + home_team + away_team
-- =============================================================================

CREATE OR REPLACE VIEW stg_travel_context AS
SELECT
  CAST(season      AS INTEGER)   AS season,
  home_team,
  away_team,
  CAST(travel_km   AS DOUBLE)    AS travel_km,
  ROUND(CAST(travel_km AS DOUBLE) / 1.60934, 1) AS travel_miles,
  CAST(timezone_shift_hours AS DOUBLE) AS timezone_shift_hours,
  CAST(east_to_west_flag    AS INTEGER) AS east_to_west_flag,
  CAST(west_to_east_flag    AS INTEGER) AS west_to_east_flag,
  CAST(international_travel_flag AS INTEGER) AS international_travel_flag,
  ingestion_ts

FROM raw_team_travel_reference
;


-- =============================================================================
-- STAGING: stg_coaching_context
-- Source:  raw_coach_reference
-- Grain:   team + season + coach_role
-- =============================================================================

CREATE OR REPLACE VIEW stg_coaching_context AS
SELECT
  CASE team
    WHEN 'OAK' THEN 'LV'
    WHEN 'SD'  THEN 'LAC'
    WHEN 'STL' THEN 'LA'
    ELSE team
  END AS team,
  CAST(season     AS INTEGER)   AS season,
  coach_name,
  UPPER(coach_role)             AS coach_role,  -- HC | OC | DC
  CAST(first_season_with_team AS INTEGER) AS first_season_with_team,
  CAST(is_rookie_hc           AS INTEGER) AS is_rookie_hc,
  CAST(coordinator_change_flag AS INTEGER) AS coordinator_change_flag,
  ingestion_ts

FROM raw_coach_reference
;
