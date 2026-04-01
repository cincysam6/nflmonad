-- =============================================================================
-- EXTENDED DIMENSION AND FACT TABLE DDL
-- Adds: dim_stadium, dim_sportsbook, dim_market_type, dim_timezone_reference
--       fact_market_game_snapshot, fact_market_game_close,
--       fact_weather_game_hourly, fact_weather_game_forecast,
--       fact_team_travel_game, fact_team_schedule_spot, fact_power_rating_week
-- =============================================================================

-- ---- dim_stadium ------------------------------------------------------------
-- Grain:   stadium_id + season_start
-- Update:  Manually — when teams relocate or new venues open
-- Pregame: Yes (static schedule-derived context)

CREATE TABLE IF NOT EXISTS dim_stadium (
  stadium_id             VARCHAR,
  stadium_name           VARCHAR,
  team                   VARCHAR,
  season_start           INTEGER,
  season_end             INTEGER,
  latitude               DOUBLE,
  longitude              DOUBLE,
  elevation_m            DOUBLE,
  roof_type              VARCHAR,   -- open | dome | retractable
  surface_type           VARCHAR,   -- grass | turf
  timezone               VARCHAR,   -- IANA timezone string
  capacity               INTEGER,
  venue_category         VARCHAR,   -- outdoor | indoor | retractable
  is_neutral_site        INTEGER,
  international_game_flag INTEGER,
  altitude_flag          INTEGER,
  PRIMARY KEY (stadium_id, season_start)
);

-- ---- dim_sportsbook ---------------------------------------------------------
-- Grain:   sportsbook (canonical key)
-- Update:  Manual

CREATE TABLE IF NOT EXISTS dim_sportsbook (
  sportsbook_key         VARCHAR PRIMARY KEY,
  sportsbook_name        VARCHAR,
  is_sharp               INTEGER,   -- 1 if considered sharp/market-moving book
  is_consensus           INTEGER,   -- 1 if represents aggregate/consensus
  is_us_legal            INTEGER,
  country                VARCHAR,
  notes                  VARCHAR
);

INSERT OR REPLACE INTO dim_sportsbook VALUES
  ('pinnacle',          'Pinnacle Sports',        1, 0, 0, 'Curacao', 'Sharp book, highest limits'),
  ('draftkings',        'DraftKings',              0, 0, 1, 'US',      ''),
  ('fanduel',           'FanDuel',                 0, 0, 1, 'US',      ''),
  ('betmgm',            'BetMGM',                  0, 0, 1, 'US',      ''),
  ('caesars',           'Caesars Sportsbook',       0, 0, 1, 'US',      ''),
  ('bet365',            'Bet365',                   0, 0, 0, 'UK',      ''),
  ('espn_consensus',    'ESPN/nflverse Consensus',  0, 1, 1, 'US',      'nflverse source'),
  ('pinnacle_consensus','SBR/Pinnacle Consensus',   1, 1, 0, 'Multi',   'SBR CSV source'),
  ('consensus',         'Market Consensus',         0, 1, 1, 'Multi',   'Aggregated')
;

-- ---- dim_market_type --------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_market_type (
  market_type_key        VARCHAR PRIMARY KEY,
  market_description     VARCHAR,
  market_category        VARCHAR,   -- game_line | player_prop | derivative
  two_sided              INTEGER    -- 1 if standard two-outcome vig market
);

INSERT OR REPLACE INTO dim_market_type VALUES
  ('spread',      'Point Spread',           'game_line',    1),
  ('total',       'Game Total (O/U)',        'game_line',    1),
  ('moneyline',   'Moneyline (H2H)',         'game_line',    1),
  ('team_total',  'Team Total',              'derivative',   1),
  ('player_prop', 'Player Proposition',      'player_prop',  0)
;

-- ---- dim_timezone_reference -------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_timezone_reference (
  team                   VARCHAR,
  season                 INTEGER,
  stadium_id             VARCHAR,
  home_timezone          VARCHAR,   -- IANA string
  latitude               DOUBLE,
  longitude              DOUBLE,
  PRIMARY KEY (team, season)
);

-- ---- dim_coach (optional/manual) -------------------------------------------
CREATE TABLE IF NOT EXISTS dim_coach (
  team                   VARCHAR,
  season                 INTEGER,
  coach_name             VARCHAR,
  coach_role             VARCHAR,   -- HC | OC | DC
  first_season_with_team INTEGER,
  is_rookie_hc           INTEGER,
  coordinator_change_flag INTEGER,
  PRIMARY KEY (team, season, coach_role)
);


-- =============================================================================
-- FACT TABLES (EXTENDED)
-- =============================================================================

-- ---- fact_market_game_snapshot ----------------------------------------------
-- Grain:   source_name + sportsbook + game_id + market_timestamp + market_type + selection
-- Update:  Live season: every 60 min; Historical: batch
-- Pregame: Opening and intraday snapshots are pre-game safe up to their timestamp.
--          Closing snapshots are NOT pre-game safe for predictive models.

CREATE TABLE IF NOT EXISTS fact_market_game_snapshot (
  source_name            VARCHAR,
  sportsbook             VARCHAR,
  game_id                VARCHAR,
  market_timestamp       TIMESTAMPTZ,
  market_type            VARCHAR,
  selection              VARCHAR,
  season                 INTEGER,
  week                   INTEGER,
  line                   DOUBLE,
  price_american         INTEGER,
  price_decimal          DOUBLE,
  implied_prob_raw       DOUBLE,
  snapshot_type          VARCHAR,   -- opening | intraday | closing
  ingestion_ts           TIMESTAMPTZ,
  PRIMARY KEY (source_name, sportsbook, game_id, market_timestamp, market_type, selection)
);

-- ---- fact_market_game_close -------------------------------------------------
-- Grain:   game_id + market_type + selection
-- Update:  Post-game
-- Pregame: NEVER — closing line only; used for CLV research

CREATE TABLE IF NOT EXISTS fact_market_game_close (
  game_id                VARCHAR,
  market_type            VARCHAR,
  selection              VARCHAR,
  closing_line           DOUBLE,
  closing_price_american INTEGER,
  close_timestamp        TIMESTAMPTZ,
  source_name            VARCHAR,
  sportsbook             VARCHAR,
  ingestion_ts           TIMESTAMPTZ,
  PRIMARY KEY (game_id, market_type, selection)
);

-- ---- fact_weather_game_hourly -----------------------------------------------
-- Grain:   stadium_id + game_date + weather_ts + weather_type
-- Update:  Historical: filled 2 days after game; Forecast: refreshed 2x/day
-- Pregame: forecast rows (weather_type='forecast') are pre-game safe;
--          historical rows are NOT pre-game safe

CREATE TABLE IF NOT EXISTS fact_weather_game_hourly (
  source_name            VARCHAR,
  stadium_id             VARCHAR,
  latitude               DOUBLE,
  longitude              DOUBLE,
  weather_type           VARCHAR,   -- historical | forecast
  weather_ts             TIMESTAMPTZ,
  forecast_run_ts        TIMESTAMPTZ,  -- NULL for historical
  game_date              DATE,
  season                 INTEGER,
  temp_f                 DOUBLE,
  apparent_temp_f        DOUBLE,
  precip_in              DOUBLE,
  rain_in                DOUBLE,
  snow_in                DOUBLE,
  wind_mph               DOUBLE,
  wind_gusts_mph         DOUBLE,
  wind_dir_deg           INTEGER,
  humidity_pct           DOUBLE,
  dew_point_f            DOUBLE,
  pressure_hpa           DOUBLE,
  weather_code           INTEGER,
  weather_condition      VARCHAR,
  ingestion_ts           TIMESTAMPTZ,
  PRIMARY KEY (stadium_id, weather_ts, weather_type)
);

-- ---- fact_weather_game_forecast ----------------------------------------------
-- Grain:   stadium_id + forecast_run_ts + forecast_target_ts
-- Separate table so historical and forecast never mingle accidentally

CREATE TABLE IF NOT EXISTS fact_weather_game_forecast (
  source_name            VARCHAR,
  stadium_id             VARCHAR,
  latitude               DOUBLE,
  longitude              DOUBLE,
  forecast_run_ts        TIMESTAMPTZ,
  forecast_target_ts     TIMESTAMPTZ,
  game_date              DATE,
  season                 INTEGER,
  temp_f                 DOUBLE,
  apparent_temp_f        DOUBLE,
  precip_in              DOUBLE,
  snow_in                DOUBLE,
  wind_mph               DOUBLE,
  wind_gusts_mph         DOUBLE,
  wind_dir_deg           INTEGER,
  humidity_pct           DOUBLE,
  weather_code           INTEGER,
  weather_condition      VARCHAR,
  ingestion_ts           TIMESTAMPTZ,
  PRIMARY KEY (stadium_id, forecast_run_ts, forecast_target_ts)
);

-- ---- fact_team_travel_game --------------------------------------------------
-- Grain:   season + week + team + game_id
-- Update:  Batch per season (schedule is known pre-season)
-- Pregame: Fully pre-game safe (derived from schedule + stadium coordinates)

CREATE TABLE IF NOT EXISTS fact_team_travel_game (
  game_id                VARCHAR,
  team                   VARCHAR,
  season                 INTEGER,
  week                   INTEGER,
  is_away_team           INTEGER,
  travel_km              DOUBLE,
  travel_miles           DOUBLE,
  timezone_shift_hours   DOUBLE,
  east_to_west_flag      INTEGER,
  west_to_east_flag      INTEGER,
  travel_distance_bucket VARCHAR,
  international_travel_flag INTEGER,
  PRIMARY KEY (game_id, team)
);

-- ---- fact_team_schedule_spot ------------------------------------------------
-- Grain:   game_id + team
-- Update:  Weekly during season
-- Pregame: Safe except days_until_next_game (descriptive only)

CREATE TABLE IF NOT EXISTS fact_team_schedule_spot (
  game_id                VARCHAR,
  team                   VARCHAR,
  season                 INTEGER,
  week                   INTEGER,
  home_flag              INTEGER,
  rest_days              INTEGER,
  short_week_flag        INTEGER,
  long_rest_flag         INTEGER,
  bye_week_prior_flag    INTEGER,
  season_opener_flag     INTEGER,
  consecutive_road_games_prior INTEGER,
  consecutive_home_games_prior INTEGER,
  third_road_in_four_flag INTEGER,
  -- DESCRIPTIVE ONLY — never use as predictive feature
  days_until_next_game_DESCRIPTIVE_ONLY INTEGER,
  PRIMARY KEY (game_id, team)
);

-- ---- fact_power_rating_week -------------------------------------------------
-- Grain:   season + week + team
-- Update:  Weekly (after all games complete)
-- Pregame: Pre-game Elo uses pre_game_elo column; post_game_elo is postgame only

CREATE TABLE IF NOT EXISTS fact_power_rating_week (
  game_id                VARCHAR,
  team                   VARCHAR,
  season                 INTEGER,
  week                   INTEGER,
  elo_rating_pregame     DOUBLE,
  elo_rating_postgame    DOUBLE,
  offensive_power        DOUBLE,
  defensive_power        DOUBLE,
  total_environment_rating DOUBLE,
  qb_pass_epa_context    DOUBLE,
  pace_proxy             DOUBLE,
  PRIMARY KEY (game_id, team)
);
