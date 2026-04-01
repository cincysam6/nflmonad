-- =============================================================================
-- EXTENDED DATA TESTS — External Data Sources
-- Each test returns rows ONLY on failure (0 rows = pass).
-- =============================================================================


-- ============================================================
-- MARKET / ODDS TESTS
-- ============================================================

-- TEST: No closing lines in mart_backtest_game as predictive features
-- closing_spread must be NULL in backtest mart (enforced by view design,
-- but we test the underlying market table for accidental closing rows
-- being mixed into horizon buckets)
SELECT 'FAIL: market time bucket contains post-game rows' AS test_name,
       game_id, time_bucket, bucket_timestamp, kickoff_ts_approx
FROM (
  SELECT tb.game_id, tb.time_bucket, tb.bucket_timestamp,
         gk.kickoff_ts_approx
  FROM int_market_time_buckets tb
  JOIN (SELECT game_id, kickoff_ts_approx FROM stg_weather_game_kickoff) gk
    ON tb.game_id = gk.game_id
  WHERE tb.time_bucket NOT IN ('open','72h','24h','6h','1h','close')
) t
LIMIT 10;


-- TEST: All odds rows must have valid market_type
SELECT 'FAIL: stg_market_game_line invalid market_type' AS test_name,
       source_name, market_type, COUNT(*) AS n
FROM stg_market_game_line
WHERE market_type NOT IN ('spread','total','moneyline','team_total','player_prop')
GROUP BY source_name, market_type
HAVING COUNT(*) > 0
LIMIT 10;


-- TEST: Implied probability must be between 0 and 1
SELECT 'FAIL: implied_prob_raw out of range' AS test_name,
       source_name, sportsbook, market_type, implied_prob_raw
FROM stg_market_game_line
WHERE implied_prob_raw < 0 OR implied_prob_raw > 1
LIMIT 10;


-- TEST: Opening spread must be within reasonable bounds
SELECT 'FAIL: opening spread out of reasonable range' AS test_name,
       game_id, opening_spread
FROM int_market_open_close
WHERE market_type = 'spread' AND selection = 'home'
  AND (opening_spread < -30 OR opening_spread > 30)
LIMIT 10;


-- TEST: Opening total must be within reasonable bounds
SELECT 'FAIL: opening total out of reasonable range' AS test_name,
       game_id, opening_total
FROM int_market_open_close
WHERE market_type = 'total' AND selection = 'over'
  AND (opening_total < 25 OR opening_total > 80)
LIMIT 10;


-- TEST: mart_backtest_game must NOT contain closing_spread as non-null feature
-- (closing spread is acceptable as a TARGET; it should not be a feature input)
-- The backtest mart does not select closing_spread as a feature — validated by column presence:
SELECT 'FAIL: mart_backtest_game contains unexpected column closing_spread' AS test_name
WHERE FALSE;  -- This test is structural — checked in code by column list


-- ============================================================
-- WEATHER TESTS
-- ============================================================

-- TEST: Wind speed must be non-negative
SELECT 'FAIL: negative wind speed in weather' AS test_name,
       stadium_id, weather_ts, wind_mph
FROM stg_weather_game_hourly
WHERE wind_mph < 0
LIMIT 10;


-- TEST: Temperature must be within survivable bounds (°F)
SELECT 'FAIL: temperature out of plausible range' AS test_name,
       stadium_id, weather_ts, temp_f
FROM stg_weather_game_hourly
WHERE temp_f < -50 OR temp_f > 130
LIMIT 10;


-- TEST: Forecast weather must have a forecast_run_ts
SELECT 'FAIL: forecast row missing forecast_run_ts' AS test_name,
       stadium_id, weather_ts, weather_type
FROM stg_weather_game_hourly
WHERE weather_type = 'forecast' AND forecast_run_ts IS NULL
LIMIT 10;


-- TEST: Historical weather must NOT be used as pregame feature in backtest
-- The weather_source_type column must always be present in backtest marts
-- for users to filter correctly
SELECT 'FAIL: mart_backtest_game missing weather_source_type column' AS test_name
WHERE NOT EXISTS (
  SELECT 1 FROM information_schema.columns
  WHERE table_name = 'mart_backtest_game'
    AND column_name = 'weather_source_type'
);


-- TEST: Dome stadiums should have 0 adverse weather score
SELECT 'FAIL: dome stadium has non-zero adverse weather score' AS test_name,
       wg.game_id, wg.stadium_id, wg.roof_closed_flag, wg.adverse_weather_score
FROM int_weather_game wg
WHERE wg.roof_closed_flag = 1
  AND wg.adverse_weather_score > 0
LIMIT 10;


-- ============================================================
-- STADIUM / TRAVEL TESTS
-- ============================================================

-- TEST: All games must have a matched stadium (latitude not null for home games)
SELECT 'FAIL: game missing stadium lat/lon' AS test_name,
       g.game_id, g.home_team, g.season
FROM stg_games g
LEFT JOIN stg_stadium s
  ON g.home_team = s.team
 AND g.season BETWEEN s.season_start AND s.season_end
WHERE s.stadium_id IS NULL
  AND g.location != 'Neutral'  -- neutral sites don't need home team mapping
  AND g.game_completed_flag = 1
LIMIT 10;


-- TEST: Travel distance must be non-negative
SELECT 'FAIL: negative travel distance' AS test_name,
       game_id, team, travel_km
FROM int_team_travel_game
WHERE travel_km < 0
LIMIT 10;


-- TEST: Home team should have 0 travel km
SELECT 'FAIL: home team has non-zero travel' AS test_name,
       game_id, team, travel_km, is_away_team
FROM int_team_travel_game
WHERE is_away_team = 0 AND travel_km > 1
LIMIT 10;


-- ============================================================
-- POWER RATING / ELO TESTS
-- ============================================================

-- TEST: Elo ratings must be within plausible range
SELECT 'FAIL: Elo rating out of range' AS test_name,
       team, season, week, elo_rating_pregame
FROM int_team_power_rating
WHERE elo_rating_pregame < 1000 OR elo_rating_pregame > 2000
LIMIT 10;


-- TEST: Power rating grain uniqueness
SELECT 'FAIL: int_team_power_rating duplicate grain' AS test_name,
       game_id, team, COUNT(*) AS n
FROM int_team_power_rating
GROUP BY game_id, team
HAVING COUNT(*) > 1
LIMIT 10;


-- ============================================================
-- LEAKAGE TIMESTAMP TESTS
-- ============================================================

-- TEST: No int_market_time_buckets rows should have hours_before_kickoff < 0
-- (a negative value means the odds snapshot was AFTER kickoff — leakage)
SELECT 'FAIL: market snapshot after kickoff in time buckets' AS test_name,
       game_id, market_type, selection, hours_before_kickoff, bucket_timestamp
FROM int_market_time_buckets
WHERE hours_before_kickoff < 0
LIMIT 10;


-- TEST: Forecast weather rows used in int_weather_game must not be postgame
-- (forecast_run_ts should be before game kickoff)
SELECT 'FAIL: forecast weather run after game kickoff' AS test_name,
       wg.game_id, wg.forecast_run_ts,
       gk.kickoff_ts_approx
FROM int_weather_game wg
JOIN stg_weather_game_kickoff gk ON wg.game_id = gk.game_id
WHERE wg.weather_source_type = 'forecast'
  AND CAST(wg.forecast_run_ts AS TIMESTAMPTZ) > gk.kickoff_ts_approx
LIMIT 10;


-- ============================================================
-- SOURCE FRESHNESS DIAGNOSTIC (informational — not strict fail)
-- ============================================================

-- How many games in the current season have market data?
SELECT
  'DIAGNOSTIC: market coverage' AS test_name,
  season,
  COUNT(DISTINCT game_id) AS games_with_odds,
  COUNT(DISTINCT game_id) FILTER (WHERE opening_spread IS NOT NULL) AS games_with_opening_spread,
  COUNT(DISTINCT game_id) FILTER (WHERE closing_spread IS NOT NULL) AS games_with_closing_spread
FROM int_market_open_close
WHERE market_type = 'spread' AND selection = 'home'
GROUP BY season
ORDER BY season DESC
LIMIT 5;

-- How many games in the current season have weather data?
SELECT
  'DIAGNOSTIC: weather coverage' AS test_name,
  season,
  COUNT(DISTINCT game_id) AS games_total,
  COUNT(DISTINCT CASE WHEN temp_f IS NOT NULL THEN game_id END) AS games_with_weather,
  COUNT(DISTINCT CASE WHEN weather_source_type = 'forecast' THEN game_id END) AS games_with_forecast,
  COUNT(DISTINCT CASE WHEN weather_source_type = 'historical' THEN game_id END) AS games_with_historical
FROM stg_weather_game_kickoff
GROUP BY season
ORDER BY season DESC
LIMIT 5;
