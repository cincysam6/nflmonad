-- =============================================================================
-- DATA VALIDATION TESTS
-- =============================================================================
-- Each test returns rows ONLY when it fails.
-- A passing test returns 0 rows.
-- Run via run_all_tests() in R or the daily pipeline.
-- =============================================================================


-- ---- TEST: PBP grain uniqueness ---------------------------------------------
-- PASS = 0 rows
SELECT 'FAIL: raw_pbp duplicate game_id+play_id' AS test_name, game_id, play_id, COUNT(*) AS n
FROM raw_pbp
GROUP BY game_id, play_id
HAVING COUNT(*) > 1
LIMIT 10;


-- ---- TEST: stg_games grain uniqueness ---------------------------------------
SELECT 'FAIL: stg_games duplicate game_id' AS test_name, game_id, COUNT(*) AS n
FROM stg_games
GROUP BY game_id
HAVING COUNT(*) > 1
LIMIT 10;


-- ---- TEST: stg_games not-null game_id ---------------------------------------
SELECT 'FAIL: stg_games null game_id' AS test_name, COUNT(*) AS n
FROM stg_games
WHERE game_id IS NULL
HAVING COUNT(*) > 0;


-- ---- TEST: stg_games home_score/away_score range ----------------------------
SELECT 'FAIL: stg_games impossible score' AS test_name, game_id, home_score, away_score
FROM stg_games
WHERE game_completed_flag = 1
  AND (home_score > 80 OR away_score > 80 OR home_score < 0 OR away_score < 0);


-- ---- TEST: stg_player_week not-null player_id --------------------------------
SELECT 'FAIL: stg_player_week null player_id' AS test_name, COUNT(*) AS n
FROM stg_player_week
WHERE player_id IS NULL
HAVING COUNT(*) > 0;


-- ---- TEST: stg_player_week grain uniqueness ---------------------------------
SELECT 'FAIL: stg_player_week duplicate grain' AS test_name,
       season, week, player_id, position, COUNT(*) AS n
FROM stg_player_week
GROUP BY season, week, player_id, position
HAVING COUNT(*) > 1
LIMIT 10;


-- ---- TEST: int_team_form leakage check (epa_pp_std should be NULL in week 1) --
-- Every team's week-1 row should have NULL or 0 games_played_std
SELECT 'FAIL: int_team_form week1 has non-null form metrics' AS test_name,
       team, season, week, games_played_std, epa_pp_std
FROM int_team_form
WHERE week = 1
  AND games_played_std > 0
LIMIT 10;


-- ---- TEST: int_team_game - team appears once per game -----------------------
SELECT 'FAIL: int_team_game team appears more than once per game' AS test_name,
       game_id, team, COUNT(*) AS n
FROM int_team_game
GROUP BY game_id, team
HAVING COUNT(*) > 1
LIMIT 10;


-- ---- TEST: mart_game_modeling leakage — home_score must be NULL for future games --
SELECT 'FAIL: mart_game_modeling future game has score' AS test_name,
       game_id, game_date, home_score, away_score
FROM mart_game_modeling
WHERE game_completed_flag = 0
  AND (home_score IS NOT NULL OR away_score IS NOT NULL)
LIMIT 10;


-- ---- TEST: dim_team - all stg_games teams exist in dim_team -----------------
SELECT 'FAIL: stg_games home_team not in dim_team' AS test_name, home_team
FROM stg_games
WHERE home_team NOT IN (SELECT team_abbr FROM dim_team)
LIMIT 10;

SELECT 'FAIL: stg_games away_team not in dim_team' AS test_name, away_team
FROM stg_games
WHERE away_team NOT IN (SELECT team_abbr FROM dim_team)
LIMIT 10;


-- ---- TEST: season range sanity ----------------------------------------------
SELECT 'FAIL: raw_pbp season out of range' AS test_name, season, COUNT(*) AS n
FROM raw_pbp
GROUP BY season
HAVING season < 1999 OR season > 2030;


-- ---- TEST: injury severity score encoding -----------------------------------
SELECT 'FAIL: stg_injuries_weekly invalid severity score' AS test_name,
       player_id, season, week, report_status, injury_severity_score
FROM stg_injuries_weekly
WHERE injury_severity_score NOT IN (0, 1, 2, 3, 4)
LIMIT 10;


-- ---- TEST: int_injury_team_impact uniqueness --------------------------------
SELECT 'FAIL: int_injury_team_impact duplicate grain' AS test_name,
       team, season, week, COUNT(*) AS n
FROM int_injury_team_impact
GROUP BY team, season, week
HAVING COUNT(*) > 1
LIMIT 10;


-- ---- TEST: mart_player_week_projection — no future rows with actuals --------
SELECT 'FAIL: projection has future actuals' AS test_name,
       player_id, season, week, actual_targets, actual_carries
FROM mart_player_week_projection
WHERE (actual_targets IS NOT NULL OR actual_carries IS NOT NULL)
  AND game_id IS NULL  -- no game yet
LIMIT 10;
