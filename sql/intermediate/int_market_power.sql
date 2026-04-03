-- =============================================================================
-- INTERMEDIATE: int_market_open_close
-- Grain:   game_id + market_type + selection
-- Purpose: Opening vs closing line comparison, movement analysis, key numbers.
-- LEAKAGE:
--   opening_* columns → safe for pregame early-week models
--   closing_* columns → NOT safe for pregame; CLV/research use only
-- =============================================================================

CREATE OR REPLACE VIEW int_market_open_close AS
WITH open_lines AS (
  SELECT
    game_id, season, week, market_type, selection,
    home_team, away_team,
    MIN(market_timestamp_utc)      AS open_timestamp,
    -- Take the first snapshot per game (true opening line)
    FIRST(line ORDER BY market_timestamp_utc) AS opening_line,
    FIRST(price_american ORDER BY market_timestamp_utc) AS opening_price,
    FIRST(implied_prob_raw ORDER BY market_timestamp_utc) AS opening_implied_prob
  FROM stg_market_game_line
  WHERE snapshot_type IN ('opening','intraday')
    AND game_id IS NOT NULL
  GROUP BY game_id, season, week, market_type, selection, home_team, away_team
),
close_lines AS (
  SELECT
    game_id, market_type, selection,
    closing_line,
    closing_price_american,
    closing_implied_prob,
    close_timestamp,
    close_sportsbook,
    book_count_close
  FROM stg_market_close_game
  LEFT JOIN (
    SELECT game_id AS gid, market_type AS mt, selection AS sel,
           COUNT(DISTINCT sportsbook) AS book_count_close
    FROM stg_market_game_line
    WHERE snapshot_type = 'closing'
    GROUP BY gid, mt, sel
  ) bc ON game_id = bc.gid AND market_type = bc.mt AND selection = bc.sel
),
consensus_at_close AS (
  SELECT
    game_id, market_type, selection,
    line_stddev AS close_consensus_dispersion,
    book_count  AS close_book_count
  FROM stg_market_consensus_game
  WHERE snapshot_type = 'closing'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY game_id, market_type, selection
    ORDER BY market_timestamp DESC
  ) = 1
)

SELECT
  o.game_id,
  o.season,
  o.week,
  o.home_team,
  o.away_team,
  o.market_type,
  o.selection,

  -- ===== OPENING (LEAKAGE-SAFE for pregame models) =====
  o.opening_line,
  o.opening_price,
  o.opening_implied_prob,
  o.open_timestamp,

  -- ===== CLOSING (NOT safe for pregame models) =====
  c.closing_line,
  c.closing_price_american,
  c.closing_implied_prob,
  c.close_timestamp,
  c.close_sportsbook,

  -- ===== MOVEMENT =====
  c.closing_line - o.opening_line                            AS move_open_to_close,
  ABS(c.closing_line - o.opening_line)                      AS abs_move_open_to_close,

  -- Favorite flip: line crossed zero (side flipped)
  CASE
    WHEN o.opening_line > 0 AND c.closing_line < 0 THEN 1
    WHEN o.opening_line < 0 AND c.closing_line > 0 THEN 1
    ELSE 0
  END AS favorite_flip_flag,

  -- ===== KEY NUMBER CROSSINGS (spread) =====
  CASE WHEN market_type = 'spread' THEN
    CASE WHEN
      (o.opening_line < -3  AND c.closing_line >= -3) OR
      (o.opening_line > -3  AND c.closing_line <= -3) OR
      (o.opening_line < 3   AND c.closing_line >= 3)  OR
      (o.opening_line > 3   AND c.closing_line <= 3)
    THEN 1 ELSE 0 END
  END AS crossed_key_3_flag,

  CASE WHEN market_type = 'spread' THEN
    CASE WHEN
      (o.opening_line < -7  AND c.closing_line >= -7) OR
      (o.opening_line > -7  AND c.closing_line <= -7) OR
      (o.opening_line < 7   AND c.closing_line >= 7)  OR
      (o.opening_line > 7   AND c.closing_line <= 7)
    THEN 1 ELSE 0 END
  END AS crossed_key_7_flag,

  CASE WHEN market_type = 'spread' THEN
    CASE WHEN
      (o.opening_line < -10 AND c.closing_line >= -10) OR
      (o.opening_line > -10 AND c.closing_line <= -10) OR
      (o.opening_line < 10  AND c.closing_line >= 10)  OR
      (o.opening_line > 10  AND c.closing_line <= 10)
    THEN 1 ELSE 0 END
  END AS crossed_key_10_flag,

  -- Line volatility (range normalized by absolute opening)
  CASE
    WHEN ABS(o.opening_line) > 0.5
    THEN ABS(c.closing_line - o.opening_line) / ABS(o.opening_line)
    ELSE ABS(c.closing_line - o.opening_line)
  END AS line_volatility_score,

  -- Consensus dispersion at close
  cc.close_consensus_dispersion,
  cc.close_book_count,

  -- Implied team totals from spread+total (where applicable)
  CASE
    WHEN market_type = 'spread' AND selection = 'home'
     AND o.opening_line IS NOT NULL
    THEN NULL  -- would need corresponding total join
  END AS opening_implied_home_total,  -- populated in mart join

  -- Home/away indicators
  CASE WHEN market_type='spread' AND selection='home' AND o.opening_line < 0 THEN 1 ELSE 0 END AS home_fav_opening,
  CASE WHEN market_type='spread' AND selection='home' AND o.opening_line > 0 THEN 1 ELSE 0 END AS home_dog_opening,
  CASE WHEN market_type='spread' AND selection='away' AND o.opening_line < 0 THEN 1 ELSE 0 END AS road_fav_opening

FROM open_lines o
LEFT JOIN close_lines c
  ON o.game_id     = c.game_id
 AND o.market_type = c.market_type
 AND o.selection   = c.selection
LEFT JOIN consensus_at_close cc
  ON o.game_id     = cc.game_id
 AND o.market_type = cc.market_type
 AND o.selection   = cc.selection
;


-- =============================================================================
-- INTERMEDIATE: int_market_time_buckets
-- Grain:   game_id + market_type + selection + time_bucket
-- Purpose: Line values at defined time windows before kickoff.
--          Enables research on line movement patterns.
-- LEAKAGE: Each bucket is safe up to its hours_before_kickoff threshold.
-- =============================================================================

CREATE OR REPLACE VIEW int_market_time_buckets AS
WITH game_kickoffs AS (
  SELECT game_id, season, week, game_date, kickoff_ts_approx
  FROM stg_weather_game_kickoff
  WHERE game_id IS NOT NULL
),
bucketed AS (
  SELECT
    l.game_id,
    l.season,
    l.week,
    l.market_type,
    l.selection,
    l.line,
    l.price_american,
    l.implied_prob_raw,
    l.market_timestamp_utc,
    gk.kickoff_ts_approx,
    EXTRACT(EPOCH FROM (gk.kickoff_ts_approx - l.market_timestamp_utc)) / 3600.0
      AS hours_before_kickoff,

    CASE
      WHEN EXTRACT(EPOCH FROM (gk.kickoff_ts_approx - l.market_timestamp_utc)) / 3600.0 > 120 THEN 'open'
      WHEN EXTRACT(EPOCH FROM (gk.kickoff_ts_approx - l.market_timestamp_utc)) / 3600.0 BETWEEN 72  AND 120 THEN '72h'
      WHEN EXTRACT(EPOCH FROM (gk.kickoff_ts_approx - l.market_timestamp_utc)) / 3600.0 BETWEEN 24  AND  72 THEN '24h'
      WHEN EXTRACT(EPOCH FROM (gk.kickoff_ts_approx - l.market_timestamp_utc)) / 3600.0 BETWEEN 6   AND  24 THEN '6h'
      WHEN EXTRACT(EPOCH FROM (gk.kickoff_ts_approx - l.market_timestamp_utc)) / 3600.0 BETWEEN 1   AND   6 THEN '1h'
      WHEN EXTRACT(EPOCH FROM (gk.kickoff_ts_approx - l.market_timestamp_utc)) / 3600.0 BETWEEN 0   AND   1 THEN 'close'
      ELSE 'post_game'
    END AS time_bucket,

    -- Within each bucket take the row closest to the bucket boundary
    ROW_NUMBER() OVER (
      PARTITION BY l.game_id, l.market_type, l.selection,
        CASE
          WHEN EXTRACT(EPOCH FROM (gk.kickoff_ts_approx - l.market_timestamp_utc)) / 3600.0 > 120 THEN 'open'
          WHEN EXTRACT(EPOCH FROM (gk.kickoff_ts_approx - l.market_timestamp_utc)) / 3600.0 BETWEEN 72  AND 120 THEN '72h'
          WHEN EXTRACT(EPOCH FROM (gk.kickoff_ts_approx - l.market_timestamp_utc)) / 3600.0 BETWEEN 24  AND  72 THEN '24h'
          WHEN EXTRACT(EPOCH FROM (gk.kickoff_ts_approx - l.market_timestamp_utc)) / 3600.0 BETWEEN 6   AND  24 THEN '6h'
          WHEN EXTRACT(EPOCH FROM (gk.kickoff_ts_approx - l.market_timestamp_utc)) / 3600.0 BETWEEN 1   AND   6 THEN '1h'
          WHEN EXTRACT(EPOCH FROM (gk.kickoff_ts_approx - l.market_timestamp_utc)) / 3600.0 BETWEEN 0   AND   1 THEN 'close'
          ELSE 'post_game'
        END
      ORDER BY l.market_timestamp_utc DESC  -- most recent within bucket
    ) AS rn_in_bucket

  FROM stg_market_game_line l
  JOIN game_kickoffs gk ON l.game_id = gk.game_id
  WHERE l.game_id IS NOT NULL
    AND EXTRACT(EPOCH FROM (gk.kickoff_ts_approx - l.market_timestamp_utc)) / 3600.0 >= 0
)

SELECT
  game_id, season, week, market_type, selection,
  time_bucket,
  line                  AS bucket_line,
  price_american        AS bucket_price,
  implied_prob_raw      AS bucket_implied_prob,
  market_timestamp_utc  AS bucket_timestamp,
  hours_before_kickoff
FROM bucketed
WHERE rn_in_bucket = 1
  AND time_bucket != 'post_game'
;


-- =============================================================================
-- INTERMEDIATE: int_team_power_rating
-- Grain:   season + week + team
-- Purpose: Internally derived team strength ratings (Elo + offense/defense split).
-- LEAKAGE: All ratings are lagged — only use data from prior games.
-- =============================================================================

CREATE OR REPLACE VIEW int_team_power_rating AS
-- Elo ratings computed incrementally via recursive CTE.
-- Initial rating 1500. Home field advantage = 65 Elo points.
-- K-factor = 20. Season reversion = 33% toward 1500.
WITH RECURSIVE

-- Seed: all teams at start of 1999 season
team_season_init AS (
  SELECT DISTINCT team, MIN(season) AS first_season
  FROM int_team_game
  WHERE season_type = 'regular'
  GROUP BY team
),

-- All games in chronological order (one row per team per game)
game_sequence AS (
  SELECT
    tg.game_id,
    tg.team,
    tg.opponent,
    tg.season,
    tg.week,
    tg.home_flag,
    tg.win_flag,
    tg.points_for,
    tg.points_against,
    tg.epa_per_play,
    tg.def_epa_per_play,
    tg.pass_epa_per_dropback,
    tg.rush_epa_per_carry,
    ROW_NUMBER() OVER (PARTITION BY tg.team ORDER BY tg.season, tg.week) AS game_seq
  FROM int_team_game tg
  WHERE tg.season_type = 'regular'
),

-- Elo update CTE (DuckDB supports recursive CTEs on columnar data)
-- For large histories this processes ~32 teams × 17+ weeks × 25 seasons = ~14k rows
elo_updates AS (
  -- Base: first game for each team
  SELECT
    gs.game_id,
    gs.team,
    gs.season,
    gs.week,
    gs.home_flag,
    gs.win_flag,
    gs.game_seq,
    1500.0                          AS pre_game_elo,
    -- Post-game elo updated below
    1500.0 + CASE
      WHEN gs.win_flag = 1 THEN 20.0 * (1.0 - 1.0/(1.0 + POWER(10.0, -65.0/400.0)))
      WHEN gs.win_flag = 0 THEN 20.0 * (0.0 - 1.0/(1.0 + POWER(10.0, -65.0/400.0)))
      ELSE 0.0
    END                             AS post_game_elo
  FROM game_sequence gs
  WHERE gs.game_seq = 1

  UNION ALL

  SELECT
    gs.game_id,
    gs.team,
    gs.season,
    gs.week,
    gs.home_flag,
    gs.win_flag,
    gs.game_seq,
    -- Apply season reversion at season boundary
    CASE
      WHEN gs.season > prev.season
      THEN prev.post_game_elo + 0.33 * (1500.0 - prev.post_game_elo)
      ELSE prev.post_game_elo
    END AS pre_game_elo,
    CASE
      WHEN gs.win_flag = 1
      THEN (prev.post_game_elo + 0.33 * CASE WHEN gs.season > prev.season THEN (1500.0 - prev.post_game_elo) ELSE 0 END)
           + 20.0 * (1.0 - 1.0/(1.0 + POWER(10.0,
               (CASE WHEN gs.home_flag=1 THEN -65.0 ELSE 65.0 END) / 400.0)))
      WHEN gs.win_flag = 0
      THEN (prev.post_game_elo + 0.33 * CASE WHEN gs.season > prev.season THEN (1500.0 - prev.post_game_elo) ELSE 0 END)
           + 20.0 * (0.0 - 1.0/(1.0 + POWER(10.0,
               (CASE WHEN gs.home_flag=1 THEN -65.0 ELSE 65.0 END) / 400.0)))
      ELSE prev.post_game_elo
    END AS post_game_elo

  FROM game_sequence gs
  JOIN elo_updates prev
    ON gs.team     = prev.team
   AND gs.game_seq = prev.game_seq + 1
)

-- Final output: pre-game ratings (entering each week)
SELECT
  eu.game_id,
  eu.team,
  eu.season,
  eu.week,
  eu.home_flag,
  eu.win_flag,
  ROUND(eu.pre_game_elo, 1)                          AS elo_rating_pregame,
  ROUND(eu.post_game_elo, 1)                         AS elo_rating_postgame,

  -- Offense / defense power from rolling EPA (joined from int_team_form)
  tf.epa_pp_blended                                  AS offensive_power,
  tf.def_epa_pp_blended                              AS defensive_power,
  -- Combined power (total environment rating = offense - defense of both teams)
  tf.epa_pp_blended + ABS(tf.def_epa_pp_blended)    AS total_environment_rating,

  -- QB-adjusted placeholder (joins to QB EPA from int_player_form)
  -- Actual QB adjustment implemented in mart layer via int_qb_team_context
  tf.pass_epa_std                                    AS qb_pass_epa_context,

  -- Pace-adjusted pace rating
  tf.pass_rate_std                                   AS pace_proxy

FROM elo_updates eu
JOIN int_team_form tf
  ON eu.team   = tf.team
 AND eu.season = tf.season
 AND eu.week   = tf.week
;
