-- =============================================================================
-- STAGING: stg_market_game_line
-- Source:  raw_market_odds_snapshot
-- Grain:   source_name + sportsbook + game_id + market_timestamp + market_type + selection
-- Leakage: Depends on snapshot_type.
--   opening  → safe for pregame prediction at any horizon
--   intraday → safe only up to the relevant prediction horizon cutoff
--   closing  → NOT safe for pregame prediction; closing-only models only
-- Update cadence: Live season — every 60 min; historical — batch
-- =============================================================================

CREATE OR REPLACE VIEW stg_market_game_line AS
WITH standardized AS (
  SELECT
    source_name,
    sportsbook,
    event_id,
    -- Join game_id from schedules using home/away team + game_date
    COALESCE(
      ods.game_id,
      g.game_id
    ) AS game_id,
    CAST(ods.season AS INTEGER)                   AS season,
    CAST(ods.week   AS INTEGER)                   AS week,

    -- Timestamps in UTC
    CAST(market_timestamp AS TIMESTAMPTZ)         AS market_timestamp_utc,
    CAST(game_datetime    AS TIMESTAMPTZ)         AS game_datetime_utc,

    -- Teams (already standardised by adapter)
    ods.home_team,
    ods.away_team,

    -- Market
    LOWER(market_type)                            AS market_type,
    LOWER(selection)                              AS selection,
    CAST(line           AS DOUBLE)                AS line,
    CAST(price_american AS INTEGER)               AS price_american,
    CAST(price_decimal  AS DOUBLE)                AS price_decimal,
    CAST(implied_probability_raw AS DOUBLE)       AS implied_prob_raw,

    -- Derived fair probabilities (vig removed for two-way markets)
    -- Will be completed in pairing step below
    CAST(overunder_line AS DOUBLE)                AS overunder_line,
    LOWER(snapshot_type)                          AS snapshot_type,   -- opening|intraday|closing
    ingestion_ts

  FROM raw_market_odds_snapshot ods
  -- Best-effort game_id match via schedules
  LEFT JOIN (
    SELECT game_id, season, home_team, away_team,
           CAST(gameday AS DATE) AS game_date
    FROM raw_schedules
    WHERE game_type != 'PRE'
  ) g
    ON ods.home_team = g.home_team
   AND ods.away_team = g.away_team
   AND CAST(ods.game_datetime AS DATE) = g.game_date
   AND ods.season = g.season
)

SELECT
  *,
  -- Implied total from spread + total (market-implied team totals)
  CASE
    WHEN market_type = 'spread' AND selection = 'home' AND overunder_line IS NOT NULL
    THEN overunder_line / 2.0 - line / 2.0
  END AS implied_home_team_total,
  CASE
    WHEN market_type = 'spread' AND selection = 'home' AND overunder_line IS NOT NULL
    THEN overunder_line / 2.0 + line / 2.0
  END AS implied_away_team_total,

  -- Favorite / underdog flags
  CASE
    WHEN market_type = 'spread' AND selection = 'home' AND line < 0 THEN 1
    ELSE 0
  END AS home_favorite_flag,
  CASE
    WHEN market_type = 'spread' AND selection = 'home' AND line > 0 THEN 1
    ELSE 0
  END AS home_underdog_flag,

  -- Key number proximity (spread)
  CASE
    WHEN market_type = 'spread'
    THEN LEAST(
           ABS(ABS(line) - 3),
           ABS(ABS(line) - 6),
           ABS(ABS(line) - 7),
           ABS(ABS(line) - 10),
           ABS(ABS(line) - 14)
         )
  END AS spread_key_number_distance,

  -- Key number proximity (total)
  CASE
    WHEN market_type = 'total'
    THEN LEAST(
           ABS(line - 37), ABS(line - 38),
           ABS(line - 41), ABS(line - 44),
           ABS(line - 45), ABS(line - 47),
           ABS(line - 48), ABS(line - 51)
         )
  END AS total_key_number_distance

FROM standardized
;


-- =============================================================================
-- STAGING: stg_market_consensus_game
-- Source:  raw_market_odds_snapshot (aggregated across books per timestamp)
-- Grain:   game_id + market_timestamp + market_type + selection
-- Purpose: Consensus line + dispersion across sportsbooks
-- =============================================================================

CREATE OR REPLACE VIEW stg_market_consensus_game AS
SELECT
  game_id,
  season,
  week,
  market_timestamp_utc  AS market_timestamp,
  market_type,
  selection,

  -- Consensus metrics across books
  COUNT(DISTINCT sportsbook)                    AS book_count,
  AVG(line)                                     AS consensus_line,
  MIN(line)                                     AS min_line,
  MAX(line)                                     AS max_line,
  STDDEV_POP(line)                              AS line_stddev,
  MAX(line) - MIN(line)                         AS line_range,

  -- Average implied probability
  AVG(implied_prob_raw)                         AS avg_implied_prob,

  snapshot_type,
  MAX(ingestion_ts)                             AS ingestion_ts

FROM stg_market_game_line
WHERE game_id IS NOT NULL
GROUP BY game_id, season, week, market_timestamp_utc, market_type, selection, snapshot_type
;


-- =============================================================================
-- STAGING: stg_market_close_game
-- Source:  stg_market_game_line (closing rows only)
-- Grain:   game_id + market_type + selection (one closing line per game/market)
-- LEAKAGE WARNING: This table must NEVER be used as a predictive feature.
--                  It is ONLY for CLV analysis and postgame research.
-- =============================================================================

CREATE OR REPLACE VIEW stg_market_close_game AS
WITH close_ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY game_id, market_type, selection
      ORDER BY market_timestamp_utc DESC
    ) AS rn
  FROM stg_market_game_line
  WHERE snapshot_type = 'closing'
    AND game_id IS NOT NULL
)

SELECT
  game_id,
  season,
  week,
  home_team,
  away_team,
  market_type,
  selection,
  market_timestamp_utc    AS close_timestamp,
  line                    AS closing_line,
  price_american          AS closing_price_american,
  price_decimal           AS closing_price_decimal,
  implied_prob_raw        AS closing_implied_prob,
  implied_home_team_total,
  implied_away_team_total,
  home_favorite_flag,
  home_underdog_flag,
  spread_key_number_distance,
  total_key_number_distance,
  sportsbook              AS close_sportsbook,
  source_name,
  ingestion_ts
FROM close_ranked
WHERE rn = 1
;
