-- =============================================================================
-- INTERMEDIATE: int_player_game
-- Grain:   game_id + player_id
-- Purpose: Per-player per-game box score + snap context + team info.
-- =============================================================================

CREATE OR REPLACE VIEW int_player_game AS
SELECT
  pw.player_id,
  pw.full_name,
  pw.position,
  pw.position_group,
  pw.team,
  pw.opponent,
  pw.season,
  pw.week,
  pw.season_type,

  -- Derive game_id from schedule join
  g.game_id,
  g.game_date,
  g.home_team,
  g.away_team,
  CASE WHEN pw.team = g.home_team THEN 1 ELSE 0 END AS home_flag,

  -- Passing
  pw.completions,
  pw.attempts,
  pw.passing_yards,
  pw.passing_tds,
  pw.interceptions,
  pw.sacks,
  pw.passing_air_yards,
  pw.passing_yac,
  pw.passing_epa,
  pw.dakota,
  CASE WHEN pw.attempts > 0
       THEN CAST(pw.completions AS DOUBLE) / pw.attempts END AS comp_pct,
  CASE WHEN pw.attempts > 0
       THEN pw.passing_yards / pw.attempts END AS yards_per_attempt,

  -- Rushing
  pw.carries,
  pw.rushing_yards,
  pw.rushing_tds,
  pw.rushing_epa,
  CASE WHEN pw.carries > 0
       THEN pw.rushing_yards / pw.carries END AS yards_per_carry,

  -- Receiving
  pw.targets,
  pw.receptions,
  pw.receiving_yards,
  pw.receiving_tds,
  pw.receiving_air_yards,
  pw.receiving_yac,
  pw.receiving_epa,
  pw.target_share,
  pw.air_yards_share,
  pw.wopr,
  pw.racr,
  CASE WHEN pw.targets > 0
       THEN CAST(pw.receptions AS DOUBLE) / pw.targets END AS catch_rate,
  CASE WHEN pw.targets > 0
       THEN pw.receiving_yards / pw.targets END AS yards_per_target,

  -- Snap counts (from snap count table — join on position proxy)
  sc.offense_snaps,
  sc.offense_snap_pct,

  -- Fantasy
  pw.fantasy_points_std,
  pw.fantasy_points_ppr

FROM stg_player_week pw
LEFT JOIN stg_games g
  ON pw.season = g.season
  AND pw.week  = g.week
  AND (pw.team = g.home_team OR pw.team = g.away_team)
  AND g.season_type IN ('regular','postseason')
LEFT JOIN stg_snap_counts_weekly sc
  ON pw.season = sc.season
  AND pw.week  = sc.week
  AND pw.team  = sc.team
  AND pw.full_name = sc.full_name   -- best available join key without gsis<->pfr crosswalk
;


-- =============================================================================
-- INTERMEDIATE: int_player_form
-- Grain:   season + week + player_id  (pre-game rolling features)
-- Purpose: Lagged usage and efficiency features for player projection.
-- =============================================================================

CREATE OR REPLACE VIEW int_player_form AS
WITH player_games AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY player_id, season ORDER BY week) AS game_num
  FROM int_player_game
  WHERE season_type = 'regular'
    AND player_id IS NOT NULL
)

SELECT
  player_id,
  full_name,
  position,
  position_group,
  team,
  season,
  week,
  game_num,

  -- ===== SEASON-TO-DATE (lagged) =====
  AVG(targets)          OVER w_std AS targets_per_game_std,
  AVG(receptions)       OVER w_std AS rec_per_game_std,
  AVG(receiving_yards)  OVER w_std AS rec_yds_per_game_std,
  AVG(receiving_tds)    OVER w_std AS rec_tds_per_game_std,
  AVG(target_share)     OVER w_std AS target_share_std,
  AVG(air_yards_share)  OVER w_std AS air_yards_share_std,
  AVG(wopr)             OVER w_std AS wopr_std,
  AVG(offense_snap_pct) OVER w_std AS snap_pct_std,
  AVG(carries)          OVER w_std AS carries_per_game_std,
  AVG(rushing_yards)    OVER w_std AS rush_yds_per_game_std,
  AVG(rushing_tds)      OVER w_std AS rush_tds_per_game_std,
  AVG(attempts)         OVER w_std AS attempts_per_game_std,
  AVG(passing_yards)    OVER w_std AS pass_yds_per_game_std,
  AVG(passing_tds)      OVER w_std AS pass_tds_per_game_std,
  AVG(receiving_epa)    OVER w_std AS rec_epa_per_game_std,
  AVG(rushing_epa)      OVER w_std AS rush_epa_per_game_std,
  AVG(passing_epa)      OVER w_std AS pass_epa_per_game_std,
  COUNT(*)              OVER w_std AS games_played_std,

  -- ===== LAST 4 GAMES =====
  AVG(targets)          OVER w_l4  AS targets_per_game_l4,
  AVG(receptions)       OVER w_l4  AS rec_per_game_l4,
  AVG(receiving_yards)  OVER w_l4  AS rec_yds_per_game_l4,
  AVG(target_share)     OVER w_l4  AS target_share_l4,
  AVG(offense_snap_pct) OVER w_l4  AS snap_pct_l4,
  AVG(carries)          OVER w_l4  AS carries_per_game_l4,
  AVG(rushing_yards)    OVER w_l4  AS rush_yds_per_game_l4,
  AVG(attempts)         OVER w_l4  AS attempts_per_game_l4,
  AVG(passing_yards)    OVER w_l4  AS pass_yds_per_game_l4,

  -- ===== LAST 2 GAMES (hot/cold signal) =====
  AVG(targets)          OVER w_l2  AS targets_per_game_l2,
  AVG(receiving_yards)  OVER w_l2  AS rec_yds_per_game_l2,
  AVG(carries)          OVER w_l2  AS carries_per_game_l2

FROM player_games

WINDOW
  w_std AS (
    PARTITION BY player_id, season
    ORDER BY week
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
  ),
  w_l4 AS (
    PARTITION BY player_id, season
    ORDER BY week
    ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
  ),
  w_l2 AS (
    PARTITION BY player_id, season
    ORDER BY week
    ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING
  )
;


-- =============================================================================
-- INTERMEDIATE: int_qb_team_context
-- Grain:   game_id + player_id (QB only)
-- Purpose: QB metrics joined with OL proxy, receiver health, opponent pressure.
-- =============================================================================

CREATE OR REPLACE VIEW int_qb_team_context AS
WITH qb_games AS (
  SELECT * FROM int_player_game
  WHERE position = 'QB'
    AND attempts >= 5   -- filter out emergency backups with 1 pass
)

SELECT
  qg.game_id,
  qg.player_id,
  qg.full_name,
  qg.team,
  qg.opponent,
  qg.season,
  qg.week,
  qg.home_flag,
  qg.game_date,

  -- QB box score
  qg.completions,
  qg.attempts,
  qg.passing_yards,
  qg.passing_tds,
  qg.interceptions,
  qg.sacks,
  qg.passing_air_yards,
  qg.passing_epa,
  qg.dakota,
  qg.comp_pct,
  qg.yards_per_attempt,

  -- Rushing contribution
  qg.carries          AS qb_carries,
  qg.rushing_yards    AS qb_rush_yards,
  qg.rushing_tds      AS qb_rush_tds,

  -- Team offensive context
  tg.pass_rate,
  tg.offensive_plays,
  tg.pass_epa_per_dropback AS team_pass_epa,

  -- OL proxy: sacks allowed by the team this game
  tg.sacks_allowed    AS oline_sacks_allowed,

  -- Opponent defensive pressure proxy
  opp_tg.def_sacks            AS opp_def_sacks,
  opp_tg.def_pass_epa_per_dropback AS opp_def_pass_epa,

  -- Receiver injury burden (WR + TE)
  COALESCE(inj.wr_burden, 0) + COALESCE(inj.te_burden, 0) AS skill_position_injury_burden,
  inj.wr_out,
  inj.te_out

FROM qb_games qg
LEFT JOIN int_team_game tg
  ON qg.game_id = tg.game_id AND qg.team = tg.team
LEFT JOIN int_team_game opp_tg
  ON qg.game_id = opp_tg.game_id AND qg.opponent = opp_tg.team
LEFT JOIN int_injury_team_impact inj
  ON qg.team = inj.team
  AND qg.season = inj.season
  AND qg.week   = inj.week
;
