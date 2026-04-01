-- =============================================================================
-- MART: mart_player_week_projection
-- Grain:   season + week + player_id
-- Purpose: Universal player projection features + targets for all positions.
-- =============================================================================

CREATE OR REPLACE VIEW mart_player_week_projection AS
SELECT
  -- ===== IDENTIFIERS =====
  pf.player_id,
  pf.full_name,
  pf.position,
  pf.position_group,
  pf.team,
  pf.season,
  pf.week,

  -- Opponent from schedule
  tw.opponent,
  tw.home_flag,
  tw.game_id,

  -- ===== USAGE FEATURES (lagged season-to-date) =====
  pf.targets_per_game_std,
  pf.rec_per_game_std,
  pf.rec_yds_per_game_std,
  pf.rec_tds_per_game_std,
  pf.target_share_std,
  pf.air_yards_share_std,
  pf.wopr_std,
  pf.snap_pct_std,
  pf.carries_per_game_std,
  pf.rush_yds_per_game_std,
  pf.rush_tds_per_game_std,
  pf.attempts_per_game_std,
  pf.pass_yds_per_game_std,
  pf.pass_tds_per_game_std,
  pf.rec_epa_per_game_std,
  pf.rush_epa_per_game_std,
  pf.pass_epa_per_game_std,
  pf.games_played_std,

  -- ===== RECENT FORM (last 4 games) =====
  pf.targets_per_game_l4,
  pf.rec_yds_per_game_l4,
  pf.target_share_l4,
  pf.snap_pct_l4,
  pf.carries_per_game_l4,
  pf.rush_yds_per_game_l4,
  pf.attempts_per_game_l4,
  pf.pass_yds_per_game_l4,

  -- ===== HOT/COLD (last 2 games) =====
  pf.targets_per_game_l2,
  pf.rec_yds_per_game_l2,
  pf.carries_per_game_l2,

  -- ===== TEAM CONTEXT (lagged) =====
  tmf.epa_pp_blended          AS team_epa_blended,
  tmf.pass_epa_std            AS team_pass_epa_std,
  tmf.rush_epa_std            AS team_rush_epa_std,
  tmf.pass_rate_std           AS team_pass_rate_std,
  tmf.pts_for_std             AS team_pts_for_std,

  -- ===== OPPONENT DEFENSIVE CONTEXT (lagged) =====
  opp_tmf.def_epa_pp_blended       AS opp_def_epa_blended,
  opp_tmf.def_pass_epa_std         AS opp_def_pass_epa_std,
  opp_tmf.def_rush_epa_std         AS opp_def_rush_epa_std,
  opp_tmf.def_success_rate_allowed_std AS opp_def_success_rate,
  opp_tmf.def_sacks_std            AS opp_def_sacks_std,
  opp_inj.cb_burden                AS opp_cb_burden,
  opp_inj.dl_burden                AS opp_dl_burden,
  opp_inj.lb_burden                AS opp_lb_burden,

  -- ===== INJURY / AVAILABILITY =====
  inj.report_status               AS injury_status,
  inj.injury_severity_score,
  CASE WHEN inj.report_status IN ('OUT','DOUBTFUL') THEN 1 ELSE 0 END AS injury_out_flag,

  -- ===== TARGETS (post-game; NULL for future games) =====
  pg.targets                      AS actual_targets,
  pg.receptions                   AS actual_receptions,
  pg.receiving_yards              AS actual_rec_yards,
  pg.receiving_tds                AS actual_rec_tds,
  pg.carries                      AS actual_carries,
  pg.rushing_yards                AS actual_rush_yards,
  pg.rushing_tds                  AS actual_rush_tds,
  pg.attempts                     AS actual_pass_attempts,
  pg.passing_yards                AS actual_pass_yards,
  pg.passing_tds                  AS actual_pass_tds,
  pg.fantasy_points_std           AS actual_fpts_std,
  pg.fantasy_points_ppr           AS actual_fpts_ppr

FROM int_player_form pf

-- Team schedule (to get game_id and opponent)
LEFT JOIN stg_team_week tw
  ON pf.team   = tw.team
  AND pf.season = tw.season
  AND pf.week   = tw.week

-- Team form
LEFT JOIN mart_team_week_modeling tmf
  ON pf.team   = tmf.team
  AND pf.season = tmf.season
  AND pf.week   = tmf.week

-- Opponent team form
LEFT JOIN mart_team_week_modeling opp_tmf
  ON tw.opponent = opp_tmf.team
  AND pf.season  = opp_tmf.season
  AND pf.week    = opp_tmf.week

-- Opponent injury context
LEFT JOIN int_injury_team_impact opp_inj
  ON tw.opponent = opp_inj.team
  AND pf.season  = opp_inj.season
  AND pf.week    = opp_inj.week

-- Player injury report this week
LEFT JOIN (
  SELECT player_id, season, week, report_status, injury_severity_score
  FROM stg_injuries_weekly
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY player_id, season, week
    ORDER BY report_date DESC
  ) = 1
) inj ON pf.player_id = inj.player_id
     AND pf.season    = inj.season
     AND pf.week      = inj.week

-- Actual outcomes (for training)
LEFT JOIN int_player_game pg
  ON pf.player_id = pg.player_id
  AND pf.season   = pg.season
  AND pf.week     = pg.week
;


-- =============================================================================
-- MART: mart_qb_projection
-- Grain:   game_id + player_id (QB games only)
-- =============================================================================

CREATE OR REPLACE VIEW mart_qb_projection AS
SELECT
  qc.game_id,
  qc.player_id,
  qc.full_name,
  qc.team,
  qc.opponent,
  qc.season,
  qc.week,
  qc.home_flag,
  qc.game_date,

  -- ===== QB LAGGED FEATURES =====
  pf.attempts_per_game_std,
  pf.pass_yds_per_game_std,
  pf.pass_tds_per_game_std,
  pf.pass_epa_per_game_std,
  pf.rush_yds_per_game_std,  -- designed runs
  pf.attempts_per_game_l4,
  pf.pass_yds_per_game_l4,
  pf.games_played_std,

  -- ===== TEAM CONTEXT =====
  qc.pass_rate,
  qc.team_pass_epa,
  qc.oline_sacks_allowed,
  qc.opp_def_sacks,
  qc.opp_def_pass_epa,
  qc.skill_position_injury_burden,
  qc.wr_out,
  qc.te_out,

  -- ===== NGS PASSING METRICS (lagged season-to-date) =====
  ngs.cpoe                        AS ngs_cpoe_std,
  ngs.aggressiveness              AS ngs_aggressiveness_std,
  ngs.avg_time_to_throw           AS ngs_avg_ttt_std,
  ngs.avg_air_yards_to_sticks     AS ngs_air_yds_to_sticks_std,

  -- ===== TARGETS =====
  qc.completions, qc.attempts AS actual_attempts,
  qc.passing_yards, qc.passing_tds, qc.interceptions,
  qc.sacks, qc.passing_epa, qc.dakota,
  qc.qb_carries, qc.qb_rush_yards, qc.qb_rush_tds

FROM int_qb_team_context qc
LEFT JOIN int_player_form pf
  ON qc.player_id = pf.player_id
  AND qc.season   = pf.season
  AND qc.week     = pf.week
LEFT JOIN (
  -- Rolling average of NGS metrics per QB per week (lagged)
  SELECT
    player_id,
    season,
    week,
    AVG(cpoe)              OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cpoe,
    AVG(aggressiveness)    OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS aggressiveness,
    AVG(avg_time_to_throw) OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_time_to_throw,
    AVG(avg_air_yards_to_sticks) OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_air_yards_to_sticks
  FROM stg_nextgen_player_week
  WHERE stat_type = 'passing'
    AND season_type = 'REG'
) ngs ON qc.player_id = ngs.player_id
      AND qc.season   = ngs.season
      AND qc.week     = ngs.week
;


-- =============================================================================
-- MART: mart_receiver_projection
-- Grain:   game_id + player_id (WR + TE)
-- =============================================================================

CREATE OR REPLACE VIEW mart_receiver_projection AS
SELECT
  p.game_id,
  p.player_id,
  p.full_name,
  p.position,
  p.team,
  p.opponent,
  p.season,
  p.week,
  p.home_flag,

  -- Usage features
  p.targets_per_game_std,
  p.rec_per_game_std,
  p.rec_yds_per_game_std,
  p.rec_tds_per_game_std,
  p.target_share_std,
  p.air_yards_share_std,
  p.wopr_std,
  p.snap_pct_std,
  p.targets_per_game_l4,
  p.target_share_l4,
  p.targets_per_game_l2,

  -- Efficiency (from int_player_game rolling, not available in int_player_form directly)
  -- These would need a separate rolling efficiency CTE; including as placeholders
  p.rec_epa_per_game_std,

  -- NGS Receiving
  ngs.avg_separation,
  ngs.avg_cushion,
  ngs.catch_percentage,
  ngs.avg_yac,
  ngs.avg_yac_above_expectation,

  -- Team context
  p.team_epa_blended,
  p.team_pass_rate_std,
  p.team_pts_for_std,

  -- Opponent DB/coverage burden
  p.opp_cb_burden,
  p.opp_def_pass_epa_std,

  -- Injury
  p.injury_status,
  p.injury_out_flag,

  -- Targets
  p.actual_targets,
  p.actual_receptions,
  p.actual_rec_yards,
  p.actual_rec_tds,
  p.actual_fpts_std,
  p.actual_fpts_ppr

FROM mart_player_week_projection p
LEFT JOIN (
  SELECT
    player_id,
    season,
    week,
    AVG(avg_separation)           OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_separation,
    AVG(avg_cushion)              OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_cushion,
    AVG(catch_percentage)         OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS catch_percentage,
    AVG(avg_yac)                  OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_yac,
    AVG(avg_yac_above_expectation) OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_yac_above_expectation
  FROM stg_nextgen_player_week
  WHERE stat_type = 'receiving'
    AND season_type = 'REG'
) ngs ON p.player_id = ngs.player_id
      AND p.season   = ngs.season
      AND p.week     = ngs.week

WHERE p.position_group IN ('WR','TE')
;


-- =============================================================================
-- MART: mart_rusher_projection
-- Grain:   game_id + player_id (RB only)
-- =============================================================================

CREATE OR REPLACE VIEW mart_rusher_projection AS
SELECT
  p.game_id,
  p.player_id,
  p.full_name,
  p.position,
  p.team,
  p.opponent,
  p.season,
  p.week,
  p.home_flag,

  -- Usage
  p.carries_per_game_std,
  p.rush_yds_per_game_std,
  p.rush_tds_per_game_std,
  p.rush_epa_per_game_std,
  p.carries_per_game_l4,
  p.rush_yds_per_game_l4,
  p.carries_per_game_l2,
  p.snap_pct_std,
  p.snap_pct_l4,

  -- Receiving role
  p.targets_per_game_std,
  p.rec_per_game_std,
  p.rec_yds_per_game_std,
  p.target_share_std,

  -- NGS Rushing
  ngs.rush_efficiency,
  ngs.stacked_box_pct,
  ngs.ryoe,
  ngs.ryoe_per_att,
  ngs.avg_time_to_los,

  -- Team game script
  p.team_pass_rate_std,
  p.team_pts_for_std,
  p.team_epa_blended,

  -- Opponent run defense
  p.opp_def_rush_epa_std,
  p.opp_lb_burden,
  p.opp_dl_burden,

  -- Injury
  p.injury_status,
  p.injury_out_flag,

  -- Targets
  p.actual_carries,
  p.actual_rush_yards,
  p.actual_rush_tds,
  p.actual_targets,
  p.actual_rec_yards,
  p.actual_fpts_std,
  p.actual_fpts_ppr

FROM mart_player_week_projection p
LEFT JOIN (
  SELECT
    player_id,
    season,
    week,
    AVG(rush_efficiency) OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS rush_efficiency,
    AVG(stacked_box_pct) OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS stacked_box_pct,
    AVG(ryoe)            OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS ryoe,
    AVG(ryoe_per_att)    OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS ryoe_per_att,
    AVG(avg_time_to_los) OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_time_to_los
  FROM stg_nextgen_player_week
  WHERE stat_type = 'rushing'
    AND season_type = 'REG'
) ngs ON p.player_id = ngs.player_id
      AND p.season   = ngs.season
      AND p.week     = ngs.week

WHERE p.position_group = 'RB'
;


-- =============================================================================
-- MART: mart_backtest_game
-- Grain:   game_id
-- Purpose: Leakage-safe historical training set.
--          Identical to mart_game_modeling; documented separately to
--          enforce the leakage contract and training/test splits.
-- =============================================================================

CREATE OR REPLACE VIEW mart_backtest_game AS
SELECT
  *,
  -- Suggested train/test year splits
  CASE
    WHEN season < 2018 THEN 'train'
    WHEN season BETWEEN 2018 AND 2021 THEN 'validation'
    ELSE 'test'
  END AS cv_split
FROM mart_game_modeling
WHERE game_completed_flag = 1
  AND season_type = 'regular'
  -- Require at minimum 3 games played for both teams (form stabilisation)
  AND home_games_played >= 1
  AND away_games_played >= 1
;


-- =============================================================================
-- MART: mart_backtest_player
-- Grain:   season + week + player_id
-- Purpose: Leakage-safe player training set.
-- =============================================================================

CREATE OR REPLACE VIEW mart_backtest_player AS
SELECT
  p.*,
  CASE
    WHEN p.season < 2018 THEN 'train'
    WHEN p.season BETWEEN 2018 AND 2021 THEN 'validation'
    ELSE 'test'
  END AS cv_split
FROM mart_player_week_projection p
WHERE p.actual_targets IS NOT NULL   -- game has been played
   OR p.actual_carries IS NOT NULL
   OR p.actual_attempts IS NOT NULL
;
