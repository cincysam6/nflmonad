-- =============================================================================
-- mart_player_week_projection
-- All types normalized in CTEs to prevent VARCHAR/DOUBLE BINDER errors.
-- raw_nextgen_stats only has receiving columns - no passing/rushing NGS.
-- =============================================================================

CREATE OR REPLACE VIEW mart_player_week_projection AS
WITH pf AS (
  SELECT
    CAST(player_id          AS VARCHAR)  AS pid,
    CAST(full_name          AS VARCHAR)  AS full_name,
    CAST(position           AS VARCHAR)  AS position,
    CAST(position_group     AS VARCHAR)  AS position_group,
    CAST(team               AS VARCHAR)  AS team,
    CAST(season             AS INTEGER)  AS season,
    CAST(week               AS INTEGER)  AS week,
    CAST(targets_per_game_std    AS DOUBLE) AS targets_per_game_std,
    CAST(rec_per_game_std        AS DOUBLE) AS rec_per_game_std,
    CAST(rec_yds_per_game_std    AS DOUBLE) AS rec_yds_per_game_std,
    CAST(rec_tds_per_game_std    AS DOUBLE) AS rec_tds_per_game_std,
    CAST(target_share_std        AS DOUBLE) AS target_share_std,
    CAST(air_yards_share_std     AS DOUBLE) AS air_yards_share_std,
    CAST(wopr_std                AS DOUBLE) AS wopr_std,
    CAST(snap_pct_std            AS DOUBLE) AS snap_pct_std,
    CAST(carries_per_game_std    AS DOUBLE) AS carries_per_game_std,
    CAST(rush_yds_per_game_std   AS DOUBLE) AS rush_yds_per_game_std,
    CAST(rush_tds_per_game_std   AS DOUBLE) AS rush_tds_per_game_std,
    CAST(attempts_per_game_std   AS DOUBLE) AS attempts_per_game_std,
    CAST(pass_yds_per_game_std   AS DOUBLE) AS pass_yds_per_game_std,
    CAST(pass_tds_per_game_std   AS DOUBLE) AS pass_tds_per_game_std,
    CAST(rec_epa_per_game_std    AS DOUBLE) AS rec_epa_per_game_std,
    CAST(rush_epa_per_game_std   AS DOUBLE) AS rush_epa_per_game_std,
    CAST(pass_epa_per_game_std   AS DOUBLE) AS pass_epa_per_game_std,
    CAST(games_played_std        AS DOUBLE) AS games_played_std,
    CAST(targets_per_game_l4     AS DOUBLE) AS targets_per_game_l4,
    CAST(rec_yds_per_game_l4     AS DOUBLE) AS rec_yds_per_game_l4,
    CAST(target_share_l4         AS DOUBLE) AS target_share_l4,
    CAST(snap_pct_l4             AS DOUBLE) AS snap_pct_l4,
    CAST(carries_per_game_l4     AS DOUBLE) AS carries_per_game_l4,
    CAST(rush_yds_per_game_l4    AS DOUBLE) AS rush_yds_per_game_l4,
    CAST(attempts_per_game_l4    AS DOUBLE) AS attempts_per_game_l4,
    CAST(pass_yds_per_game_l4    AS DOUBLE) AS pass_yds_per_game_l4,
    CAST(targets_per_game_l2     AS DOUBLE) AS targets_per_game_l2,
    CAST(rec_yds_per_game_l2     AS DOUBLE) AS rec_yds_per_game_l2,
    CAST(carries_per_game_l2     AS DOUBLE) AS carries_per_game_l2
  FROM int_player_form
),
tw AS (
  SELECT
    CAST(game_id   AS VARCHAR)  AS game_id,
    CAST(team      AS VARCHAR)  AS team,
    CAST(opponent  AS VARCHAR)  AS opponent,
    CAST(season    AS INTEGER)  AS season,
    CAST(week      AS INTEGER)  AS week,
    CAST(home_flag AS INTEGER)  AS home_flag
  FROM stg_team_week
),
pg AS (
  SELECT
    CAST(player_id          AS VARCHAR)  AS pid,
    CAST(season             AS INTEGER)  AS season,
    CAST(week               AS INTEGER)  AS week,
    CAST(targets            AS DOUBLE)   AS targets,
    CAST(receptions         AS DOUBLE)   AS receptions,
    CAST(receiving_yards    AS DOUBLE)   AS receiving_yards,
    CAST(receiving_tds      AS DOUBLE)   AS receiving_tds,
    CAST(carries            AS DOUBLE)   AS carries,
    CAST(rushing_yards      AS DOUBLE)   AS rushing_yards,
    CAST(rushing_tds        AS DOUBLE)   AS rushing_tds,
    CAST(attempts           AS DOUBLE)   AS attempts,
    CAST(passing_yards      AS DOUBLE)   AS passing_yards,
    CAST(passing_tds        AS DOUBLE)   AS passing_tds,
    CAST(fantasy_points_std AS DOUBLE)   AS fantasy_points_std,
    CAST(fantasy_points_ppr AS DOUBLE)   AS fantasy_points_ppr
  FROM int_player_game
),
inj AS (
  SELECT
    CAST(gsis_id  AS VARCHAR) AS pid,
    CAST(season   AS INTEGER) AS season,
    CAST(week     AS INTEGER) AS week,
    report_status,
    CASE UPPER(report_status)
      WHEN 'OUT'          THEN 4
      WHEN 'DOUBTFUL'     THEN 3
      WHEN 'QUESTIONABLE' THEN 2
      WHEN 'LIMITED'      THEN 1
      ELSE 0
    END AS injury_severity_score,
    ROW_NUMBER() OVER (
      PARTITION BY gsis_id, season, week
      ORDER BY date_modified DESC
    ) AS rn
  FROM raw_injuries
  WHERE gsis_id IS NOT NULL
)
SELECT
  pf.pid AS player_id,
  pf.full_name,
  pf.position,
  pf.position_group,
  pf.team,
  pf.season,
  pf.week,
  tw.opponent,
  tw.home_flag,
  tw.game_id,
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
  pf.targets_per_game_l4,
  pf.rec_yds_per_game_l4,
  pf.target_share_l4,
  pf.snap_pct_l4,
  pf.carries_per_game_l4,
  pf.rush_yds_per_game_l4,
  pf.attempts_per_game_l4,
  pf.pass_yds_per_game_l4,
  pf.targets_per_game_l2,
  pf.rec_yds_per_game_l2,
  pf.carries_per_game_l2,
  tmf.epa_pp_blended               AS team_epa_blended,
  tmf.pass_epa_std                 AS team_pass_epa_std,
  tmf.rush_epa_std                 AS team_rush_epa_std,
  tmf.pass_rate_std                AS team_pass_rate_std,
  tmf.pts_for_std                  AS team_pts_for_std,
  opp_tmf.def_epa_pp_blended           AS opp_def_epa_blended,
  opp_tmf.def_pass_epa_std             AS opp_def_pass_epa_std,
  opp_tmf.def_rush_epa_std             AS opp_def_rush_epa_std,
  opp_tmf.def_success_rate_allowed_std AS opp_def_success_rate,
  opp_tmf.def_sacks_std                AS opp_def_sacks_std,
  opp_inj.cb_burden                    AS opp_cb_burden,
  opp_inj.dl_burden                    AS opp_dl_burden,
  opp_inj.lb_burden                    AS opp_lb_burden,
  inj.report_status                    AS injury_status,
  inj.injury_severity_score,
  CASE WHEN inj.report_status IN ('OUT','DOUBTFUL') THEN 1 ELSE 0 END AS injury_out_flag,
  pg.targets                       AS actual_targets,
  pg.receptions                    AS actual_receptions,
  pg.receiving_yards               AS actual_rec_yards,
  pg.receiving_tds                 AS actual_rec_tds,
  pg.carries                       AS actual_carries,
  pg.rushing_yards                 AS actual_rush_yards,
  pg.rushing_tds                   AS actual_rush_tds,
  pg.attempts                      AS actual_pass_attempts,
  pg.passing_yards                 AS actual_pass_yards,
  pg.passing_tds                   AS actual_pass_tds,
  pg.fantasy_points_std            AS actual_fpts_std,
  pg.fantasy_points_ppr            AS actual_fpts_ppr
FROM pf
LEFT JOIN tw
  ON  pf.team   = tw.team
 AND  pf.season = tw.season
 AND  pf.week   = tw.week
LEFT JOIN mart_team_week_modeling tmf
  ON  pf.team   = tmf.team
 AND  pf.season = tmf.season
 AND  pf.week   = tmf.week
LEFT JOIN mart_team_week_modeling opp_tmf
  ON  tw.opponent = opp_tmf.team
 AND  pf.season   = opp_tmf.season
 AND  pf.week     = opp_tmf.week
LEFT JOIN int_injury_team_impact opp_inj
  ON  tw.opponent = opp_inj.team
 AND  pf.season   = opp_inj.season
 AND  pf.week     = opp_inj.week
LEFT JOIN inj
  ON  pf.pid       = inj.pid
 AND  pf.season    = inj.season
 AND  pf.week      = inj.week
 AND  inj.rn       = 1
LEFT JOIN pg
  ON  pf.pid       = pg.pid
 AND  pf.season    = pg.season
 AND  pf.week      = pg.week
;


-- =============================================================================
-- mart_qb_projection
-- NGS: only avg_intended_air_yards confirmed in raw_nextgen_stats
-- =============================================================================

CREATE OR REPLACE VIEW mart_qb_projection AS
WITH qc AS (
  SELECT
    CAST(player_id  AS VARCHAR) AS pid,
    CAST(season     AS INTEGER) AS season,
    CAST(week       AS INTEGER) AS week,
    CAST(game_id    AS VARCHAR) AS game_id,
    CAST(team       AS VARCHAR) AS team,
    CAST(opponent   AS VARCHAR) AS opponent,
    CAST(home_flag  AS INTEGER) AS home_flag,
    CAST(game_date  AS DATE)    AS game_date,
    CAST(full_name  AS VARCHAR) AS full_name,
    pass_rate, team_pass_epa, oline_sacks_allowed,
    opp_def_sacks, opp_def_pass_epa, skill_position_injury_burden,
    wr_out, te_out,
    completions, attempts, passing_yards, passing_tds,
    interceptions, sacks, passing_epa, dakota,
    qb_carries, qb_rush_yards, qb_rush_tds
  FROM int_qb_team_context
),
pf AS (
  SELECT
    CAST(player_id           AS VARCHAR) AS pid,
    CAST(season              AS INTEGER) AS season,
    CAST(week                AS INTEGER) AS week,
    CAST(attempts_per_game_std   AS DOUBLE) AS attempts_per_game_std,
    CAST(pass_yds_per_game_std   AS DOUBLE) AS pass_yds_per_game_std,
    CAST(pass_tds_per_game_std   AS DOUBLE) AS pass_tds_per_game_std,
    CAST(pass_epa_per_game_std   AS DOUBLE) AS pass_epa_per_game_std,
    CAST(rush_yds_per_game_std   AS DOUBLE) AS rush_yds_per_game_std,
    CAST(attempts_per_game_l4    AS DOUBLE) AS attempts_per_game_l4,
    CAST(pass_yds_per_game_l4    AS DOUBLE) AS pass_yds_per_game_l4,
    CAST(games_played_std        AS DOUBLE) AS games_played_std
  FROM int_player_form
),
ngs AS (
  SELECT
    b.pid,
    b.season,
    b.week,
    AVG(b.avg_intended_air_yards) OVER (
      PARTITION BY b.pid, b.season ORDER BY b.week
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS avg_intended_air_yards
  FROM (
    SELECT
      CAST(player_id AS VARCHAR) AS pid,
      CAST(season    AS INTEGER) AS season,
      CAST(week      AS INTEGER) AS week,
      CAST(avg_intended_air_yards AS DOUBLE) AS avg_intended_air_yards
    FROM stg_nextgen_player_week
    WHERE season_type = 'REG'
  ) b
)
SELECT
  qc.game_id, qc.pid AS player_id, qc.full_name, qc.team, qc.opponent,
  qc.season, qc.week, qc.home_flag, qc.game_date,
  pf.attempts_per_game_std, pf.pass_yds_per_game_std,
  pf.pass_tds_per_game_std, pf.pass_epa_per_game_std,
  pf.rush_yds_per_game_std, pf.attempts_per_game_l4,
  pf.pass_yds_per_game_l4, pf.games_played_std,
  qc.pass_rate, qc.team_pass_epa, qc.oline_sacks_allowed,
  qc.opp_def_sacks, qc.opp_def_pass_epa,
  qc.skill_position_injury_burden, qc.wr_out, qc.te_out,
  ngs.avg_intended_air_yards       AS ngs_intended_air_yards_std,
  qc.completions,
  qc.attempts                      AS actual_pass_attempts,
  qc.passing_yards, qc.passing_tds, qc.interceptions,
  qc.sacks, qc.passing_epa, qc.dakota,
  qc.qb_carries, qc.qb_rush_yards, qc.qb_rush_tds
FROM qc
LEFT JOIN pf
  ON  qc.pid       = pf.pid
 AND  qc.season    = pf.season
 AND  qc.week      = pf.week
LEFT JOIN ngs
  ON  qc.pid       = ngs.pid
 AND  qc.season    = ngs.season
 AND  qc.week      = ngs.week
;


CREATE OR REPLACE VIEW mart_receiver_projection AS
WITH p AS (
  SELECT
    CAST(player_id      AS VARCHAR) AS pid,
    CAST(season         AS INTEGER) AS season,
    CAST(week           AS INTEGER) AS week,
    CAST(position_group AS VARCHAR) AS position_group,
    CAST(game_id        AS VARCHAR) AS game_id,
    full_name, position, team, opponent, home_flag,
    targets_per_game_std, rec_per_game_std, rec_yds_per_game_std,
    rec_tds_per_game_std, target_share_std, air_yards_share_std,
    wopr_std, snap_pct_std, targets_per_game_l4, target_share_l4,
    targets_per_game_l2, rec_epa_per_game_std,
    team_epa_blended, team_pass_rate_std, team_pts_for_std,
    opp_def_epa_blended, opp_def_pass_epa_std,
    opp_cb_burden, opp_dl_burden,
    injury_status, injury_out_flag,
    actual_targets, actual_receptions, actual_rec_yards,
    actual_rec_tds, actual_fpts_std, actual_fpts_ppr
  FROM mart_player_week_projection
),
ngs AS (
  SELECT
    b.pid,
    b.season,
    b.week,
    AVG(b.avg_separation)            OVER w AS avg_separation,
    AVG(b.avg_cushion)               OVER w AS avg_cushion,
    AVG(b.catch_percentage)          OVER w AS catch_percentage,
    AVG(b.avg_yac)                   OVER w AS avg_yac,
    AVG(b.avg_yac_above_expectation) OVER w AS avg_yac_above_expectation
  FROM (
    SELECT
      CAST(player_id AS VARCHAR) AS pid,
      CAST(season    AS INTEGER) AS season,
      CAST(week      AS INTEGER) AS week,
      CAST(avg_separation            AS DOUBLE) AS avg_separation,
      CAST(avg_cushion               AS DOUBLE) AS avg_cushion,
      CAST(catch_percentage          AS DOUBLE) AS catch_percentage,
      CAST(avg_yac                   AS DOUBLE) AS avg_yac,
      CAST(avg_yac_above_expectation AS DOUBLE) AS avg_yac_above_expectation
    FROM stg_nextgen_player_week
    WHERE season_type = 'REG'
  ) b
  WINDOW w AS (
    PARTITION BY b.pid, b.season ORDER BY b.week
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
  )
)
SELECT
  p.game_id, p.pid AS player_id, p.full_name, p.position,
  p.team, p.opponent, p.season, p.week, p.home_flag,
  p.targets_per_game_std, p.rec_per_game_std, p.rec_yds_per_game_std,
  p.rec_tds_per_game_std, p.target_share_std, p.air_yards_share_std,
  p.wopr_std, p.snap_pct_std,
  p.targets_per_game_l4, p.target_share_l4, p.targets_per_game_l2,
  p.rec_epa_per_game_std,
  ngs.avg_separation, ngs.avg_cushion, ngs.catch_percentage,
  ngs.avg_yac, ngs.avg_yac_above_expectation,
  p.team_epa_blended, p.team_pass_rate_std, p.team_pts_for_std,
  p.opp_def_epa_blended, p.opp_def_pass_epa_std,
  p.opp_cb_burden, p.opp_dl_burden,
  p.injury_status, p.injury_out_flag,
  p.actual_targets, p.actual_receptions, p.actual_rec_yards,
  p.actual_rec_tds, p.actual_fpts_std, p.actual_fpts_ppr
FROM p
LEFT JOIN ngs
  ON  p.pid       = ngs.pid
 AND  p.season    = ngs.season
 AND  p.week      = ngs.week
WHERE p.position_group IN ('WR','TE')
;

CREATE OR REPLACE VIEW mart_rusher_projection AS
WITH p AS (
  SELECT
    CAST(player_id      AS VARCHAR) AS pid,
    CAST(season         AS INTEGER) AS season,
    CAST(week           AS INTEGER) AS week,
    CAST(position_group AS VARCHAR) AS position_group,
    CAST(game_id        AS VARCHAR) AS game_id,
    full_name, position, team, opponent, home_flag,
    carries_per_game_std, rush_yds_per_game_std, rush_tds_per_game_std,
    rush_epa_per_game_std, carries_per_game_l4, rush_yds_per_game_l4,
    carries_per_game_l2, snap_pct_std, snap_pct_l4,
    targets_per_game_std, rec_per_game_std, rec_yds_per_game_std,
    target_share_std,
    team_pass_rate_std, team_pts_for_std, team_epa_blended,
    opp_def_rush_epa_std, opp_lb_burden, opp_dl_burden,
    injury_status, injury_out_flag,
    actual_carries, actual_rush_yards, actual_rush_tds,
    actual_targets, actual_rec_yards,
    actual_fpts_std, actual_fpts_ppr
  FROM mart_player_week_projection
)
SELECT
  p.game_id, p.pid AS player_id, p.full_name, p.position,
  p.team, p.opponent, p.season, p.week, p.home_flag,
  p.carries_per_game_std, p.rush_yds_per_game_std, p.rush_tds_per_game_std,
  p.rush_epa_per_game_std, p.carries_per_game_l4, p.rush_yds_per_game_l4,
  p.carries_per_game_l2, p.snap_pct_std, p.snap_pct_l4,
  p.targets_per_game_std, p.rec_per_game_std, p.rec_yds_per_game_std,
  p.target_share_std,
  NULL::DOUBLE AS rush_efficiency,
  NULL::DOUBLE AS stacked_box_pct,
  NULL::DOUBLE AS ryoe,
  NULL::DOUBLE AS ryoe_per_att,
  NULL::DOUBLE AS avg_time_to_los,
  p.team_pass_rate_std, p.team_pts_for_std, p.team_epa_blended,
  p.opp_def_rush_epa_std, p.opp_lb_burden, p.opp_dl_burden,
  p.injury_status, p.injury_out_flag,
  p.actual_carries, p.actual_rush_yards, p.actual_rush_tds,
  p.actual_targets, p.actual_rec_yards,
  p.actual_fpts_std, p.actual_fpts_ppr
FROM p
WHERE p.position_group = 'RB'
;

-- =============================================================================
-- mart_backtest_player
-- =============================================================================

CREATE OR REPLACE VIEW mart_backtest_player AS
SELECT
  p.*,
  CASE
    WHEN p.season < 2015                THEN 'train'
    WHEN p.season BETWEEN 2015 AND 2019 THEN 'validation'
    ELSE 'test'
  END AS cv_split
FROM mart_player_week_projection p
WHERE p.actual_targets       IS NOT NULL
   OR p.actual_carries       IS NOT NULL
   OR p.actual_pass_attempts IS NOT NULL
;
