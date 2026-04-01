-- =============================================================================
-- INTERMEDIATE: int_team_form
-- Grain:   season + week + team
-- Purpose: Pre-game rolling team strength features.
--          ALL metrics are lagged — only data from games BEFORE this week.
--          Use for modeling and joining to game rows.
-- Rolling windows: last 3, last 5, season-to-date, prior season (full)
-- =============================================================================

CREATE OR REPLACE VIEW int_team_form AS
WITH ranked_games AS (
  SELECT
    tg.*,
    ROW_NUMBER() OVER (PARTITION BY team, season ORDER BY week) AS game_num_in_season
  FROM int_team_game tg
  WHERE season_type = 'regular'
),

-- ---- Trailing averages using frame-based window functions ------------------
-- We compute rolling stats EXCLUDING the current game (rows BETWEEN
-- UNBOUNDED PRECEDING AND 1 PRECEDING) to prevent leakage.

rolling AS (
  SELECT
    team,
    season,
    week,
    game_id,
    game_num_in_season,
    game_date,
    points_for,
    points_against,
    margin,
    win_flag,

    -- ===== SEASON-TO-DATE (all prior games this season) =====
    AVG(epa_per_play) OVER w_std           AS epa_pp_std,
    AVG(pass_epa_per_dropback) OVER w_std  AS pass_epa_std,
    AVG(rush_epa_per_carry)   OVER w_std   AS rush_epa_std,
    AVG(def_epa_per_play)     OVER w_std   AS def_epa_pp_std,
    AVG(def_pass_epa_per_dropback) OVER w_std AS def_pass_epa_std,
    AVG(def_rush_epa_per_carry)    OVER w_std AS def_rush_epa_std,
    AVG(success_rate)         OVER w_std   AS success_rate_std,
    AVG(pass_success_rate)    OVER w_std   AS pass_success_rate_std,
    AVG(rush_success_rate)    OVER w_std   AS rush_success_rate_std,
    AVG(def_success_rate_allowed) OVER w_std AS def_success_rate_allowed_std,
    AVG(explosive_pass_rate)  OVER w_std   AS explosive_pass_rate_std,
    AVG(explosive_rush_rate)  OVER w_std   AS explosive_rush_rate_std,
    AVG(pass_rate)            OVER w_std   AS pass_rate_std,
    AVG(CAST(turnovers AS DOUBLE)) OVER w_std AS turnovers_per_game_std,
    AVG(CAST(def_turnovers_forced AS DOUBLE)) OVER w_std AS def_to_forced_std,
    AVG(CAST(sacks_allowed AS DOUBLE)) OVER w_std AS sacks_allowed_std,
    AVG(CAST(def_sacks AS DOUBLE)) OVER w_std AS def_sacks_std,
    AVG(red_zone_td_rate)     OVER w_std   AS rz_td_rate_std,
    AVG(CAST(points_for AS DOUBLE))   OVER w_std AS pts_for_std,
    AVG(CAST(points_against AS DOUBLE)) OVER w_std AS pts_against_std,
    COUNT(*) OVER w_std                    AS games_played_std,

    -- ===== LAST 5 GAMES =====
    AVG(epa_per_play)         OVER w_l5    AS epa_pp_l5,
    AVG(pass_epa_per_dropback) OVER w_l5   AS pass_epa_l5,
    AVG(rush_epa_per_carry)   OVER w_l5    AS rush_epa_l5,
    AVG(def_epa_per_play)     OVER w_l5    AS def_epa_pp_l5,
    AVG(success_rate)         OVER w_l5    AS success_rate_l5,
    AVG(def_success_rate_allowed) OVER w_l5 AS def_success_rate_l5,
    AVG(CAST(turnovers AS DOUBLE)) OVER w_l5 AS turnovers_l5,
    AVG(CAST(points_for AS DOUBLE)) OVER w_l5 AS pts_for_l5,
    AVG(CAST(points_against AS DOUBLE)) OVER w_l5 AS pts_against_l5,

    -- ===== LAST 3 GAMES =====
    AVG(epa_per_play)         OVER w_l3    AS epa_pp_l3,
    AVG(pass_epa_per_dropback) OVER w_l3   AS pass_epa_l3,
    AVG(def_epa_per_play)     OVER w_l3    AS def_epa_pp_l3,
    AVG(CAST(points_for AS DOUBLE)) OVER w_l3 AS pts_for_l3,
    AVG(CAST(points_against AS DOUBLE)) OVER w_l3 AS pts_against_l3,

    -- ===== WIN STREAK =====
    SUM(win_flag) OVER w_l5               AS wins_l5

  FROM ranked_games

  WINDOW
    -- All prior games in the current season (lagged: PRECEDING only)
    w_std AS (
      PARTITION BY team, season
      ORDER BY week
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ),
    w_l5 AS (
      PARTITION BY team, season
      ORDER BY week
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ),
    w_l3 AS (
      PARTITION BY team, season
      ORDER BY week
      ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
    )
),

-- Pull prior full season averages for cross-season weighting
prior_season_agg AS (
  SELECT
    team,
    season,
    AVG(epa_per_play)            AS prior_epa_pp,
    AVG(def_epa_per_play)        AS prior_def_epa_pp,
    AVG(success_rate)            AS prior_success_rate,
    AVG(pass_rate)               AS prior_pass_rate,
    AVG(CAST(points_for AS DOUBLE)) AS prior_pts_for,
    AVG(CAST(points_against AS DOUBLE)) AS prior_pts_against
  FROM int_team_game
  WHERE season_type = 'regular'
  GROUP BY team, season
)

SELECT
  r.*,
  -- Blend current STD with prior season (weight 0.3 prior, 0.7 current)
  CASE
    WHEN r.games_played_std >= 3
    THEN 0.7 * COALESCE(r.epa_pp_std, 0) + 0.3 * COALESCE(ps.prior_epa_pp, 0)
    ELSE COALESCE(ps.prior_epa_pp, 0)
  END AS epa_pp_blended,
  CASE
    WHEN r.games_played_std >= 3
    THEN 0.7 * COALESCE(r.def_epa_pp_std, 0) + 0.3 * COALESCE(ps.prior_def_epa_pp, 0)
    ELSE COALESCE(ps.prior_def_epa_pp, 0)
  END AS def_epa_pp_blended,
  ps.prior_epa_pp,
  ps.prior_def_epa_pp,
  ps.prior_success_rate,
  ps.prior_pass_rate

FROM rolling r
LEFT JOIN prior_season_agg ps
  ON r.team = ps.team
  AND r.season = ps.season + 1  -- previous season
;


-- =============================================================================
-- INTERMEDIATE: int_injury_team_impact
-- Grain:   season + week + team
-- Purpose: Injury burden summary by position group, available pre-game.
--          Lagged: uses injury reports filed BEFORE the game.
-- =============================================================================

CREATE OR REPLACE VIEW int_injury_team_impact AS
WITH inj_scored AS (
  SELECT
    team,
    season,
    week,
    -- Map position to group
    CASE UPPER(position)
      WHEN 'QB'  THEN 'QB'
      WHEN 'RB'  THEN 'RB' WHEN 'HB' THEN 'RB' WHEN 'FB' THEN 'RB'
      WHEN 'WR'  THEN 'WR'
      WHEN 'TE'  THEN 'TE'
      WHEN 'C'   THEN 'OL' WHEN 'G'  THEN 'OL' WHEN 'T'  THEN 'OL'
      WHEN 'DE'  THEN 'DL' WHEN 'DT' THEN 'DL' WHEN 'NT' THEN 'DL'
      WHEN 'ILB' THEN 'LB' WHEN 'OLB' THEN 'LB' WHEN 'MLB' THEN 'LB'
      WHEN 'CB'  THEN 'CB'
      WHEN 'FS'  THEN 'S'  WHEN 'SS'  THEN 'S'  WHEN 'DB' THEN 'S'
      ELSE 'OTHER'
    END AS position_group,
    UPPER(report_status) AS report_status,
    injury_severity_score
  FROM stg_injuries_weekly
  WHERE UPPER(report_status) != 'FULL'  -- only players with some limitation
),
grouped AS (
  SELECT
    team,
    season,
    week,
    position_group,
    COUNT(CASE WHEN report_status = 'OUT'         THEN 1 END) AS starters_out,
    COUNT(CASE WHEN report_status = 'DOUBTFUL'    THEN 1 END) AS doubtful_count,
    COUNT(CASE WHEN report_status = 'QUESTIONABLE' THEN 1 END) AS questionable_count,
    SUM(injury_severity_score)                                 AS weighted_absence_score
  FROM inj_scored
  GROUP BY team, season, week, position_group
)

-- Pivot position groups into columns
SELECT
  team,
  season,
  week,

  SUM(weighted_absence_score)         AS total_injury_burden,

  -- QB
  MAX(CASE WHEN position_group='QB' THEN starters_out       ELSE 0 END) AS qb_out,
  MAX(CASE WHEN position_group='QB' THEN doubtful_count      ELSE 0 END) AS qb_doubtful,
  MAX(CASE WHEN position_group='QB' THEN questionable_count  ELSE 0 END) AS qb_questionable,
  MAX(CASE WHEN position_group='QB' THEN weighted_absence_score ELSE 0 END) AS qb_burden,

  -- OL
  MAX(CASE WHEN position_group='OL' THEN starters_out       ELSE 0 END) AS ol_out,
  MAX(CASE WHEN position_group='OL' THEN weighted_absence_score ELSE 0 END) AS ol_burden,

  -- WR
  MAX(CASE WHEN position_group='WR' THEN starters_out       ELSE 0 END) AS wr_out,
  MAX(CASE WHEN position_group='WR' THEN doubtful_count      ELSE 0 END) AS wr_doubtful,
  MAX(CASE WHEN position_group='WR' THEN questionable_count  ELSE 0 END) AS wr_questionable,
  MAX(CASE WHEN position_group='WR' THEN weighted_absence_score ELSE 0 END) AS wr_burden,

  -- TE
  MAX(CASE WHEN position_group='TE' THEN starters_out       ELSE 0 END) AS te_out,
  MAX(CASE WHEN position_group='TE' THEN weighted_absence_score ELSE 0 END) AS te_burden,

  -- RB
  MAX(CASE WHEN position_group='RB' THEN starters_out       ELSE 0 END) AS rb_out,
  MAX(CASE WHEN position_group='RB' THEN weighted_absence_score ELSE 0 END) AS rb_burden,

  -- DL
  MAX(CASE WHEN position_group='DL' THEN starters_out       ELSE 0 END) AS dl_out,
  MAX(CASE WHEN position_group='DL' THEN weighted_absence_score ELSE 0 END) AS dl_burden,

  -- LB
  MAX(CASE WHEN position_group='LB' THEN starters_out       ELSE 0 END) AS lb_out,
  MAX(CASE WHEN position_group='LB' THEN weighted_absence_score ELSE 0 END) AS lb_burden,

  -- CB
  MAX(CASE WHEN position_group='CB' THEN starters_out       ELSE 0 END) AS cb_out,
  MAX(CASE WHEN position_group='CB' THEN weighted_absence_score ELSE 0 END) AS cb_burden,

  -- S
  MAX(CASE WHEN position_group='S'  THEN starters_out       ELSE 0 END) AS s_out,
  MAX(CASE WHEN position_group='S'  THEN weighted_absence_score ELSE 0 END) AS s_burden

FROM grouped
GROUP BY team, season, week
;
