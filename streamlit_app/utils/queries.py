"""SQL query helpers for the NFL player dashboard."""

from __future__ import annotations
import pandas as pd
import streamlit as st
from .db import query


@st.cache_data(ttl=600)
def get_all_players(positions: list[str] | None = None) -> pd.DataFrame:
    pos_filter = ""
    if positions:
        quoted = ", ".join(f"'{p}'" for p in positions)
        pos_filter = f"AND pw.position IN ({quoted})"

    sql = f"""
    SELECT
        pw.player_id,
        pw.full_name,
        pw.position,
        pl.headshot_url,
        pl.height,
        pl.weight,
        pl.entry_year,
        pl.college,
        pl.active_flag,
        MAX(pw.season)  AS latest_season,
        MIN(pw.season)  AS first_season,
        MAX(pw.team)    AS latest_team,
        SUM(pw.fantasy_points_ppr) AS career_fpts_ppr
    FROM stg_player_week pw
    LEFT JOIN stg_players pl ON pw.player_id = pl.player_id
    WHERE 1=1 {pos_filter}
    GROUP BY pw.player_id, pw.full_name, pw.position,
             pl.headshot_url, pl.height, pl.weight,
             pl.entry_year, pl.college, pl.active_flag
    HAVING SUM(pw.fantasy_points_ppr) > 50
    ORDER BY pw.full_name
    """
    return query(sql)


@st.cache_data(ttl=600)
def get_career_stats(player_id: str, position: str) -> pd.DataFrame:
    """Season-by-season totals for a player."""
    sql = """
    SELECT
        season,
        season_type,
        team,
        -- games
        COUNT(DISTINCT week) AS games,
        -- Passing
        SUM(completions)    AS completions,
        SUM(attempts)       AS attempts,
        SUM(passing_yards)  AS passing_yards,
        SUM(passing_tds)    AS passing_tds,
        SUM(interceptions)  AS interceptions,
        SUM(sacks)          AS sacks,
        ROUND(SUM(passing_yards)::DOUBLE / NULLIF(SUM(attempts), 0), 1) AS ypa,
        ROUND(SUM(completions)::DOUBLE  / NULLIF(SUM(attempts), 0) * 100, 1) AS comp_pct,
        ROUND(SUM(passing_epa), 1)      AS passing_epa,
        ROUND(AVG(dakota), 2)           AS cpoe,
        -- Rushing
        SUM(carries)        AS carries,
        SUM(rushing_yards)  AS rushing_yards,
        SUM(rushing_tds)    AS rushing_tds,
        ROUND(SUM(rushing_yards)::DOUBLE / NULLIF(SUM(carries), 0), 1) AS ypc,
        ROUND(SUM(rushing_epa), 1)      AS rushing_epa,
        -- Receiving
        SUM(targets)        AS targets,
        SUM(receptions)     AS receptions,
        SUM(receiving_yards)AS receiving_yards,
        SUM(receiving_tds)  AS receiving_tds,
        ROUND(SUM(receptions)::DOUBLE  / NULLIF(SUM(targets), 0) * 100, 1) AS catch_rate,
        ROUND(SUM(receiving_yards)::DOUBLE / NULLIF(SUM(targets), 0), 1)   AS ypt,
        ROUND(SUM(receiving_epa), 1)    AS receiving_epa,
        ROUND(AVG(target_share) * 100, 1) AS tgt_share_pct,
        ROUND(AVG(wopr), 3)             AS wopr,
        -- Fantasy
        ROUND(SUM(fantasy_points_std), 1) AS fpts_std,
        ROUND(SUM(fantasy_points_ppr), 1) AS fpts_ppr,
        ROUND(SUM(fantasy_points_ppr)::DOUBLE / NULLIF(COUNT(DISTINCT week), 0), 1) AS fpts_ppr_per_game
    FROM stg_player_week
    WHERE player_id = ?
    GROUP BY season, season_type, team
    ORDER BY season, season_type
    """
    return query(sql, player_id=player_id)


@st.cache_data(ttl=600)
def get_weekly_stats(player_id: str, season: int) -> pd.DataFrame:
    """Week-by-week stats for a specific season."""
    sql = """
    SELECT
        week,
        season_type,
        team,
        opponent,
        completions,
        attempts,
        passing_yards,
        passing_tds,
        interceptions,
        sacks,
        ROUND(passing_epa, 2) AS passing_epa,
        ROUND(dakota, 2)      AS cpoe,
        carries,
        rushing_yards,
        rushing_tds,
        ROUND(rushing_epa, 2) AS rushing_epa,
        targets,
        receptions,
        receiving_yards,
        receiving_tds,
        ROUND(receiving_epa, 2) AS receiving_epa,
        ROUND(target_share * 100, 1) AS tgt_share_pct,
        ROUND(air_yards_share * 100, 1) AS air_yds_share_pct,
        ROUND(wopr, 3)        AS wopr,
        ROUND(fantasy_points_std, 1) AS fpts_std,
        ROUND(fantasy_points_ppr, 1) AS fpts_ppr
    FROM stg_player_week
    WHERE player_id = ? AND season = ?
    ORDER BY week
    """
    return query(sql, player_id=player_id, season=season)


@st.cache_data(ttl=600)
def get_ngs_stats(player_id: str, season: int) -> pd.DataFrame:
    """Next Gen Stats (receiving) for a player/season."""
    sql = """
    SELECT
        week,
        team,
        targets,
        receptions,
        yards,
        ROUND(avg_separation, 1)         AS separation,
        ROUND(avg_cushion, 1)            AS cushion,
        ROUND(avg_intended_air_yards, 1) AS intended_adot,
        ROUND(avg_yac, 1)                AS avg_yac,
        ROUND(avg_yac_above_expectation, 1) AS yac_oe,
        ROUND(catch_percentage, 1)       AS catch_pct
    FROM stg_nextgen_player_week
    WHERE player_id = ? AND season = ? AND stat_type = 'receiving'
    ORDER BY week
    """
    return query(sql, player_id=player_id, season=season)


@st.cache_data(ttl=600)
def get_season_ngs_avg(player_id: str, season: int) -> pd.DataFrame:
    """Aggregated NGS averages for a player/season."""
    sql = """
    SELECT
        COUNT(DISTINCT week) AS weeks,
        SUM(targets)    AS targets,
        SUM(receptions) AS receptions,
        SUM(yards)      AS yards,
        ROUND(AVG(avg_separation), 2)         AS avg_separation,
        ROUND(AVG(avg_cushion), 2)            AS avg_cushion,
        ROUND(AVG(avg_intended_air_yards), 2) AS avg_adot,
        ROUND(AVG(avg_yac), 2)                AS avg_yac,
        ROUND(AVG(avg_yac_above_expectation), 2) AS avg_yac_oe,
        ROUND(AVG(catch_percentage), 1)       AS catch_pct
    FROM stg_nextgen_player_week
    WHERE player_id = ? AND season = ? AND stat_type = 'receiving'
    """
    return query(sql, player_id=player_id, season=season)


@st.cache_data(ttl=600)
def get_player_seasons(player_id: str) -> list[int]:
    sql = """
    SELECT DISTINCT season FROM stg_player_week
    WHERE player_id = ?
    ORDER BY season DESC
    """
    df = query(sql, player_id=player_id)
    return df["season"].tolist()
