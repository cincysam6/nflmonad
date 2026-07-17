"""NFL Fantasy Player Dashboard — ESPN-style Streamlit app."""

from __future__ import annotations
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

from utils.db import get_connection
from utils.queries import (
    get_all_players,
    get_career_stats,
    get_weekly_stats,
    get_ngs_stats,
    get_season_ngs_avg,
    get_player_seasons,
)
from utils.team_colors import get_team_colors, get_team_logo_url

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NFL Player Stats",
    page_icon="🏈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown(
    """
<style>
/* ── Global ── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
}

/* Hide default streamlit chrome */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
.stDeployButton {display: none;}

/* ── Background ── */
.stApp {
    background-color: #0a0e1a;
}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0d1117 0%, #0a0e1a 100%);
    border-right: 1px solid #1e2a3a;
}
section[data-testid="stSidebar"] .block-container {
    padding-top: 2rem;
}

/* ── Sidebar labels ── */
.sidebar-brand {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 1.5rem;
    padding: 0 0.5rem;
}
.sidebar-brand h1 {
    font-size: 1.2rem;
    font-weight: 800;
    color: #ffffff;
    margin: 0;
    letter-spacing: -0.5px;
}
.sidebar-brand span {
    font-size: 1.6rem;
}

/* ── Player hero card ── */
.player-hero {
    background: linear-gradient(135deg, #0d1117 0%, #1a2332 100%);
    border: 1px solid #1e2a3a;
    border-radius: 16px;
    padding: 28px;
    margin-bottom: 20px;
    position: relative;
    overflow: hidden;
}
.player-hero::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 4px;
    background: var(--team-primary, #013369);
    border-radius: 16px 16px 0 0;
}

.player-name {
    font-size: 2.4rem;
    font-weight: 900;
    color: #ffffff;
    line-height: 1.1;
    letter-spacing: -1px;
    margin: 0 0 4px 0;
}
.player-meta {
    font-size: 0.95rem;
    color: #8b9ab5;
    font-weight: 500;
    letter-spacing: 0.5px;
    text-transform: uppercase;
    margin-bottom: 16px;
}
.player-meta span {
    margin: 0 8px;
    color: #3d4f6e;
}

.position-badge {
    display: inline-block;
    background: var(--team-primary, #013369);
    color: #ffffff;
    font-size: 0.75rem;
    font-weight: 700;
    padding: 3px 10px;
    border-radius: 20px;
    letter-spacing: 1px;
    text-transform: uppercase;
}

/* ── Metric cards ── */
.metric-row {
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
    margin: 16px 0;
}
.metric-card {
    background: rgba(255,255,255,0.04);
    border: 1px solid #1e2a3a;
    border-radius: 10px;
    padding: 14px 18px;
    min-width: 110px;
    flex: 1;
}
.metric-label {
    font-size: 0.68rem;
    color: #6b7fa3;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    margin-bottom: 4px;
}
.metric-value {
    font-size: 1.6rem;
    font-weight: 800;
    color: #ffffff;
    line-height: 1;
}
.metric-sub {
    font-size: 0.72rem;
    color: #8b9ab5;
    margin-top: 2px;
}

/* ── Section headers ── */
.section-header {
    font-size: 1rem;
    font-weight: 700;
    color: #ffffff;
    text-transform: uppercase;
    letter-spacing: 1px;
    border-left: 3px solid var(--team-primary, #013369);
    padding-left: 10px;
    margin: 24px 0 12px 0;
}

/* ── Data tables ── */
.stDataFrame {
    border-radius: 10px;
    overflow: hidden;
    border: 1px solid #1e2a3a !important;
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    gap: 2px;
    background: transparent;
    border-bottom: 1px solid #1e2a3a;
}
.stTabs [data-baseweb="tab"] {
    background: transparent;
    border-radius: 8px 8px 0 0;
    color: #6b7fa3;
    font-weight: 600;
    font-size: 0.85rem;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    padding: 8px 16px;
    border: none;
}
.stTabs [aria-selected="true"] {
    background: rgba(255,255,255,0.06) !important;
    color: #ffffff !important;
    border-bottom: 2px solid var(--team-primary, #013369) !important;
}

/* ── Select boxes ── */
.stSelectbox label, .stMultiSelect label {
    color: #8b9ab5 !important;
    font-size: 0.8rem !important;
    font-weight: 600 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.5px !important;
}

/* ── Info boxes ── */
.stat-group-label {
    font-size: 0.72rem;
    font-weight: 700;
    color: #6b7fa3;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin: 20px 0 8px 0;
}

.no-player-state {
    text-align: center;
    padding: 80px 20px;
    color: #3d4f6e;
}
.no-player-state h2 {
    font-size: 2rem;
    color: #3d4f6e;
    margin-bottom: 8px;
}
.no-player-state p {
    font-size: 1rem;
    color: #2a3a52;
}

/* ── Advanced stat pill ── */
.ngs-pill {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: rgba(255,255,255,0.05);
    border: 1px solid #1e2a3a;
    border-radius: 8px;
    padding: 12px 16px;
    margin: 4px;
}
.ngs-label {
    font-size: 0.7rem;
    color: #6b7fa3;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}
.ngs-value {
    font-size: 1.3rem;
    font-weight: 800;
    color: #ffffff;
}

/* Override streamlit default bg for main content */
.main .block-container {
    background: transparent;
    padding-top: 2rem;
}
</style>
""",
    unsafe_allow_html=True,
)

PLOTLY_TEMPLATE = dict(
    layout=go.Layout(
        paper_bgcolor="#0a0e1a",
        plot_bgcolor="#0d1117",
        font=dict(color="#c9d1d9", family="Inter, sans-serif"),
        xaxis=dict(
            gridcolor="#1e2a3a",
            linecolor="#1e2a3a",
            tickfont=dict(color="#8b9ab5"),
        ),
        yaxis=dict(
            gridcolor="#1e2a3a",
            linecolor="#1e2a3a",
            tickfont=dict(color="#8b9ab5"),
        ),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(color="#c9d1d9"),
        ),
        margin=dict(t=40, b=40, l=10, r=10),
    )
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fmt(val, decimals=0, suffix=""):
    """Format a number nicely, handling NaN/None."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    if decimals == 0:
        return f"{int(val):,}{suffix}"
    return f"{val:,.{decimals}f}{suffix}"


def _col_config_base():
    return {
        "season": st.column_config.NumberColumn("Season", format="%d"),
        "games": st.column_config.NumberColumn("G"),
        "team": st.column_config.TextColumn("Team"),
        "fpts_ppr": st.column_config.NumberColumn("PPR Pts", format="%.1f"),
        "fpts_std": st.column_config.NumberColumn("Std Pts", format="%.1f"),
        "fpts_ppr_per_game": st.column_config.NumberColumn("PPR/G", format="%.1f"),
    }


def _styled_table(df: pd.DataFrame, col_config=None, height=None):
    kwargs = dict(use_container_width=True, hide_index=True)
    if col_config:
        kwargs["column_config"] = col_config
    if height:
        kwargs["height"] = height
    st.dataframe(df.style.set_properties(**{"background-color": "#0d1117", "color": "#c9d1d9"}), **kwargs)


# ── Plotly chart builders ─────────────────────────────────────────────────────

def _bar_chart(df, x, y, color="#013369", title="", yaxis_title=""):
    fig = go.Figure(go.Bar(
        x=df[x], y=df[y], marker_color=color,
        hovertemplate=f"<b>%{{x}}</b><br>{yaxis_title or y}: %{{y:.1f}}<extra></extra>",
    ))
    fig.update_layout(
        **PLOTLY_TEMPLATE["layout"].to_plotly_json(),
        title=dict(text=title, font=dict(size=14, color="#ffffff"), x=0),
        xaxis_title=None, yaxis_title=yaxis_title,
        height=300,
    )
    return fig


def _line_chart(df, x, ys: list[tuple], title="", height=320):
    """ys = list of (col, label, color)"""
    fig = go.Figure()
    for col, label, color in ys:
        if col in df.columns:
            fig.add_trace(go.Scatter(
                x=df[x], y=df[col], name=label,
                mode="lines+markers",
                line=dict(color=color, width=2.5),
                marker=dict(size=6, color=color),
                hovertemplate=f"<b>Wk %{{x}}</b><br>{label}: %{{y:.1f}}<extra></extra>",
            ))
    fig.update_layout(
        **PLOTLY_TEMPLATE["layout"].to_plotly_json(),
        title=dict(text=title, font=dict(size=14, color="#ffffff"), x=0),
        height=height,
        hovermode="x unified",
    )
    return fig


def _dual_axis_chart(df, x, bar_col, bar_label, line_col, line_label, bar_color, line_color, title=""):
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(
        x=df[x], y=df[bar_col], name=bar_label,
        marker_color=bar_color, opacity=0.85,
        hovertemplate=f"<b>%{{x}}</b><br>{bar_label}: %{{y:.1f}}<extra></extra>",
    ), secondary_y=False)
    if line_col in df.columns:
        fig.add_trace(go.Scatter(
            x=df[x], y=df[line_col], name=line_label,
            mode="lines+markers",
            line=dict(color=line_color, width=2.5, dash="dot"),
            marker=dict(size=6),
            hovertemplate=f"<b>%{{x}}</b><br>{line_label}: %{{y:.1f}}<extra></extra>",
        ), secondary_y=True)
    layout = PLOTLY_TEMPLATE["layout"].to_plotly_json()
    layout["yaxis2"] = dict(
        gridcolor="#1e2a3a", linecolor="#1e2a3a",
        tickfont=dict(color=line_color), showgrid=False,
    )
    fig.update_layout(
        **layout,
        title=dict(text=title, font=dict(size=14, color="#ffffff"), x=0),
        height=320,
        hovermode="x unified",
    )
    return fig


# ── Position-specific stat views ──────────────────────────────────────────────

def _render_career_qb(career_df, colors):
    reg = career_df[career_df["season_type"] == "REG"].copy()
    if reg.empty:
        st.info("No regular season data found.")
        return

    display_cols = ["season", "team", "games", "completions", "attempts", "comp_pct",
                    "passing_yards", "ypa", "passing_tds", "interceptions", "sacks",
                    "carries", "rushing_yards", "rushing_tds",
                    "passing_epa", "cpoe", "fpts_ppr", "fpts_ppr_per_game"]
    display_cols = [c for c in display_cols if c in reg.columns]

    col_config = {
        **_col_config_base(),
        "completions": st.column_config.NumberColumn("CMP"),
        "attempts": st.column_config.NumberColumn("ATT"),
        "comp_pct": st.column_config.NumberColumn("CMP%", format="%.1f"),
        "passing_yards": st.column_config.NumberColumn("Pass Yds", format="%d"),
        "ypa": st.column_config.NumberColumn("YPA", format="%.1f"),
        "passing_tds": st.column_config.NumberColumn("Pass TD"),
        "interceptions": st.column_config.NumberColumn("INT"),
        "sacks": st.column_config.NumberColumn("SCK"),
        "carries": st.column_config.NumberColumn("Car"),
        "rushing_yards": st.column_config.NumberColumn("Rush Yds", format="%d"),
        "rushing_tds": st.column_config.NumberColumn("Rush TD"),
        "passing_epa": st.column_config.NumberColumn("Pass EPA", format="%.1f"),
        "cpoe": st.column_config.NumberColumn("CPOE", format="%.2f"),
    }
    _styled_table(reg[display_cols], col_config=col_config)

    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(
            _bar_chart(reg, "season", "passing_yards", colors["primary"], "Passing Yards by Season", "Yards"),
            use_container_width=True,
        )
    with c2:
        st.plotly_chart(
            _bar_chart(reg, "season", "fpts_ppr", colors["secondary"] or "#FFB612", "PPR Fantasy Points by Season", "Points"),
            use_container_width=True,
        )


def _render_career_rb(career_df, colors):
    reg = career_df[career_df["season_type"] == "REG"].copy()
    if reg.empty:
        st.info("No regular season data found.")
        return

    display_cols = ["season", "team", "games", "carries", "rushing_yards", "ypc",
                    "rushing_tds", "targets", "receptions", "receiving_yards",
                    "receiving_tds", "rushing_epa", "receiving_epa",
                    "fpts_ppr", "fpts_ppr_per_game"]
    display_cols = [c for c in display_cols if c in reg.columns]

    col_config = {
        **_col_config_base(),
        "carries": st.column_config.NumberColumn("Car"),
        "rushing_yards": st.column_config.NumberColumn("Rush Yds", format="%d"),
        "ypc": st.column_config.NumberColumn("YPC", format="%.1f"),
        "rushing_tds": st.column_config.NumberColumn("Rush TD"),
        "targets": st.column_config.NumberColumn("Tgt"),
        "receptions": st.column_config.NumberColumn("Rec"),
        "receiving_yards": st.column_config.NumberColumn("Rec Yds", format="%d"),
        "receiving_tds": st.column_config.NumberColumn("Rec TD"),
        "rushing_epa": st.column_config.NumberColumn("Rush EPA", format="%.1f"),
        "receiving_epa": st.column_config.NumberColumn("Rec EPA", format="%.1f"),
    }
    _styled_table(reg[display_cols], col_config=col_config)

    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(
            _bar_chart(reg, "season", "rushing_yards", colors["primary"], "Rushing Yards by Season", "Yards"),
            use_container_width=True,
        )
    with c2:
        st.plotly_chart(
            _bar_chart(reg, "season", "fpts_ppr", colors["secondary"] or "#FFB612", "PPR Fantasy Points by Season", "Points"),
            use_container_width=True,
        )


def _render_career_wrte(career_df, colors):
    reg = career_df[career_df["season_type"] == "REG"].copy()
    if reg.empty:
        st.info("No regular season data found.")
        return

    display_cols = ["season", "team", "games", "targets", "receptions", "catch_rate",
                    "receiving_yards", "ypt", "receiving_tds",
                    "receiving_air_yards", "tgt_share_pct", "wopr",
                    "receiving_epa", "fpts_ppr", "fpts_ppr_per_game"]
    display_cols = [c for c in display_cols if c in reg.columns]

    col_config = {
        **_col_config_base(),
        "targets": st.column_config.NumberColumn("Tgt"),
        "receptions": st.column_config.NumberColumn("Rec"),
        "catch_rate": st.column_config.NumberColumn("Catch%", format="%.1f"),
        "receiving_yards": st.column_config.NumberColumn("Rec Yds", format="%d"),
        "ypt": st.column_config.NumberColumn("YPT", format="%.1f"),
        "receiving_tds": st.column_config.NumberColumn("Rec TD"),
        "receiving_air_yards": st.column_config.NumberColumn("Air Yds", format="%d"),
        "tgt_share_pct": st.column_config.NumberColumn("Tgt Share%", format="%.1f"),
        "wopr": st.column_config.NumberColumn("WOPR", format="%.3f"),
        "receiving_epa": st.column_config.NumberColumn("Rec EPA", format="%.1f"),
    }
    _styled_table(reg[display_cols], col_config=col_config)

    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(
            _bar_chart(reg, "season", "receiving_yards", colors["primary"], "Receiving Yards by Season", "Yards"),
            use_container_width=True,
        )
    with c2:
        st.plotly_chart(
            _bar_chart(reg, "season", "fpts_ppr", colors["secondary"] or "#FFB612", "PPR Fantasy Points by Season", "Points"),
            use_container_width=True,
        )


def _render_weekly_qb(weekly_df, colors):
    if weekly_df.empty:
        st.info("No weekly data available.")
        return
    display = ["week", "opponent", "completions", "attempts", "passing_yards",
               "passing_tds", "interceptions", "sacks", "cpoe", "passing_epa",
               "carries", "rushing_yards", "rushing_tds", "rushing_epa",
               "fpts_ppr", "fpts_std"]
    display = [c for c in display if c in weekly_df.columns]
    col_config = {
        "week": st.column_config.NumberColumn("Wk"),
        "opponent": st.column_config.TextColumn("Opp"),
        "completions": st.column_config.NumberColumn("CMP"),
        "attempts": st.column_config.NumberColumn("ATT"),
        "passing_yards": st.column_config.NumberColumn("Pass Yds", format="%d"),
        "passing_tds": st.column_config.NumberColumn("Pass TD"),
        "interceptions": st.column_config.NumberColumn("INT"),
        "sacks": st.column_config.NumberColumn("SCK"),
        "cpoe": st.column_config.NumberColumn("CPOE", format="%.2f"),
        "passing_epa": st.column_config.NumberColumn("Pass EPA", format="%.2f"),
        "carries": st.column_config.NumberColumn("Car"),
        "rushing_yards": st.column_config.NumberColumn("Rush Yds", format="%d"),
        "rushing_tds": st.column_config.NumberColumn("Rush TD"),
        "rushing_epa": st.column_config.NumberColumn("Rush EPA", format="%.2f"),
        "fpts_ppr": st.column_config.NumberColumn("PPR Pts", format="%.1f"),
        "fpts_std": st.column_config.NumberColumn("Std Pts", format="%.1f"),
    }
    _styled_table(weekly_df[display], col_config=col_config)

    fig = _line_chart(
        weekly_df, "week",
        [
            ("passing_yards", "Pass Yards", colors["primary"]),
            ("fpts_ppr", "PPR Points", colors["secondary"] or "#FFB612"),
        ],
        title="Weekly Performance",
    )
    st.plotly_chart(fig, use_container_width=True)

    # EPA chart
    fig2 = _line_chart(
        weekly_df, "week",
        [
            ("passing_epa", "Passing EPA", "#4ade80"),
            ("rushing_epa", "Rushing EPA", "#60a5fa"),
        ],
        title="Weekly EPA",
    )
    st.plotly_chart(fig2, use_container_width=True)


def _render_weekly_rb(weekly_df, colors):
    if weekly_df.empty:
        st.info("No weekly data available.")
        return
    display = ["week", "opponent", "carries", "rushing_yards", "rushing_tds",
               "rushing_epa", "targets", "receptions", "receiving_yards",
               "receiving_tds", "receiving_epa", "fpts_ppr", "fpts_std"]
    display = [c for c in display if c in weekly_df.columns]
    col_config = {
        "week": st.column_config.NumberColumn("Wk"),
        "opponent": st.column_config.TextColumn("Opp"),
        "carries": st.column_config.NumberColumn("Car"),
        "rushing_yards": st.column_config.NumberColumn("Rush Yds", format="%d"),
        "rushing_tds": st.column_config.NumberColumn("Rush TD"),
        "rushing_epa": st.column_config.NumberColumn("Rush EPA", format="%.2f"),
        "targets": st.column_config.NumberColumn("Tgt"),
        "receptions": st.column_config.NumberColumn("Rec"),
        "receiving_yards": st.column_config.NumberColumn("Rec Yds", format="%d"),
        "receiving_tds": st.column_config.NumberColumn("Rec TD"),
        "receiving_epa": st.column_config.NumberColumn("Rec EPA", format="%.2f"),
        "fpts_ppr": st.column_config.NumberColumn("PPR Pts", format="%.1f"),
        "fpts_std": st.column_config.NumberColumn("Std Pts", format="%.1f"),
    }
    _styled_table(weekly_df[display], col_config=col_config)

    fig = _line_chart(
        weekly_df, "week",
        [
            ("rushing_yards", "Rush Yards", colors["primary"]),
            ("receiving_yards", "Rec Yards", colors["secondary"] or "#60a5fa"),
            ("fpts_ppr", "PPR Points", "#FFB612"),
        ],
        title="Weekly Performance",
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_weekly_wrte(weekly_df, colors):
    if weekly_df.empty:
        st.info("No weekly data available.")
        return
    display = ["week", "opponent", "targets", "receptions", "receiving_yards",
               "receiving_tds", "tgt_share_pct", "air_yds_share_pct",
               "wopr", "receiving_epa", "fpts_ppr", "fpts_std"]
    display = [c for c in display if c in weekly_df.columns]
    col_config = {
        "week": st.column_config.NumberColumn("Wk"),
        "opponent": st.column_config.TextColumn("Opp"),
        "targets": st.column_config.NumberColumn("Tgt"),
        "receptions": st.column_config.NumberColumn("Rec"),
        "receiving_yards": st.column_config.NumberColumn("Rec Yds", format="%d"),
        "receiving_tds": st.column_config.NumberColumn("Rec TD"),
        "tgt_share_pct": st.column_config.NumberColumn("Tgt Share%", format="%.1f"),
        "air_yds_share_pct": st.column_config.NumberColumn("AY Share%", format="%.1f"),
        "wopr": st.column_config.NumberColumn("WOPR", format="%.3f"),
        "receiving_epa": st.column_config.NumberColumn("Rec EPA", format="%.2f"),
        "fpts_ppr": st.column_config.NumberColumn("PPR Pts", format="%.1f"),
        "fpts_std": st.column_config.NumberColumn("Std Pts", format="%.1f"),
    }
    _styled_table(weekly_df[display], col_config=col_config)

    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(
            _line_chart(
                weekly_df, "week",
                [
                    ("receiving_yards", "Rec Yards", colors["primary"]),
                    ("fpts_ppr", "PPR Points", "#FFB612"),
                ],
                title="Weekly Yards & Fantasy Points",
            ),
            use_container_width=True,
        )
    with c2:
        st.plotly_chart(
            _line_chart(
                weekly_df, "week",
                [
                    ("targets", "Targets", colors["primary"]),
                    ("receptions", "Receptions", colors["secondary"] or "#60a5fa"),
                    ("tgt_share_pct", "Target Share %", "#f59e0b"),
                ],
                title="Weekly Usage",
            ),
            use_container_width=True,
        )


def _render_advanced_wrte(ngs_df, ngs_avg, colors):
    if ngs_avg.empty or ngs_avg.iloc[0]["weeks"] == 0:
        st.info("No Next Gen Stats available for this player/season. (NGS data available from 2016 for WR/TE.)")
        return

    row = ngs_avg.iloc[0]

    st.markdown('<div class="section-header">Season Averages — Next Gen Stats</div>', unsafe_allow_html=True)

    cols = st.columns(5)
    metrics = [
        ("Separation", f"{_fmt(row.get('avg_separation'), 2)} yds", "Avg yards of separation from nearest defender at time of throw"),
        ("Cushion", f"{_fmt(row.get('avg_cushion'), 2)} yds", "Avg yards between receiver and nearest defender at snap"),
        ("aDOT", f"{_fmt(row.get('avg_adot'), 1)} yds", "Average depth of target (intended air yards per target)"),
        ("Avg YAC", f"{_fmt(row.get('avg_yac'), 1)} yds", "Average yards after catch"),
        ("YAC OE", f"{_fmt(row.get('avg_yac_oe'), 2)}", "YAC above expectation per reception"),
    ]
    for col, (label, val, help_text) in zip(cols, metrics):
        with col:
            st.metric(label=label, value=val, help=help_text)

    if not ngs_df.empty:
        st.markdown('<div class="section-header">Week-by-Week Next Gen Stats</div>', unsafe_allow_html=True)
        col_config = {
            "week": st.column_config.NumberColumn("Wk"),
            "targets": st.column_config.NumberColumn("Tgt"),
            "receptions": st.column_config.NumberColumn("Rec"),
            "yards": st.column_config.NumberColumn("Yds", format="%d"),
            "separation": st.column_config.NumberColumn("Sep (yds)", format="%.1f"),
            "cushion": st.column_config.NumberColumn("Cushion (yds)", format="%.1f"),
            "intended_adot": st.column_config.NumberColumn("aDOT", format="%.1f"),
            "avg_yac": st.column_config.NumberColumn("YAC", format="%.1f"),
            "yac_oe": st.column_config.NumberColumn("YAC OE", format="%.1f"),
            "catch_pct": st.column_config.NumberColumn("Catch%", format="%.1f"),
        }
        _styled_table(ngs_df, col_config=col_config)

        # Separation chart
        fig = _line_chart(
            ngs_df, "week",
            [
                ("separation", "Separation (yds)", "#4ade80"),
                ("intended_adot", "aDOT (yds)", colors["primary"]),
                ("yac_oe", "YAC OE", "#f59e0b"),
            ],
            title="Weekly Next Gen Stats",
        )
        st.plotly_chart(fig, use_container_width=True)


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown(
        '<div class="sidebar-brand"><span>🏈</span><h1>NFL Player Hub</h1></div>',
        unsafe_allow_html=True,
    )

    position_options = ["QB", "RB", "WR", "TE"]
    selected_positions = st.multiselect(
        "Position",
        options=position_options,
        default=position_options,
    )

    st.markdown("---")

    search_term = st.text_input("Search player", placeholder="e.g. Patrick Mahomes")

    # Load player list
    with st.spinner("Loading players…"):
        players_df = get_all_players(selected_positions if selected_positions else position_options)

    if search_term:
        players_df = players_df[
            players_df["full_name"].str.contains(search_term, case=False, na=False)
        ]

    if players_df.empty:
        st.warning("No players found.")
        st.stop()

    # Group by position for better UX
    player_options = players_df["full_name"].tolist()
    player_ids = players_df["player_id"].tolist()

    selected_name = st.selectbox(
        "Select player",
        options=player_options,
        index=0,
    )

    selected_row = players_df[players_df["full_name"] == selected_name].iloc[0]
    player_id = selected_row["player_id"]
    position = selected_row["position"]

    st.markdown("---")

    seasons = get_player_seasons(player_id)
    if seasons:
        selected_season = st.selectbox(
            "Season (Weekly View)",
            options=seasons,
            index=0,
        )
    else:
        selected_season = None

    st.markdown("---")
    st.caption("Data: nflverse · nflfastR · Next Gen Stats")
    st.caption("Updated through 2025 season")


# ── Main content ──────────────────────────────────────────────────────────────

colors = get_team_colors(str(selected_row.get("latest_team", "")))
team_abbr = str(selected_row.get("latest_team", ""))
team_logo_url = get_team_logo_url(team_abbr)
headshot_url = selected_row.get("headshot_url", "")

# Inject team color as CSS variable
st.markdown(
    f"<style>:root {{ --team-primary: {colors['primary']}; }}</style>",
    unsafe_allow_html=True,
)

# ── Player hero header ────────────────────────────────────────────────────────
hero_col1, hero_col2, hero_col3 = st.columns([1.2, 4, 1.2])

with hero_col1:
    if headshot_url and isinstance(headshot_url, str) and headshot_url.startswith("http"):
        st.image(headshot_url, width=180)
    else:
        st.markdown(
            f"<div style='width:140px;height:140px;border-radius:50%;background:{colors['primary']};"
            f"display:flex;align-items:center;justify-content:center;"
            f"font-size:3rem;color:white;'>🏈</div>",
            unsafe_allow_html=True,
        )

with hero_col2:
    pos_badge = f"<span class='position-badge'>{position}</span>"
    active_badge = (
        "<span class='position-badge' style='background:#15803d;margin-left:8px;'>Active</span>"
        if selected_row.get("active_flag") == 1
        else ""
    )
    latest_season = int(selected_row.get("latest_season", 0))
    first_season = int(selected_row.get("first_season", 0))
    seasons_text = f"{first_season}–{latest_season}" if first_season != latest_season else str(first_season)

    height = selected_row.get("height", "")
    weight = selected_row.get("weight", "")
    college = selected_row.get("college", "")
    entry_year = selected_row.get("entry_year", "")

    bio_parts = []
    if team_abbr:
        bio_parts.append(team_abbr)
    if height:
        bio_parts.append(str(height))
    if weight:
        bio_parts.append(f"{int(weight)} lbs" if weight else "")
    if college:
        bio_parts.append(str(college))
    if entry_year and not pd.isna(entry_year):
        bio_parts.append(f"Draft: {int(entry_year)}")
    bio_parts = [b for b in bio_parts if b and str(b) != "nan"]

    st.markdown(
        f"""
        <div class="player-hero">
            <div>{pos_badge}{active_badge}</div>
            <div class="player-name">{selected_name}</div>
            <div class="player-meta">
                {"<span>·</span>".join(bio_parts)}
            </div>
            <div class="player-meta" style="font-size:0.8rem; margin-bottom:0;">
                Seasons: {seasons_text}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with hero_col3:
    if team_abbr:
        st.image(team_logo_url, width=120)

# ── Career summary metrics ────────────────────────────────────────────────────
career_df = get_career_stats(player_id, position)
if not career_df.empty and "season_type" in career_df.columns:
    reg_career = career_df[career_df["season_type"] == "REG"]
else:
    reg_career = career_df

if not reg_career.empty:
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    total_games = int(reg_career["games"].sum())
    total_fpts = reg_career["fpts_ppr"].sum()
    fpts_pg = total_fpts / total_games if total_games else 0

    metric_configs = []
    if position == "QB":
        metric_configs = [
            (m1, "Games", _fmt(total_games), ""),
            (m2, "Pass Yards", _fmt(reg_career["passing_yards"].sum()), "career"),
            (m3, "Pass TDs", _fmt(reg_career["passing_tds"].sum()), "career"),
            (m4, "INTs", _fmt(reg_career["interceptions"].sum()), "career"),
            (m5, "PPR Pts", _fmt(total_fpts, 0), "career"),
            (m6, "PPR/G", _fmt(fpts_pg, 1), "avg"),
        ]
    elif position == "RB":
        metric_configs = [
            (m1, "Games", _fmt(total_games), ""),
            (m2, "Rush Yards", _fmt(reg_career["rushing_yards"].sum()), "career"),
            (m3, "Rush TDs", _fmt(reg_career["rushing_tds"].sum()), "career"),
            (m4, "Receptions", _fmt(reg_career["receptions"].sum()), "career"),
            (m5, "PPR Pts", _fmt(total_fpts, 0), "career"),
            (m6, "PPR/G", _fmt(fpts_pg, 1), "avg"),
        ]
    else:  # WR/TE
        metric_configs = [
            (m1, "Games", _fmt(total_games), ""),
            (m2, "Targets", _fmt(reg_career["targets"].sum()), "career"),
            (m3, "Rec Yards", _fmt(reg_career["receiving_yards"].sum()), "career"),
            (m4, "Rec TDs", _fmt(reg_career["receiving_tds"].sum()), "career"),
            (m5, "PPR Pts", _fmt(total_fpts, 0), "career"),
            (m6, "PPR/G", _fmt(fpts_pg, 1), "avg"),
        ]

    for col, label, value, sub in metric_configs:
        with col:
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-label">{label}</div>
                    <div class="metric-value">{value}</div>
                    <div class="metric-sub">{sub}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

st.markdown("<br>", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_labels = ["Career Stats", f"{selected_season} — Weekly", "Advanced Stats"]
tab1, tab2, tab3 = st.tabs(tab_labels)

# ── Tab 1: Career Stats ───────────────────────────────────────────────────────
with tab1:
    st.markdown('<div class="section-header">Season-by-Season Stats</div>', unsafe_allow_html=True)
    if career_df.empty:
        st.info("No career data available for this player.")
    else:
        if position == "QB":
            _render_career_qb(career_df, colors)
        elif position == "RB":
            _render_career_rb(career_df, colors)
        else:
            _render_career_wrte(career_df, colors)

        # Postseason if available
        post = career_df[career_df["season_type"] == "POST"]
        if not post.empty:
            with st.expander("Postseason Stats"):
                _styled_table(post, col_config=_col_config_base())

# ── Tab 2: Weekly (selected season) ──────────────────────────────────────────
with tab2:
    if not selected_season:
        st.info("No season data available.")
    else:
        st.markdown(
            f'<div class="section-header">Week-by-Week — {selected_season} Season</div>',
            unsafe_allow_html=True,
        )
        weekly_df = get_weekly_stats(player_id, selected_season)

        if weekly_df.empty:
            st.info(f"No weekly data for {selected_season}.")
        else:
            # Season aggregate quick stats
            agg_row = weekly_df.agg({
                "fpts_ppr": ["sum", "mean", "max"],
            })
            total_fpts_s = weekly_df["fpts_ppr"].sum()
            avg_fpts_s = weekly_df["fpts_ppr"].mean()
            max_fpts_s = weekly_df["fpts_ppr"].max()

            sc1, sc2, sc3, sc4 = st.columns(4)
            sc1.metric("Weeks Played", len(weekly_df))
            sc2.metric("Total PPR Pts", f"{total_fpts_s:.1f}")
            sc3.metric("Avg PPR/Wk", f"{avg_fpts_s:.1f}")
            sc4.metric("Best Week", f"{max_fpts_s:.1f}")

            st.markdown("---")

            if position == "QB":
                _render_weekly_qb(weekly_df, colors)
            elif position == "RB":
                _render_weekly_rb(weekly_df, colors)
            else:
                _render_weekly_wrte(weekly_df, colors)

# ── Tab 3: Advanced Stats ─────────────────────────────────────────────────────
with tab3:
    if not selected_season:
        st.info("Select a season in the sidebar.")
    else:
        st.markdown(
            f'<div class="section-header">Advanced Stats — {selected_season}</div>',
            unsafe_allow_html=True,
        )

        if position == "QB":
            st.markdown("**EPA & Efficiency (regular season)**")
            reg = career_df[career_df["season_type"] == "REG"].copy() if not career_df.empty else pd.DataFrame()
            if not reg.empty and "passing_epa" in reg.columns:
                fig = _line_chart(
                    reg, "season",
                    [
                        ("passing_epa", "Passing EPA", "#4ade80"),
                        ("rushing_epa", "Rushing EPA", "#60a5fa"),
                    ],
                    title="EPA by Season (Regular Season)",
                )
                st.plotly_chart(fig, use_container_width=True)

                # CPOE trend
                if "cpoe" in reg.columns:
                    fig2 = _bar_chart(
                        reg, "season", "cpoe", colors["primary"],
                        "Completion % Over Expected (CPOE) by Season", "CPOE"
                    )
                    st.plotly_chart(fig2, use_container_width=True)
            else:
                st.info("No advanced EPA data available.")

        elif position == "RB":
            reg = career_df[career_df["season_type"] == "REG"].copy() if not career_df.empty else pd.DataFrame()
            if not reg.empty:
                c1, c2 = st.columns(2)
                with c1:
                    if "rushing_epa" in reg.columns:
                        st.plotly_chart(
                            _bar_chart(reg, "season", "rushing_epa", colors["primary"], "Rushing EPA by Season", "EPA"),
                            use_container_width=True,
                        )
                with c2:
                    if "receiving_epa" in reg.columns:
                        st.plotly_chart(
                            _bar_chart(reg, "season", "receiving_epa", colors["secondary"] or "#60a5fa", "Receiving EPA by Season", "EPA"),
                            use_container_width=True,
                        )
                # Weekly EPA scatter
                weekly_adv = get_weekly_stats(player_id, selected_season)
                if not weekly_adv.empty:
                    fig = _line_chart(
                        weekly_adv, "week",
                        [
                            ("rushing_epa", "Rush EPA", colors["primary"]),
                            ("receiving_epa", "Rec EPA", colors["secondary"] or "#60a5fa"),
                        ],
                        title=f"Weekly EPA — {selected_season}",
                    )
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No advanced data available.")

        else:  # WR/TE — show NGS
            ngs_df = get_ngs_stats(player_id, selected_season)
            ngs_avg = get_season_ngs_avg(player_id, selected_season)
            _render_advanced_wrte(ngs_df, ngs_avg, colors)

            # Also show EPA
            reg = career_df[career_df["season_type"] == "REG"].copy() if not career_df.empty else pd.DataFrame()
            if not reg.empty and "receiving_epa" in reg.columns:
                st.markdown('<div class="section-header">Receiving EPA — Career Trend</div>', unsafe_allow_html=True)
                st.plotly_chart(
                    _bar_chart(reg, "season", "receiving_epa", colors["primary"], "Receiving EPA by Season", "EPA"),
                    use_container_width=True,
                )
