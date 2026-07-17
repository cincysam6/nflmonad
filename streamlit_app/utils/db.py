import duckdb
import pandas as pd
from pathlib import Path
import streamlit as st

# Project root is two levels up from this file (streamlit_app/utils/db.py)
PROJECT_ROOT = Path(__file__).parent.parent.parent


def _data_path(relative: str) -> str:
    """Return forward-slash path for DuckDB read_parquet."""
    return str(PROJECT_ROOT / relative).replace("\\", "/")


@st.cache_resource
def get_connection():
    """Create a shared in-memory DuckDB connection with all views registered."""
    con = duckdb.connect(":memory:")
    _register_views(con)
    return con


def _register_views(con: duckdb.DuckDBPyConnection):
    """Register staging Parquet directories as DuckDB views."""
    staging_base = PROJECT_ROOT / "data" / "staging"

    partitioned = [
        "stg_player_week",
        "stg_nextgen_player_week",
        "stg_snap_counts_weekly",
        "stg_rosters_weekly",
    ]
    for name in partitioned:
        path = staging_base / name
        if path.exists():
            p = str(path).replace("\\", "/") + "/**/*.parquet"
            con.execute(
                f"CREATE OR REPLACE VIEW {name} AS "
                f"SELECT * FROM read_parquet('{p}', hive_partitioning=true, union_by_name=true)"
            )

    single = ["stg_players"]
    for name in single:
        path = staging_base / name / "data.parquet"
        if path.exists():
            p = str(path).replace("\\", "/")
            con.execute(
                f"CREATE OR REPLACE VIEW {name} AS "
                f"SELECT * FROM read_parquet('{p}')"
            )


def query(sql: str, **params) -> pd.DataFrame:
    """Run a SQL query and return a DataFrame."""
    con = get_connection()
    if params:
        return con.execute(sql, list(params.values())).df()
    return con.execute(sql).df()
