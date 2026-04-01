# NFL Analytics Data Platform — Architecture Documentation

## Overview

Production-grade NFL data infrastructure built on R (nflverse) + DuckDB + Parquet.
Supports game result, spread, totals, and player projection modeling for betting workflows.

---

## Table Inventory

### BRONZE / RAW LAYER
_Exact ingested data from nflverse. No business logic._

| Table | Grain | Partition | Source | Notes |
|---|---|---|---|---|
| `raw_pbp` | game_id + play_id | season | `nflreadr::load_pbp` | 1999+ |
| `raw_schedules` | game_id | season | `nflreadr::load_schedules` | All game types |
| `raw_player_stats` | season+week+player_id+stat_type | season | `nflreadr::load_player_stats` | |
| `raw_team_stats` | season+week+team | season | nflfastR series conversion | |
| `raw_participation` | game_id+play_id+player_id | season | `nflreadr::load_participation` | 2016+ |
| `raw_players` | gsis_id | none | `nflreadr::load_players` | Full refresh |
| `raw_rosters` | season+team+gsis_id | season | `nflreadr::load_rosters` | |
| `raw_rosters_weekly` | season+week+team+gsis_id | season | `nflreadr::load_rosters(weekly=T)` | 2002+ |
| `raw_depth_charts` | season+week+team+gsis_id+pos+depth+formation | season | `nflreadr::load_depth_charts` | 2001+ |
| `raw_injuries` | season+week+team+gsis_id+injury | season | `nflreadr::load_injuries` | 2009+ |
| `raw_nextgen_stats` | season+week+gsis_id+stat_type | season | `nflreadr::load_nextgen_stats` | 2016+, 3 types |
| `raw_pfr_advstats` | season+week+pfr_id+stat_type | season | `nflreadr::load_pfr_advstats` | 2018+, 4 types |
| `raw_snap_counts` | season+week+pfr_player_id | season | `nflreadr::load_snap_counts` | 2012+ |
| `raw_ftn_charting` | game_id+play_id | season | `nflreadr::load_ftn_charting` | 2022+ |
| `raw_ff_opportunity` | season+week+player_id | season | `nflreadr::load_ff_opportunity` | 2016+ |
| `raw_external_odds` | game_id+market_ts+sportsbook | season | External (placeholder) | |

---

### STAGING / SILVER LAYER
_Standardised, typed, deduped. Canonical keys established._

| Table | Grain | Key | Purpose |
|---|---|---|---|
| `stg_games` | game_id | game_id | Clean schedule/result table; team abbrs standardised |
| `stg_plays` | game_id + play_id | (game_id, play_id) | Typed PBP columns, derived flags |
| `stg_drives` | game_id + drive_id | (game_id, drive_id) | Aggregated drive summaries from PBP |
| `stg_teams` | team_abbr | team_abbr | Canonical team list across all seasons |
| `stg_players` | player_id (gsis_id) | player_id | Player registry with position groups |
| `stg_player_week` | season+week+player_id+position | (season,week,player_id,position) | Box score stats per player per week |
| `stg_team_week` | season+week+team | (season,week,team) | One row per team per game (home+away unpivoted) |
| `stg_rosters_weekly` | season+week+team+player_id | (season,week,team,player_id) | 53-man roster state by week |
| `stg_injuries_weekly` | season+week+team+player_id+injury | composite | Injury severity with encoded status |
| `stg_snap_counts_weekly` | season+week+team+pfr_id | (season,week,pfr_id) | Snap counts with clean type casting |
| `stg_nextgen_player_week` | season+week+player_id+stat_type | composite | NGS metrics per player per week |
| `stg_external_odds_game` | game_id+market_ts+sportsbook | composite | Cleaned odds lines (placeholder) |

---

### INTERMEDIATE LAYER
_Reusable derived tables. Pre-game features are fully lag-safe._

| Table | Grain | Key | Purpose |
|---|---|---|---|
| `int_game_base` | game_id | game_id | Game context: rest days, venue, flags, scores |
| `int_team_game` | game_id + team | (game_id, team) | Offense + defense metrics per team per game |
| `int_player_game` | game_id + player_id | (game_id, player_id) | Full box score + snaps + team context |
| `int_player_form` | season+week+player_id | (season,week,player_id) | Lagged rolling usage and efficiency features |
| `int_qb_team_context` | game_id + player_id | (game_id, player_id) | QB metrics + OL proxy + opponent pressure |
| `int_team_form` | season+week+team | (season,week,team) | Lagged rolling team strength (STD, L5, L3, blended) |
| `int_injury_team_impact` | season+week+team | (season,week,team) | Injury burden by position group, pre-game |

---

### MART / GOLD LAYER
_Final model-ready tables. Fully documented leakage contracts._

| Table | Grain | Purpose |
|---|---|---|
| `mart_game_modeling` | game_id | Win/spread/total prediction features + targets |
| `mart_team_week_modeling` | season+week+team | Team priors for downstream joins |
| `mart_player_week_projection` | season+week+player_id | Universal player projection features |
| `mart_qb_projection` | game_id+player_id | Dedicated QB model table |
| `mart_receiver_projection` | game_id+player_id | WR/TE target share and yardage model table |
| `mart_rusher_projection` | game_id+player_id | RB carries and rushing yards model table |
| `mart_backtest_game` | game_id | Leakage-safe historical game training set |
| `mart_backtest_player` | season+week+player_id | Leakage-safe historical player training set |

---

### DIMENSION TABLES

| Table | Grain | Purpose |
|---|---|---|
| `dim_season` | season | Season metadata, week counts |
| `dim_week` | season+week | Week date bounds |
| `dim_team` | team_abbr | Canonical team info, conference/division |
| `dim_player` | player_id | Player registry with cross-reference IDs |
| `dim_game` | game_id | Game schedule dimension |
| `dim_position_group` | position | Maps raw positions to groups and side of ball |

---

### FACT TABLES

| Table | Grain | Purpose |
|---|---|---|
| `fact_play` | game_id+play_id | Core event table |
| `fact_drive` | game_id+drive_id | Drive outcomes |
| `fact_team_game` | game_id+team | Team game results and efficiency |
| `fact_player_game` | game_id+player_id | Player game box scores |
| `fact_team_week` | season+week+team | Weekly team ledger |
| `fact_player_week` | season+week+player_id | Weekly player stat ledger |
| `fact_injury_status` | season+week+player_id+report_date | Injury timeline |
| `fact_depth_chart` | season+week+team+player_id+pos+depth+formation | Depth chart snapshots |
| `fact_snap_count` | season+week+pfr_player_id | Snap volume by player |
| `fact_market_game` | game_id+market_ts+sportsbook+market_type | Sportsbook lines (placeholder) |

---

## Leakage Prevention Rules

All intermediate and mart tables enforce these rules:

1. **Rolling window functions** use `ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`
   to exclude the current row (current game) from all aggregates.

2. **Rest days / bye week flags** use `LAG()` over the prior game date — entirely pre-game information.

3. **Injury burden** uses injury reports filed before game week. The `stg_injuries_weekly`
   table's week column refers to the reporting week, which precedes the game.

4. **`mart_backtest_game`** includes a filter `home_games_played >= 1` and
   `away_games_played >= 1` to exclude weeks with no prior form data.

5. The `cv_split` column in backtest marts provides suggested train/validation/test splits
   by season year. **Do not** use random row splits across seasons — always split by season
   to prevent temporal leakage.

---

## Feature Engineering Reference

### Team Strength Features (int_team_form)

| Feature | Description | Window |
|---|---|---|
| `epa_pp_std` | Offensive EPA/play | Season-to-date (lagged) |
| `pass_epa_std` | Pass EPA/dropback | Season-to-date |
| `rush_epa_std` | Rush EPA/carry | Season-to-date |
| `def_epa_pp_std` | Defensive EPA/play allowed | Season-to-date |
| `success_rate_std` | Offensive success rate | Season-to-date |
| `def_success_rate_allowed_std` | Defense success rate allowed | Season-to-date |
| `explosive_pass_rate_std` | % pass plays 20+ yds | Season-to-date |
| `pass_rate_std` | Pass play % | Season-to-date |
| `turnovers_per_game_std` | Giveaways/game | Season-to-date |
| `rz_td_rate_std` | Red zone TD% | Season-to-date |
| `epa_pp_l5` / `epa_pp_l3` | Recent EPA form | Last 5 / Last 3 |
| `epa_pp_blended` | 70% current + 30% prior season | Blended |

### Player Usage Features (int_player_form)

| Feature | Description |
|---|---|
| `target_share_std` | % of team targets |
| `air_yards_share_std` | % of team air yards |
| `wopr_std` | Weighted opportunity rating |
| `snap_pct_std` | Offensive snap % |
| `carries_per_game_std` | Avg carries per game |
| `rush_yds_per_game_std` | Avg rush yards |
| `targets_per_game_l4` | Target rate recent 4 games |
| `carries_per_game_l2` | Carry rate recent 2 games |

### Game Context Features (int_game_base)

| Feature | Description |
|---|---|
| `home_rest_days` | Days since last game (home team) |
| `home_short_week_flag` | ≤ 6 days rest |
| `home_bye_prior_flag` | Coming off bye |
| `rest_days_advantage_home` | Home rest minus away rest |
| `primetime_flag` | Mon/Thu/Sun Night game |
| `div_game` | Divisional matchup |
| `surface` | Grass/Turf |
| `roof` | Dome/Outdoor/Retractable |

---

## External Odds Integration

When odds data becomes available, add it via `ingest_external_odds()` which writes
to `data/raw/external_odds/`. The schema is pre-defined in `stg_external_odds_game`.

`mart_game_modeling` already includes LEFT JOINs to the odds staging table:
- Opening line (earliest `market_timestamp` per `game_id`)
- Closing line (latest `market_timestamp` per `game_id`)

Derived targets (`home_covered_flag`, `over_flag`) become active once odds are loaded.

Recommended odds sources:
- **The Odds API** (commercial, reasonable pricing)
- **Sportradar Odds** (enterprise)
- **historical-odds.com** for backtesting
- **nflverse's own `nflreadr::load_espn_betting_lines()`** — check availability

---

## Scalability Migration Paths

### dbt Cloud
Replace the SQL view files 1:1 as dbt models. The layered naming (`stg_`, `int_`, `mart_`)
maps directly to dbt's staging/intermediate/marts convention. Add `schema.yml` files
for documentation and tests. Use `dbt build --select tag:daily` for incremental runs.

### Apache Airflow / Prefect / Dagster
Replace `pipeline.R` with a DAG. Each ingestion function and `materialise_table()` call
becomes a task node. Dependencies are implicit in the current code structure.

### Cloud Warehouses
| Platform | Migration Path |
|---|---|
| BigQuery | Replace Parquet + DuckDB with BigQuery tables. Use `bigrquery` R package. Partition by `season`. |
| Snowflake | Use `RSQLite`→`odbc`/`DBI` with Snowflake DSN. Stage Parquet to S3 then COPY INTO. |
| Databricks | PySpark or SparkR with Delta Lake. Convert SQL to Spark SQL with minor syntax changes. |

The SQL in `sql/` is standard ANSI SQL with DuckDB-specific window functions — all are
supported by BigQuery, Snowflake, and Spark SQL with minimal changes.

---

## Directory Structure

```
nfl_analytics/
├── pipeline.R                    # Master pipeline entry point
├── config/
│   └── config.yml                # Seasons, paths, refresh modes
├── R/
│   ├── ingest/
│   │   ├── ingest_pbp.R
│   │   └── ingest_all_sources.R
│   ├── transform/
│   │   └── run_transforms.R
│   ├── features/                 # (optional R feature functions)
│   ├── models/                   # (your model training code)
│   └── utils/
│       ├── config_loader.R
│       ├── db_connection.R
│       ├── parquet_io.R
│       ├── logging.R
│       ├── run_tests.R
│       └── install_packages.R
├── sql/
│   ├── staging/
│   │   ├── stg_games.sql
│   │   ├── stg_plays_drives_teams_players.sql
│   │   └── stg_weekly_tables.sql
│   ├── intermediate/
│   │   ├── int_game_team.sql
│   │   ├── int_player_qb.sql
│   │   └── int_team_form_injury.sql
│   ├── marts/
│   │   ├── mart_game_team_modeling.sql
│   │   └── mart_player_projections.sql
│   ├── dimensions/
│   │   └── ddl_dimensions_facts.sql
│   └── tests/
│       └── data_tests.sql
├── data/
│   ├── raw/          (Parquet, partitioned by season)
│   ├── staging/      (Parquet, partitioned by season)
│   ├── intermediate/ (Parquet, partitioned by season)
│   └── marts/        (Parquet, partitioned by season)
├── db/
│   └── nfl_analytics.duckdb
├── logs/
└── .github/
    └── workflows/
        └── daily_refresh.yml
```

---

## Design Decisions and Tradeoffs

### Why DuckDB + Parquet instead of a full warehouse?
- Zero infrastructure cost for solo/small-team usage
- DuckDB executes analytical SQL at warehouse-class speed on a laptop
- Parquet partitioning gives cloud-warehouse-like query pruning locally
- Trivially migrated to BigQuery/Snowflake by replacing `arrow::write_parquet` with LOAD statements

### Why R + nflverse instead of Python?
- nflverse is the authoritative NFL data provider with the best coverage and maintenance
- nflfastR's EPA model is the industry standard for NFL play valuation
- DBI/arrow/duckdb R packages are mature and production-ready
- Python users can consume the Parquet outputs via `pandas.read_parquet` or `polars.read_parquet`

### Why views materialised to Parquet rather than pure in-database views?
- Enables offline analysis without re-running computations
- Supports sharing mart tables with downstream modeling notebooks
- Provides a recovery point: if the DB file is lost, Parquet remains
- Allows querying specific partitions cheaply

### Why separate mart tables per player type (QB/WR/RB) rather than one wide table?
- Avoids sparse columns (a QB row would have 80% NULL receiving columns)
- Keeps model training datasets narrow and focused
- Easier to iterate on position-specific features without schema conflicts

### Leakage vs Coverage Tradeoff
Early-season games (weeks 1-3) have little rolling history.
The `epa_pp_blended` feature addresses this by falling back to the prior full season
when fewer than 3 games are played. Prior-season metrics act as a regularising prior
— this is standard practice in Bayesian-style sports models.
