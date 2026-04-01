# NFL Analytics Platform — Extended Architecture Documentation
## External Data Integration: Odds, Weather, Travel, Power Ratings

---

## Expanded Architecture Summary

The platform now ingests six additional data domains layered over the nflverse base:

| Domain | Sources | Leakage Status |
|---|---|---|
| Sportsbook odds | nflverse ESPN, The Odds API, SBR CSV | Opening: safe; Closing: CLV only |
| Realized weather | Open-Meteo ERA5 historical | Postgame retrospective only |
| Forecast weather | Open-Meteo ECMWF forecast | Safe when forecast_run_ts < cutoff |
| Stadium/venue | Bundled CSV | Fully pre-game safe |
| Travel/timezone | Computed from stadium coords | Fully pre-game safe |
| Team power ratings | Elo computed from PBP | Pre-game Elo is safe; post-game is not |

---

## Additional Table Inventory

### RAW LAYER ADDITIONS

| Table | Grain | Partition | Leakage | Update |
|---|---|---|---|---|
| `raw_market_odds_snapshot` | source+book+event_id+ts+market+selection | season | Opening/intraday: safe; Closing: CLV only | Live: 60min; Hist: batch |
| `raw_weather_hourly_history` | source+stadium+weather_ts | season/game_date | POST-GAME ONLY — never use for pregame | 2 days after game |
| `raw_weather_forecast_hourly` | source+stadium+forecast_run_ts+target_ts | season/game_date | Safe when forecast_run_ts < prediction cutoff | 2x daily |
| `raw_stadium_metadata` | stadium_id+season_start | none | Fully safe | Manual |
| `raw_team_timezone_reference` | team+season | season | Fully safe | Batch/annual |
| `raw_team_travel_reference` | season+home_team+away_team | season | Fully safe | Batch/annual |
| `raw_coach_reference` | team+season+coach_role | season | Fully safe | Manual |

### STAGING LAYER ADDITIONS

| Table | Grain | Key | Purpose | Leakage |
|---|---|---|---|---|
| `stg_market_game_line` | source+book+game_id+ts+market+sel | composite | Standardised odds rows with implied probs | Mixed — check snapshot_type |
| `stg_market_consensus_game` | game_id+ts+market+sel | composite | Book-count consensus + dispersion | Mixed |
| `stg_market_close_game` | game_id+market+sel | (game_id,market,sel) | Official closing lines | CLV/research ONLY |
| `stg_weather_game_hourly` | stadium+game_date+weather_ts+type | composite | Hourly weather; historical + forecast combined | Check weather_type col |
| `stg_weather_game_kickoff` | game_id | game_id | Single kickoff-time weather record | Check weather_source_type |
| `stg_stadium` | stadium_id+season_start | (stadium_id,season_start) | Venue metadata | Fully safe |
| `stg_travel_context` | season+home_team+away_team | composite | Away team travel distance + tz shift | Fully safe |
| `stg_coaching_context` | team+season+coach_role | composite | HC/OC/DC continuity | Fully safe |

### INTERMEDIATE LAYER ADDITIONS

| Table | Grain | Purpose | Leakage |
|---|---|---|---|
| `int_market_open_close` | game_id+market_type+selection | O/C line comparison, key number crossings | opening_*: safe; closing_*: CLV only |
| `int_market_time_buckets` | game_id+market+sel+time_bucket | Line at 6 time horizons | Safe up to bucket boundary hours |
| `int_weather_game` | game_id | Engineered weather features with roof-aware logic | Check weather_source_type |
| `int_team_travel_game` | game_id+team | Distance, tz shift, body clock buckets | Fully safe |
| `int_team_schedule_spot` | game_id+team | Rest days, short/long week, road streak | Safe (except days_until_next) |
| `int_team_power_rating` | game_id+team | Elo + EPA-based power ratings | pre_game_elo: safe; post_game_elo: not |

### MART LAYER ADDITIONS

| Table | Grain | Purpose | Notes |
|---|---|---|---|
| `mart_spread_modeling` | game_id | Side/spread model table | Has both football-only and market features |
| `mart_total_modeling` | game_id | Totals/O-U model table | Weather suppression features included |
| `mart_market_research_game` | game_id+market+sel | CLV analysis, line movement research | **Not for predictive use** |
| `mart_game_modeling` | game_id | Full-featured game model table | Enhanced from v1 |
| `mart_game_modeling_no_market` | game_id | Football-only variant | Safe for market-agnostic models |
| `mart_backtest_game` | game_id+prediction_horizon | 3 rows/game, one per horizon | Full leakage-safe backtest set |
| `mart_backtest_player` | season+week+player_id+horizon | Player backtest with horizon tagging | Extends original |

### DIMENSION TABLE ADDITIONS

| Table | Grain | Purpose |
|---|---|---|
| `dim_stadium` | stadium_id+season_start | Venue metadata dimension |
| `dim_sportsbook` | sportsbook_key | Sharp/square/consensus flags |
| `dim_market_type` | market_type_key | Market category classification |
| `dim_timezone_reference` | team+season | Team home timezone lookup |
| `dim_coach` | team+season+coach_role | Coaching continuity (manual) |

### FACT TABLE ADDITIONS

| Table | Grain | Update | Leakage |
|---|---|---|---|
| `fact_market_game_snapshot` | source+book+game_id+ts+market+sel | Live 60min | Check snapshot_type |
| `fact_market_game_close` | game_id+market+sel | Post-game | CLV only |
| `fact_weather_game_hourly` | stadium+weather_ts+type | Historical: +2d; Forecast: 2x/day | Check weather_type |
| `fact_weather_game_forecast` | stadium+forecast_run_ts+target_ts | 2x daily | Safe per forecast_run_ts |
| `fact_team_travel_game` | game_id+team | Batch/annual | Fully safe |
| `fact_team_schedule_spot` | game_id+team | Weekly | Safe except days_until_next |
| `fact_power_rating_week` | game_id+team | Weekly | pre_game_elo safe; post-game: not |

---

## Leakage Prevention — Full Reference

### The Three Zones

**Zone 1: Always Pre-Game Safe**
- Football rolling metrics (lagged window functions)
- Opening odds lines
- Schedule-derived features (rest, travel, timezone, venue)
- Pre-game Elo ratings
- Forecast weather (when `forecast_run_ts < prediction_cutoff_ts`)
- Initial and mid-week injury reports (by report date)

**Zone 2: Horizon-Conditional**
- Intraday odds snapshots → safe only for models at that prediction horizon
- Injury status changes → safe only if report precedes prediction cutoff
- 72h/24h/6h/1h market bucket lines → safe for models at that horizon

**Zone 3: Never Predictive (Postgame/CLV Research Only)**
- Closing spread, closing total, closing moneyline
- Realized weather (historical observations)
- Post-game Elo ratings
- `days_until_next_game` (future schedule leakage)

### Backtest Horizon Design

```
mart_backtest_game has three rows per game:

  prediction_horizon = 'early_week'
    └── uses: opening_spread, initial injury, 5-day forecast
    └── safe for: Tuesday after schedule release, initial model runs

  prediction_horizon = 'mid_week'
    └── uses: 72h spread, Thursday injury report, 3-day forecast
    └── safe for: Thursday pre-game models

  prediction_horizon = 'pre_kickoff'
    └── uses: 1h spread, final injury status, latest forecast
    └── safe for: kickoff-time automation, live model scoring
```

### Weather Leakage Rule

```sql
-- CORRECT: forecast weather for pregame model
SELECT * FROM int_weather_game
WHERE weather_source_type = 'forecast'
  AND CAST(forecast_run_ts AS TIMESTAMPTZ) < [prediction_cutoff_ts]

-- CORRECT: historical weather for postgame retrospective
SELECT * FROM int_weather_game
WHERE weather_source_type = 'historical'
  -- Use for 'realized' backtest variant only

-- WRONG: DO NOT use historical weather as if it were available pregame
```

---

## Feature Engineering Reference

### Market Features

| Feature | Formula | Safe for Pregame |
|---|---|---|
| `implied_prob_raw` | American≥100: 100/(price+100); else: -price/(-price+100) | Yes (if opening) |
| `no_vig_prob` | prob_a / (prob_a + prob_b) | Yes |
| `opening_home_team_total` | total/2 - spread/2 | Yes |
| `opening_away_team_total` | total/2 + spread/2 | Yes |
| `spread_key_number_distance` | MIN(ABS(line - 3), ABS(line - 7), ...) | Yes |
| `line_volatility_score` | ABS(close - open) / ABS(open) | Closing: No |
| `crossed_key_3_flag` | Line crossed ±3 between open and close | Closing: No |

### Weather Features

| Feature | Description | Pregame |
|---|---|---|
| `eff_wind_mph` | Roof-adjusted wind (0 if dome) | Forecast only |
| `adverse_weather_score` | 0-100 composite (wind + cold + precip) | Forecast only |
| `passing_condition_score` | 0-100 inverse (higher = better passing) | Forecast only |
| `total_suppression_score` | 0-3 integer (wind_20 + cold + precip) | Forecast only |
| `wind_bucket` | calm/mild/moderate/strong/severe/dome | Forecast only |
| `cold_wind_interaction` | temp<32 AND wind>10 | Forecast only |
| `roof_closed_flag` | 1 if dome or confirmed-closed retractable | Safe (static) |

### Travel Features

| Feature | Formula | Pregame |
|---|---|---|
| `travel_km` | Haversine distance from away home stadium | Yes |
| `timezone_shift_hours` | game_utc_offset - away_home_utc_offset | Yes |
| `west_east_early_flag` | west_to_east AND shift≥2 | Yes |
| `travel_distance_bucket` | short/medium/long/cross_country | Yes |

### Power Rating Features

| Feature | Description | Pregame |
|---|---|---|
| `elo_rating_pregame` | Elo before current game | Yes |
| `elo_diff` | home_elo - away_elo | Yes |
| `offensive_power` | Rolling blended EPA offense | Yes |
| `defensive_power` | Rolling blended EPA defense | Yes |
| `total_environment_rating` | off_power + def_power | Yes |

---

## Source Quality Framework

### Odds Source Guidance

| Source | Coverage | Granularity | Cost | Recommended Use |
|---|---|---|---|---|
| nflverse ESPN | 2002+ | Open/close | Free | Long backtests; coarse only |
| The Odds API | 2018+ live | Snapshots hourly | ~$30/month | Current season live feed |
| SBR-style CSV | 2007+ | Open/close | Free | Historical spread backtesting |
| Pinnacle (premium) | 2010+ | Full movement | Paid | Sharp line reference |

### Weather Source Guidance

| Source | Type | Coverage | Cost |
|---|---|---|---|
| Open-Meteo Archive (ERA5) | Historical | 1979+ | Free |
| Open-Meteo Forecast (ECMWF) | Forecast 7-day | Current | Free |
| VisualCrossing | Both | 1970+ | Free tier limited |
| Tomorrow.io | Forecast | Current + 15d | Paid tier for history |

**Key limitation**: Free historical weather is ERA5 reanalysis at ~25km grid resolution. Actual stadium microclimates (especially wind in open stadiums like Arrowhead or Gillette) may differ. For premium kicking/passing condition research, stadium-level data from a paid provider is better.

---

## Incomplete Odds Coverage Handling

The architecture is designed so that missing odds do not break the platform:

1. All mart joins to odds sources use `LEFT JOIN` — missing odds produce NULLs
2. `odds_available`, `market_available`, and `market_available_at_horizon` flags indicate coverage
3. `mart_game_modeling_no_market` provides a complete football-only mart independent of odds availability
4. `mart_backtest_game.market_available_at_horizon = 0` rows train football-only model weights
5. Data tests include diagnostic queries showing odds coverage by season (not hard failures)

**Coverage fallback order** (implemented in staging join logic):
```
1. The Odds API (if enabled + paid)
2. nflverse ESPN lines (free, 2002+)
3. SBR CSV (if manually loaded)
4. NULL (market_available = 0)
```

---

## Automation Schedule Summary

| Job | Trigger | Purpose |
|---|---|---|
| Base refresh | Tue/Thu/Sun 6am UTC | nflverse football data |
| Odds live poll | Sun/Mon/Thu every 2h | Current season line snapshots |
| Weather forecast | Daily 12pm + 6pm UTC | 7-day forecast update |
| Weather historical | +2 days after completed game | ERA5 realized weather fill |
| Full refresh (Tue PM) | Tue 2pm UTC | Complete post-weekend rebuild |
| Reference data | Manual / season start | Stadium, travel matrix |

---

## Migration to Cloud / dbt / Orchestrators

All SQL is standard ANSI (with window functions) — portable to BigQuery, Snowflake, Spark.

**dbt migration path:**
- Each SQL file → one or more dbt models
- `stg_` prefix → `staging/` folder
- `int_` prefix → `intermediate/` folder  
- `mart_` prefix → `marts/` folder
- Add `schema.yml` with column-level `not_null`, `accepted_values`, `relationships` tests
- Replace Python R ingestion → dbt seeds (stadium CSV) + dbt sources + custom ingestion scripts
- Use `dbt build --select tag:daily` for incremental runs
- Add `dbt-expectations` or `elementary` packages for the diagnostic tests

**Airflow / Prefect / Dagster:**
- `ingest_odds()` → Task
- `refresh_weather()` → Task with upstream dependency on `stg_stadium`
- `run_transforms_extended()` → Task group with dependency on all ingest tasks
- Add SLA monitoring on the odds freshness check

**BigQuery:**
- Replace `write_parquet_partition()` → `bq_load` from GCS bucket
- Use partitioned BQ tables with `partition_by = DATE(_PARTITIONTIME)`
- External tables over GCS Parquet work immediately for staging layer
- DuckDB local work + BQ export to Parquet → BQ load is a clean workflow

---

## Known Limitations

1. **Weather wind direction**: Open-Meteo provides `wind_direction_10m` but stadiums vary in orientation. Wind direction relative to the field requires stadium orientation data (manually maintained).

2. **Intraday odds**: Without a paid feed, intraday line movement between open and close is not available. Free sources only provide open + close snapshots.

3. **Sharp vs. square line divergence**: True sharp/square detection requires simultaneous book comparison across Pinnacle, Circa, and other sharp books. Consensus figures smooth this.

4. **Player prop market**: Architecturally supported (`market_type = 'player_prop'`), but no free historical player prop odds source exists. The Odds API or ActionNetwork are needed.

5. **Team totals**: Less consistently available from free sources. The `implied_team_totals` function derives a proxy from spread + total, but this is an approximation vs. actual team total markets.

6. **Referee crews**: No reliable free automated source. Schema is defined in `dim_referee` (placeholder). Manual collection from NFL.com game pages required.

7. **Coach continuity**: Manually maintained CSV. Automating from roster/team pages would require scraping.

8. **Elevation effects**: Denver (Empower Field, 1598m) has documented advantages for kicking and early-season games. `altitude_flag` marks this but the magnitude requires calibration with historical data.
