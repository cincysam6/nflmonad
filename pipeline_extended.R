# =============================================================================
# pipeline_extended.R
# Extended pipeline orchestrator for external data sources.
# Runs AFTER the base nflverse pipeline.
# =============================================================================

# Load all modules
modules <- c(
  "R/utils/config_loader.R",
  "R/utils/logging.R",
  "R/utils/parquet_io.R",
  "R/utils/db_connection.R",
  "config/config_loader_extended.R",
  "R/ingest/source_contracts.R",
  "R/ingest/ingest_odds.R",
  "R/ingest/ingest_weather.R",
  "R/ingest/ingest_reference.R",
  "R/transform/run_transforms.R",
  "R/utils/run_tests.R"
)

purrr::walk(modules, ~ source(here::here(.x)))

# =============================================================================
# REFERENCE DATA REFRESH (run once per season or when venues change)
# =============================================================================

#' Refresh all static reference tables
#' @export
refresh_reference_data <- function(cfg = load_config_extended()) {
  log_step("ingest_stadium_metadata", ingest_stadium_metadata(cfg))
  log_step("ingest_team_timezone",    ingest_team_timezone(cfg))
  log_step("ingest_coaching_context", ingest_coaching_context(cfg = cfg))

  # Compute and write travel matrix
  log_step("compute_travel_matrix", {
    travel_df <- compute_travel_matrix(cfg) |>
      add_ingestion_metadata("travel_matrix", "computed")
    base_path <- file.path(cfg$paths$raw, "team_travel_reference")
    write_parquet_partition(travel_df, base_path, partition_cols = "season")
  })
}

# =============================================================================
# ODDS REFRESH
# =============================================================================

#' Refresh odds data from all enabled sources
#'
#' @param seasons Integer vector of seasons (NULL = config-based)
#' @param cfg Extended config
#' @param live_only If TRUE, only poll live/upcoming games (faster)
#' @export
refresh_odds <- function(seasons   = NULL,
                         cfg       = load_config_extended(),
                         live_only = FALSE) {
  active_srcs <- active_odds_sources(cfg)

  if (length(active_srcs) == 0) {
    logger::log_info("No odds sources enabled. Skipping odds refresh.")
    return(invisible(NULL))
  }

  logger::log_info("Active odds sources: {paste(names(active_srcs), collapse=', ')}")

  # nflverse lines (historical batch)
  if (!live_only && !is.null(active_srcs$nflverse_lines)) {
    log_step("odds_nflverse", ingest_odds_nflverse(seasons = seasons, cfg = cfg))
  }

  # The Odds API live snapshot
  if (!is.null(active_srcs$the_odds_api)) {
    log_step("odds_the_odds_api_live", ingest_odds_the_odds_api_live(cfg = cfg))
  }

  invisible(NULL)
}

# =============================================================================
# WEATHER REFRESH
# =============================================================================

#' Refresh weather data — historical for completed games, forecast for upcoming
#'
#' @param cfg Extended config
#' @param include_historical Refresh historical realized weather (postgame only)
#' @param include_forecast Refresh forecast for upcoming games
#' @export
refresh_weather <- function(cfg                 = load_config_extended(),
                             include_historical  = TRUE,
                             include_forecast    = TRUE) {
  if (length(active_weather_sources(cfg)) == 0) {
    logger::log_info("No weather sources enabled. Skipping.")
    return(invisible(NULL))
  }

  # Load stadium metadata
  stadiums <- load_stadium_metadata(cfg)

  # Load games from staging
  con <- db_connect(cfg, read_only = TRUE)
  on.exit(db_disconnect(con), add = TRUE)
  register_all_views(con, cfg)

  all_games <- tryCatch(
    DBI::dbGetQuery(con,
      "SELECT g.game_id, g.season, g.week, g.game_date, g.kickoff_time_local,
              g.home_team, g.roof, g.game_completed_flag
       FROM stg_games g
       WHERE g.season >= 2016 AND g.season_type IN ('regular','postseason')"
    ),
    error = function(e) {
      logger::log_warn("Could not load games for weather: {e$message}")
      NULL
    }
  )

  if (is.null(all_games) || nrow(all_games) == 0) {
    logger::log_warn("No games found for weather refresh.")
    return(invisible(NULL))
  }

  # Join stadium coordinates
  games_with_stadium <- join_stadium_to_games(all_games, stadiums)

  if (include_historical) {
    completed_games <- games_with_stadium |>
      dplyr::filter(game_completed_flag == 1, !is.na(latitude))

    log_step("weather_historical", {
      ingest_weather_historical(completed_games, cfg = cfg, force_full = FALSE)
    })
  }

  if (include_forecast) {
    upcoming_games <- games_with_stadium |>
      dplyr::filter(
        game_completed_flag == 0,
        !is.na(latitude),
        !is.na(game_date),
        as.Date(game_date) >= Sys.Date(),
        as.Date(game_date) <= Sys.Date() + 7
      )

    log_step("weather_forecast", {
      ingest_weather_forecast(upcoming_games, cfg = cfg)
    })
  }
}

# =============================================================================
# EXTENDED TRANSFORM PIPELINE
# Runs SQL views for new external layers + materialises to Parquet
# =============================================================================

#' Run extended SQL transforms (market, weather, travel, power ratings)
#'
#' @param cfg Extended config
#' @param incremental Only rebuild current season rows
#' @export
run_transforms_extended <- function(cfg = load_config_extended(),
                                     incremental = FALSE) {
  con <- db_connect(cfg)
  on.exit(db_disconnect(con), add = TRUE)

  register_all_views(con, cfg)

  logger::log_info("=== EXTENDED STAGING ===")
  staging_sql_dir <- here::here("sql/staging")
  ext_staging <- list(
    list(file = "stg_market_odds.sql",
         tables = c("stg_market_game_line","stg_market_consensus_game","stg_market_close_game")),
    list(file = "stg_weather_stadium_travel.sql",
         tables = c("stg_weather_game_hourly","stg_weather_game_kickoff",
                    "stg_stadium","stg_travel_context","stg_coaching_context"))
  )

  purrr::walk(ext_staging, function(grp) {
    sql <- readr::read_file(file.path(staging_sql_dir, grp$file))
    DBI::dbExecute(con, sql)
    purrr::walk(grp$tables, function(tbl) {
      tryCatch({
        df <- DBI::dbGetQuery(con, glue::glue(
          if (incremental) "SELECT * FROM {tbl} WHERE season = {cfg$seasons$current_season}"
          else             "SELECT * FROM {tbl}"
        ))
        if (nrow(df) > 0) {
          out_path <- file.path(cfg$paths$staging, tbl)
          write_parquet_partition(df, out_path,
                                  partition_cols = if ("season" %in% names(df)) "season" else NULL)
          register_parquet_view(con, tbl, out_path)
          logger::log_info("Extended staging: {tbl} ({nrow(df)} rows)")
        }
      }, error = function(e) {
        logger::log_warn("Extended staging {tbl} failed: {e$message}")
      })
    })
  })

  logger::log_info("=== EXTENDED INTERMEDIATE ===")
  int_sql_dir <- here::here("sql/intermediate")
  ext_int <- list(
    list(file = "int_market_power.sql",
         tables = c("int_market_open_close","int_market_time_buckets","int_team_power_rating")),
    list(file = "int_weather_travel_schedule.sql",
         tables = c("int_weather_game","int_team_travel_game","int_team_schedule_spot"))
  )

  purrr::walk(ext_int, function(grp) {
    sql <- readr::read_file(file.path(int_sql_dir, grp$file))
    DBI::dbExecute(con, sql)
    purrr::walk(grp$tables, function(tbl) {
      tryCatch({
        df <- DBI::dbGetQuery(con, glue::glue(
          if (incremental) "SELECT * FROM {tbl} WHERE season = {cfg$seasons$current_season}"
          else             "SELECT * FROM {tbl}"
        ))
        if (nrow(df) > 0) {
          out_path <- file.path(cfg$paths$intermediate, tbl)
          write_parquet_partition(df, out_path,
                                  partition_cols = if ("season" %in% names(df)) "season" else NULL)
          register_parquet_view(con, tbl, out_path)
          logger::log_info("Extended intermediate: {tbl} ({nrow(df)} rows)")
        }
      }, error = function(e) {
        logger::log_warn("Extended intermediate {tbl} failed: {e$message}")
      })
    })
  })

  logger::log_info("=== EXTENDED MARTS ===")
  mart_sql_dir <- here::here("sql/marts")
  ext_marts <- list(
    list(file = "mart_spread_total_market.sql",
         tables = c("mart_spread_modeling","mart_total_modeling","mart_market_research_game")),
    list(file = "mart_game_modeling_enhanced.sql",
         tables = c("mart_game_modeling","mart_game_modeling_no_market")),
    list(file = "mart_backtest_enhanced.sql",
         tables = c("mart_backtest_game","mart_backtest_player"))
  )

  purrr::walk(ext_marts, function(grp) {
    sql <- readr::read_file(file.path(mart_sql_dir, grp$file))
    DBI::dbExecute(con, sql)
    purrr::walk(grp$tables, function(tbl) {
      tryCatch({
        df <- DBI::dbGetQuery(con, glue::glue(
          if (incremental) "SELECT * FROM {tbl} WHERE season = {cfg$seasons$current_season}"
          else             "SELECT * FROM {tbl}"
        ))
        if (nrow(df) > 0) {
          out_path <- file.path(cfg$paths$marts, tbl)
          write_parquet_partition(df, out_path,
                                  partition_cols = if ("season" %in% names(df)) "season" else NULL)
          register_parquet_view(con, tbl, out_path)
          logger::log_info("Extended mart: {tbl} ({nrow(df)} rows)")
        }
      }, error = function(e) {
        logger::log_warn("Extended mart {tbl} failed: {e$message}")
      })
    })
  })
}

# =============================================================================
# DAILY EXTENDED REFRESH
# =============================================================================

#' Run daily extended refresh (odds + weather + transforms + tests)
#'
#' @param cfg Extended config
#' @export
run_daily_refresh_extended <- function(cfg = load_config_extended()) {
  setup_logging(cfg, context = "daily_extended")
  ensure_directories(cfg)

  t_start <- proc.time()
  logger::log_info("===================================")
  logger::log_info("DAILY EXTENDED REFRESH — {Sys.Date()}")
  logger::log_info("===================================")

  # 1. Base nflverse pipeline (from original pipeline.R)
  source(here::here("pipeline.R"))
  run_daily_refresh(cfg = cfg)

  # 2. Odds refresh
  log_step("refresh_odds", refresh_odds(cfg = cfg, live_only = TRUE))

  # 3. Weather refresh (historical + forecast)
  log_step("refresh_weather", refresh_weather(cfg = cfg))

  # 4. Extended transforms
  log_step("run_transforms_extended",
    run_transforms_extended(cfg = cfg, incremental = TRUE))

  # 5. Extended data tests
  log_step("data_tests_extended", {
    test_sql <- readr::read_file(here::here("sql/tests/data_tests_extended.sql"))
    con <- db_connect(cfg, read_only = TRUE)
    on.exit(db_disconnect(con), add = TRUE)
    register_all_views(con, cfg)

    test_blocks <- stringr::str_split(test_sql, "\n\n")[[1]] |>
      purrr::keep(~ stringr::str_detect(.x, "SELECT"))

    failures <- purrr::map_dfr(test_blocks, function(block) {
      tryCatch({
        rows <- DBI::dbGetQuery(con, block)
        if (nrow(rows) > 0) {
          logger::log_warn("EXT TEST FAILED: {nrow(rows)} rows")
          rows
        } else tibble::tibble()
      }, error = function(e) tibble::tibble())
    })

    if (nrow(failures) > 0) {
      logger::log_error("{nrow(failures)} extended test failures.")
    }
  })

  elapsed <- round((proc.time() - t_start)[["elapsed"]] / 60, 1)
  logger::log_info("EXTENDED REFRESH COMPLETE in {elapsed} min.")
  prune_logs(cfg)
}

# =============================================================================
# CLI
# Rscript pipeline_extended.R daily
# Rscript pipeline_extended.R backfill
# Rscript pipeline_extended.R reference
# =============================================================================

if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  mode <- args[1]
  cfg  <- load_config_extended()

  if (mode == "daily")     run_daily_refresh_extended(cfg)
  else if (mode == "reference") refresh_reference_data(cfg)
  else if (mode == "backfill") {
    refresh_reference_data(cfg)
    refresh_odds(cfg = cfg)
    refresh_weather(cfg = cfg, include_historical = TRUE, include_forecast = FALSE)
    run_transforms_extended(cfg = cfg, incremental = FALSE)
  } else {
    cat("Usage: Rscript pipeline_extended.R [daily|backfill|reference]\n")
    quit(status = 1)
  }
}
