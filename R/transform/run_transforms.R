# =============================================================================
# TRANSFORM PIPELINE
# Executes SQL staging -> intermediate -> mart layers in order.
# Materialises each view as a persisted Parquet table.
# =============================================================================

source(here::here("R/utils/config_loader.R"))
source(here::here("R/utils/db_connection.R"))
source(here::here("R/utils/parquet_io.R"))
source(here::here("R/utils/logging.R"))

#' Execute a SQL file and materialise result as Parquet
#'
#' @param con DuckDB connection
#' @param sql_file Path to .sql file containing a single SELECT or CREATE VIEW
#' @param output_table Name of the output table (used for Parquet path and view)
#' @param output_dir Base output directory (staging/intermediate/marts)
#' @param partition_cols Column names to partition by (or NULL)
#' @param filter_current_season If TRUE, only reprocess current season rows
#' @param cfg Config list
materialise_table <- function(con,
                               sql_file,
                               output_table,
                               output_dir,
                               partition_cols = "season",
                               filter_current_season = FALSE,
                               cfg = load_config()) {
  log_step(glue::glue("materialise {output_table}"), {
    sql <- readr::read_file(sql_file)

    # Extract the view name from the SQL and create it in DuckDB
    DBI::dbExecute(con, sql)

    # Then query it to collect data
    if (filter_current_season) {
      query <- glue::glue(
        "SELECT * FROM {output_table} WHERE season = {cfg$seasons$current_season}"
      )
    } else {
      query <- glue::glue("SELECT * FROM {output_table}")
    }

    df <- DBI::dbGetQuery(con, query)

    if (nrow(df) == 0) {
      logger::log_warn("{output_table}: 0 rows returned, skipping write.")
      return(invisible(NULL))
    }

    out_path <- file.path(output_dir, output_table)
    write_parquet_partition(df, out_path, partition_cols = partition_cols)

    # Re-register the materialised Parquet as the view source for downstream queries
    register_parquet_view(con, output_table, out_path)
  })
}

#' Run the full staging layer
#'
#' @param con DuckDB connection with raw views registered
#' @param cfg Config list
#' @param incremental If TRUE, only current season
run_staging <- function(con, cfg, incremental = FALSE) {
  logger::log_info("=== STAGING LAYER ===")
  sql_dir <- here::here("sql/staging")
  out_dir <- cfg$paths$staging

  tables_and_files <- list(
    list(file = "stg_games.sql",                   table = "stg_games",               partition = "season"),
    list(file = "stg_plays_drives_teams_players.sql", table = "stg_plays",             partition = "season"),
    list(file = "stg_plays_drives_teams_players.sql", table = "stg_drives",            partition = "season"),
    list(file = "stg_plays_drives_teams_players.sql", table = "stg_teams",             partition = NULL),
    list(file = "stg_plays_drives_teams_players.sql", table = "stg_players",           partition = NULL),
    list(file = "stg_weekly_tables.sql",           table = "stg_player_week",          partition = "season"),
    list(file = "stg_weekly_tables.sql",           table = "stg_team_week",            partition = "season"),
    list(file = "stg_weekly_tables.sql",           table = "stg_rosters_weekly",       partition = "season"),
    list(file = "stg_weekly_tables.sql",           table = "stg_injuries_weekly",      partition = "season"),
    list(file = "stg_weekly_tables.sql",           table = "stg_snap_counts_weekly",   partition = "season"),
    list(file = "stg_weekly_tables.sql",           table = "stg_nextgen_player_week",  partition = "season"),
    list(file = "stg_weekly_tables.sql",           table = "stg_external_odds_game",   partition = "season")
  )

  purrr::walk(tables_and_files, function(t) {
    materialise_table(
      con                   = con,
      sql_file              = file.path(sql_dir, t$file),
      output_table          = t$table,
      output_dir            = out_dir,
      partition_cols        = t$partition,
      filter_current_season = incremental,
      cfg                   = cfg
    )
  })
}

#' Run the intermediate layer
#'
#' @param con DuckDB connection with staging views registered
#' @param cfg Config list
#' @param incremental If TRUE, only current season
run_intermediate <- function(con, cfg, incremental = FALSE) {
  logger::log_info("=== INTERMEDIATE LAYER ===")
  sql_dir <- here::here("sql/intermediate")
  out_dir <- cfg$paths$intermediate

  tables_and_files <- list(
    list(file = "int_game_team.sql",     table = "int_game_base",           partition = "season"),
    list(file = "int_game_team.sql",     table = "int_team_game",           partition = "season"),
    list(file = "int_player_qb.sql",     table = "int_player_game",         partition = "season"),
    list(file = "int_player_qb.sql",     table = "int_player_form",         partition = "season"),
    list(file = "int_player_qb.sql",     table = "int_qb_team_context",     partition = "season"),
    list(file = "int_team_form_injury.sql", table = "int_team_form",        partition = "season"),
    list(file = "int_team_form_injury.sql", table = "int_injury_team_impact", partition = "season")
  )

  purrr::walk(tables_and_files, function(t) {
    materialise_table(
      con                   = con,
      sql_file              = file.path(sql_dir, t$file),
      output_table          = t$table,
      output_dir            = out_dir,
      partition_cols        = t$partition,
      filter_current_season = incremental,
      cfg                   = cfg
    )
  })
}

#' Run the mart layer
#'
#' @param con DuckDB connection with intermediate views registered
#' @param cfg Config list
#' @param incremental If TRUE, only current season
run_marts <- function(con, cfg, incremental = FALSE) {
  logger::log_info("=== MART LAYER ===")
  sql_dir <- here::here("sql/marts")
  out_dir <- cfg$paths$marts

  tables_and_files <- list(
    list(file = "mart_game_team_modeling.sql",   table = "mart_game_modeling",        partition = "season"),
    list(file = "mart_game_team_modeling.sql",   table = "mart_team_week_modeling",   partition = "season"),
    list(file = "mart_player_projections.sql",   table = "mart_player_week_projection", partition = "season"),
    list(file = "mart_player_projections.sql",   table = "mart_qb_projection",        partition = "season"),
    list(file = "mart_player_projections.sql",   table = "mart_receiver_projection",  partition = "season"),
    list(file = "mart_player_projections.sql",   table = "mart_rusher_projection",    partition = "season"),
    list(file = "mart_player_projections.sql",   table = "mart_backtest_game",        partition = "season"),
    list(file = "mart_player_projections.sql",   table = "mart_backtest_player",      partition = "season")
  )

  purrr::walk(tables_and_files, function(t) {
    materialise_table(
      con                   = con,
      sql_file              = file.path(sql_dir, t$file),
      output_table          = t$table,
      output_dir            = out_dir,
      partition_cols        = t$partition,
      filter_current_season = incremental,
      cfg                   = cfg
    )
  })
}

#' Run the complete transformation pipeline
#'
#' @param cfg Config list
#' @param incremental Logical; if TRUE only current season is refreshed
#' @export
run_transforms <- function(cfg = load_config(), incremental = FALSE) {
  con <- db_connect(cfg)
  on.exit(db_disconnect(con), add = TRUE)

  # Register all raw Parquet views
  register_all_raw_views(con, cfg)

  run_staging(con, cfg, incremental = incremental)

  # Re-register staging outputs before intermediate layer
  purrr::walk(
    fs::dir_ls(cfg$paths$staging, type = "directory"),
    ~ register_parquet_view(con, basename(.x), .x)
  )

  run_intermediate(con, cfg, incremental = incremental)

  purrr::walk(
    fs::dir_ls(cfg$paths$intermediate, type = "directory"),
    ~ register_parquet_view(con, basename(.x), .x)
  )

  run_marts(con, cfg, incremental = incremental)
}
