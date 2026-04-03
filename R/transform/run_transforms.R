# =============================================================================
# TRANSFORM PIPELINE  (v3 — verified column names, cache-busting DROP)
# =============================================================================

source(here::here("R/utils/config_loader.R"))
source(here::here("R/utils/db_connection.R"))
source(here::here("R/utils/parquet_io.R"))
source(here::here("R/utils/logging.R"))

# ---- Drop all stale views from DuckDB catalog before rebuilding -------------
# DuckDB persists CREATE OR REPLACE VIEW definitions in the .duckdb file.
# If a prior run wrote a broken view, it stays cached and re-executes even
# when the SQL file on disk is fixed. This function clears the slate.
.drop_all_platform_views <- function(con) {
  views_to_drop <- c(
    # Staging
    "stg_games", "stg_plays", "stg_drives", "stg_teams", "stg_players",
    "stg_player_week", "stg_team_week", "stg_rosters_weekly",
    "stg_injuries_weekly", "stg_snap_counts_weekly", "stg_nextgen_player_week",
    "stg_external_odds_game",
    # Raw views
    "raw_pbp", "raw_schedules", "raw_player_stats", "raw_team_stats",
    "raw_participation", "raw_players", "raw_rosters", "raw_rosters_weekly",
    "raw_depth_charts", "raw_injuries", "raw_nextgen_stats", "raw_pfr_advstats",
    "raw_snap_counts", "raw_ftn_charting", "raw_ff_opportunity", "raw_external_odds",
    # Intermediate
    "int_game_base", "int_team_game", "int_player_game", "int_player_form",
    "int_qb_team_context", "int_team_form", "int_injury_team_impact",
    # Marts
    "mart_game_modeling", "mart_team_week_modeling", "mart_player_week_projection",
    "mart_qb_projection", "mart_receiver_projection", "mart_rusher_projection",
    "mart_backtest_game", "mart_backtest_player"
  )

  dropped <- 0L
  purrr::walk(views_to_drop, function(v) {
    tryCatch({
      DBI::dbExecute(con, glue::glue("DROP VIEW IF EXISTS {v};"))
      dropped <<- dropped + 1L
    }, error = function(e) invisible(NULL))
  })
  logger::log_info("Dropped {dropped} stale views from DuckDB catalog.")
}

# ---- Build team_stats from raw_pbp parquet (no re-download) ----------------
.build_team_stats_from_raw_pbp <- function(con, cfg) {
  base_path <- file.path(cfg$paths$raw, "team_stats")
  pbp_path  <- file.path(cfg$paths$raw, "pbp")

  if (!.has_parquet_files(pbp_path)) {
    logger::log_warn("team_stats: raw_pbp not found, skipping.")
    return(invisible(NULL))
  }

  existing_seasons <- character(0)
  if (fs::dir_exists(base_path)) {
    season_dirs      <- fs::dir_ls(base_path, regexp = "season=\\d{4}$", type = "directory")
    existing_seasons <- stringr::str_extract(basename(season_dirs), "\\d{4}")
  }

  pbp_season_dirs <- fs::dir_ls(pbp_path, regexp = "season=\\d{4}$", type = "directory")
  pbp_seasons     <- as.integer(stringr::str_extract(basename(pbp_season_dirs), "\\d{4}"))
  incremental     <- as.character(cfg$seasons$incremental_seasons)

  seasons_needed <- pbp_seasons[
    !as.character(pbp_seasons) %in% existing_seasons |
     as.character(pbp_seasons) %in% incremental
  ]

  if (length(seasons_needed) == 0) {
    logger::log_info("team_stats: all seasons already built, skipping.")
    return(invisible(NULL))
  }

  logger::log_info("team_stats: building for seasons: {paste(seasons_needed, collapse=', ')}")

  purrr::walk(seasons_needed, function(s) {
    log_step(glue::glue("build team_stats season={s}"), {
      season_pbp_path <- file.path(pbp_path, glue::glue("season={s}"))
      if (!.has_parquet_files(season_pbp_path)) {
        logger::log_warn("team_stats season={s}: no PBP parquet, skipping.")
        return(invisible(NULL))
      }

      pbp <- arrow::open_dataset(season_pbp_path) |> dplyr::collect()

      df <- tryCatch(
        nflfastR::calculate_series_conversion_rates(pbp),
        error = function(e) {
          logger::log_warn("team_stats season={s}: {e$message}")
          NULL
        }
      )

      if (is.null(df) || nrow(df) == 0) {
        logger::log_warn("team_stats season={s}: 0 rows, skipping.")
        return(invisible(NULL))
      }

      df |>
        dplyr::mutate(season = as.integer(s)) |>
        add_ingestion_metadata("nflfastR::calculate_series_conversion_rates",
                               "computed_from_raw_pbp",
                               as.character(packageVersion("nflfastR"))) |>
        write_parquet_partition(base_path, partition_cols = "season")
    })
  })
  invisible(base_path)
}

# ---- Core materialise -------------------------------------------------------
materialise_table <- function(con, sql_file, output_table, output_dir,
                               partition_cols = "season",
                               filter_current_season = FALSE,
                               cfg = load_config()) {
  log_step(glue::glue("materialise {output_table}"), {
    tryCatch({
      sql <- readr::read_file(sql_file)
      DBI::dbExecute(con, sql)

      query <- if (filter_current_season) {
        glue::glue("SELECT * FROM {output_table} WHERE season = {cfg$seasons$current_season}")
      } else {
        glue::glue("SELECT * FROM {output_table}")
      }

      df <- DBI::dbGetQuery(con, query)

      if (nrow(df) == 0) {
        logger::log_warn("{output_table}: 0 rows returned, skipping write.")
        return(invisible(NULL))
      }

      out_path <- file.path(output_dir, output_table)
      if (!filter_current_season && fs::dir_exists(out_path)) {
        fs::dir_delete(out_path)
        logger::log_info("{output_table}: cleared existing output directory for full rebuild.")
      }
      write_parquet_partition(df, out_path, partition_cols = partition_cols)
      register_parquet_view(con, output_table, out_path)
      logger::log_info("{output_table}: {nrow(df)} rows written.")

    }, error = function(e) {
      logger::log_error("FAILED materialise {output_table}: {e$message}")
    })
  })
}

.reregister_layer <- function(con, layer_path) {
  if (!fs::dir_exists(layer_path)) return(invisible(NULL))
  purrr::walk(fs::dir_ls(layer_path, type = "directory"), function(d) {
    if (.has_parquet_files(d)) register_parquet_view(con, basename(d), d)
  })
}

# ---- Staging ----------------------------------------------------------------
run_staging <- function(con, cfg, incremental = FALSE) {
  logger::log_info("=== STAGING LAYER ===")
  sql_dir <- here::here("sql/staging")
  out_dir <- cfg$paths$staging

  tables <- list(
    list(file = "stg_games.sql",                       table = "stg_games",              partition = "season"),
    list(file = "stg_plays_drives_teams_players.sql",  table = "stg_plays",              partition = "season"),
    list(file = "stg_plays_drives_teams_players.sql",  table = "stg_drives",             partition = "season"),
    list(file = "stg_plays_drives_teams_players.sql",  table = "stg_teams",              partition = NULL),
    list(file = "stg_plays_drives_teams_players.sql",  table = "stg_players",            partition = NULL),
    list(file = "stg_weekly_tables.sql",               table = "stg_player_week",        partition = "season"),
    list(file = "stg_weekly_tables.sql",               table = "stg_team_week",          partition = "season"),
    list(file = "stg_weekly_tables.sql",               table = "stg_rosters_weekly",     partition = "season"),
    list(file = "stg_weekly_tables.sql",               table = "stg_injuries_weekly",    partition = "season"),
    list(file = "stg_weekly_tables.sql",               table = "stg_snap_counts_weekly", partition = "season"),
    list(file = "stg_weekly_tables.sql",               table = "stg_nextgen_player_week",partition = "season"),
    list(file = "stg_external_odds_game.sql",           table = "stg_external_odds_game", partition = "season")
  )

  purrr::walk(tables, function(t) {
    materialise_table(con, file.path(sql_dir, t$file), t$table, out_dir,
                      t$partition, incremental, cfg)
  })

  # Ensure stg_external_odds_game view exists (empty stub) so mart layer
  # can LEFT JOIN to it even when no odds data has been ingested yet.
  tryCatch(
    DBI::dbGetQuery(con, "SELECT 1 FROM stg_external_odds_game LIMIT 0"),
    error = function(e) {
      logger::log_info("Creating empty stub for stg_external_odds_game (no odds data ingested).")
      DBI::dbExecute(con, "
        CREATE OR REPLACE VIEW stg_external_odds_game AS
        SELECT
          NULL::VARCHAR   AS game_id,
          NULL::INTEGER   AS season,
          NULL::INTEGER   AS week,
          NULL::DATE      AS game_date,
          NULL::VARCHAR   AS home_team,
          NULL::VARCHAR   AS away_team,
          NULL::TIMESTAMP AS market_timestamp,
          NULL::VARCHAR   AS sportsbook,
          NULL::VARCHAR   AS market_type,
          NULL::DOUBLE    AS home_spread,
          NULL::DOUBLE    AS away_spread,
          NULL::DOUBLE    AS spread_juice_home,
          NULL::DOUBLE    AS spread_juice_away,
          NULL::DOUBLE    AS total_line,
          NULL::DOUBLE    AS over_juice,
          NULL::DOUBLE    AS under_juice,
          NULL::INTEGER   AS home_ml,
          NULL::INTEGER   AS away_ml,
          NULL::DOUBLE    AS opening_spread,
          NULL::DOUBLE    AS opening_total,
          NULL::DOUBLE    AS closing_spread,
          NULL::DOUBLE    AS closing_total,
          NULL::TIMESTAMP AS ingestion_ts
        WHERE FALSE
      ")
    }
  )
}

# ---- Intermediate -----------------------------------------------------------
run_intermediate <- function(con, cfg, incremental = FALSE) {
  logger::log_info("=== INTERMEDIATE LAYER ===")
  sql_dir <- here::here("sql/intermediate")
  out_dir <- cfg$paths$intermediate

  tables <- list(
    list(file = "int_game_team.sql",        table = "int_game_base",          partition = "season"),
    list(file = "int_game_team.sql",        table = "int_team_game",          partition = "season"),
    list(file = "int_team_form_injury.sql", table = "int_team_form",          partition = "season"),
    list(file = "int_team_form_injury.sql", table = "int_injury_team_impact", partition = "season"),
    list(file = "int_player_qb.sql",        table = "int_player_game",        partition = "season"),
    list(file = "int_player_qb.sql",        table = "int_player_form",        partition = "season"),
    list(file = "int_player_qb.sql",        table = "int_qb_team_context",    partition = "season")
  )

  purrr::walk(tables, function(t) {
    materialise_table(con, file.path(sql_dir, t$file), t$table, out_dir,
                      t$partition, incremental, cfg)
  })
}




# ---- Master entry point -----------------------------------------------------
#' @export
run_transforms <- function(cfg = load_config(), incremental = FALSE) {
  con <- db_connect(cfg)
  on.exit(db_disconnect(con), add = TRUE)
  
  .drop_all_platform_views(con)
  log_step("build_team_stats", .build_team_stats_from_raw_pbp(con, cfg))
  register_all_raw_views(con, cfg)
  
  run_staging(con, cfg, incremental = incremental)
  .reregister_layer(con, cfg$paths$staging)
  
  run_intermediate(con, cfg, incremental = incremental)
  .reregister_layer(con, cfg$paths$intermediate)
  
  # ---- Recast all tables that feed mart_player_week_projection ----
  # Arrow reads Hive partition season=YYYY as BIGINT — recast to INTEGER
  # and normalize identifier columns to VARCHAR.
  .recast <- function(tbl, path, id_cols = character()) {
    p <- gsub("\\\\", "/", normalizePath(path, mustWork = FALSE))
    id_cols <- unique(id_cols)
    id_sql <- if (length(id_cols) > 0) {
      paste0("CAST(", id_cols, " AS VARCHAR) AS ", id_cols, collapse = ", ")
    } else ""
    select_prefix <- paste(
      c(id_sql, "CAST(season AS INTEGER) AS season", "CAST(week AS INTEGER) AS week"),
      collapse = ", "
    )
    ex  <- paste(c(id_cols, "season", "week"), collapse = ", ")
    sql <- paste0("CREATE OR REPLACE VIEW ", tbl, " AS SELECT ", select_prefix, ", ",
                  "* EXCLUDE (", ex, ") ",
                  "FROM read_parquet('", p, "/**/*.parquet', hive_partitioning=true)")
    tryCatch(
      { DBI::dbExecute(con, sql); logger::log_info("{tbl} recast OK") },
      error = function(e) logger::log_warn("{tbl} recast FAILED: {e$message}")
    )
  }
  
  .recast("int_player_form",        file.path(cfg$paths$intermediate, "int_player_form"),         c("player_id"))
  .recast("int_player_game",        file.path(cfg$paths$intermediate, "int_player_game"),         c("player_id"))
  .recast("stg_team_week",          file.path(cfg$paths$staging,      "stg_team_week"))
  .recast("stg_nextgen_player_week",file.path(cfg$paths$staging,      "stg_nextgen_player_week"), c("player_id"))
  .recast("int_injury_team_impact", file.path(cfg$paths$intermediate, "int_injury_team_impact"))
  .recast("raw_injuries",           file.path(cfg$paths$raw,          "raw_injuries"),            c("gsis_id"))
  
  # 6. Marts intentionally skipped.
  logger::log_info("Marts step skipped in run_transforms; build marts manually.")
  
  logger::log_info("run_transforms complete.")
}
