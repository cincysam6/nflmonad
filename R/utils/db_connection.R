# =============================================================================
# DuckDB Connection Manager
# =============================================================================

#' Open (or create) the platform DuckDB database
#'
#' Returns a live DBI connection. Caller is responsible for calling
#' db_disconnect() when done, or use with_db_connection() for scoped access.
#'
#' @param cfg Config list
#' @param read_only Logical, open in read-only mode
#' @return DBI connection object
#' @export
db_connect <- function(cfg, read_only = FALSE) {
  db_path <- cfg$paths$db

  fs::dir_create(dirname(db_path))

  con <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir    = db_path,
    read_only = read_only
  )

  # Apply performance settings
  DBI::dbExecute(con, glue::glue("SET memory_limit='{cfg$duckdb$memory_limit}';"))
  DBI::dbExecute(con, glue::glue("SET threads TO {cfg$duckdb$threads};"))

  tmp_dir <- cfg$duckdb$temp_directory
  fs::dir_create(tmp_dir)
  DBI::dbExecute(con, glue::glue("SET temp_directory='{tmp_dir}';"))

  # Register the httpfs extension for potential remote parquet reads
  tryCatch(
    DBI::dbExecute(con, "LOAD httpfs;"),
    error = function(e) {
      logger::log_warn("httpfs extension not available; remote parquet reads disabled.")
    }
  )

  logger::log_info("DuckDB connected: {db_path}")
  con
}

#' Close a DuckDB connection
#'
#' @param con DBI connection
#' @export
db_disconnect <- function(con) {
  DBI::dbDisconnect(con, shutdown = TRUE)
  logger::log_info("DuckDB disconnected.")
}

#' Execute a block of code with a scoped DB connection
#'
#' Automatically disconnects on exit, even on error.
#'
#' @param cfg Config list
#' @param expr Expression to evaluate with `con` in scope
#' @param read_only Logical
#' @export
with_db_connection <- function(cfg, expr, read_only = FALSE) {
  con <- db_connect(cfg, read_only = read_only)
  on.exit(db_disconnect(con), add = TRUE)
  force(expr)
}

# ---- Parquet view registration -----------------------------------------------

#' Register a Hive-partitioned Parquet directory as a DuckDB view
#'
#' @param con DuckDB DBI connection
#' @param view_name Name for the DuckDB view
#' @param parquet_path Directory containing partitioned parquet files
#' @param hive_partition Logical; treat subdirs as Hive partitions
#' @export
register_parquet_view <- function(con,
                                  view_name,
                                  parquet_path,
                                  hive_partition = TRUE) {
  if (!fs::dir_exists(parquet_path)) {
    logger::log_warn("Parquet path does not exist, skipping view: {parquet_path}")
    return(invisible(NULL))
  }

  hp_flag <- if (hive_partition) "hive_partitioning=true" else ""

  sql <- glue::glue(
    "CREATE OR REPLACE VIEW {view_name} AS ",
    "SELECT * FROM read_parquet('{parquet_path}/**/*.parquet', {hp_flag});"
  )

  DBI::dbExecute(con, sql)
  logger::log_info("Registered view: {view_name} -> {parquet_path}")
}

#' Register all raw layer Parquet directories as DuckDB views
#'
#' @param con DuckDB DBI connection
#' @param cfg Config list
#' @export
register_all_raw_views <- function(con, cfg) {
  raw_tables <- c(
    "pbp", "schedules", "player_stats", "team_stats", "participation",
    "players", "rosters", "rosters_weekly", "depth_charts", "injuries",
    "nextgen_stats", "pfr_advstats", "snap_counts", "ftn_charting",
    "ff_opportunity", "external_odds"
  )

  purrr::walk(raw_tables, function(tbl) {
    path <- file.path(cfg$paths$raw, tbl)
    register_parquet_view(con, paste0("raw_", tbl), path)
  })

  invisible(NULL)
}

#' Register all staging / intermediate / mart Parquet dirs as views
#'
#' @param con DuckDB DBI connection
#' @param cfg Config list
#' @export
register_all_views <- function(con, cfg) {
  register_all_raw_views(con, cfg)

  layers <- list(
    staging     = cfg$paths$staging,
    intermediate = cfg$paths$intermediate,
    marts       = cfg$paths$marts
  )

  purrr::iwalk(layers, function(base_path, layer_prefix) {
    if (!fs::dir_exists(base_path)) return(invisible(NULL))
    subdirs <- fs::dir_ls(base_path, type = "directory")
    purrr::walk(subdirs, function(d) {
      tbl_name <- basename(d)
      register_parquet_view(con, tbl_name, d)
    })
  })

  invisible(NULL)
}
