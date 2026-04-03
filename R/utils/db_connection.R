# =============================================================================
# DuckDB Connection Manager
# Fixed: register_parquet_view now skips gracefully when folder exists but
#        contains no parquet files (avoids "No files found" IO error).
# =============================================================================

#' Open (or create) the platform DuckDB database
#' @param cfg Config list
#' @param read_only Logical
#' @return DBI connection object
#' @export
db_connect <- function(cfg, read_only = FALSE) {
  db_path <- cfg$paths$db
  fs::dir_create(dirname(db_path))

  con <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir     = db_path,
    read_only = read_only
  )

  DBI::dbExecute(con, glue::glue("SET memory_limit='{cfg$duckdb$memory_limit}';"))
  DBI::dbExecute(con, glue::glue("SET threads TO {cfg$duckdb$threads};"))

  tmp_dir <- cfg$duckdb$temp_directory
  fs::dir_create(tmp_dir)
  DBI::dbExecute(con, glue::glue("SET temp_directory='{tmp_dir}';"))

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
#' @param con DBI connection
#' @export
db_disconnect <- function(con) {
  DBI::dbDisconnect(con, shutdown = TRUE)
  logger::log_info("DuckDB disconnected.")
}

#' Execute a block of code with a scoped DB connection
#' @export
with_db_connection <- function(cfg, expr, read_only = FALSE) {
  con <- db_connect(cfg, read_only = read_only)
  on.exit(db_disconnect(con), add = TRUE)
  force(expr)
}

# ---- Parquet view registration -----------------------------------------------

#' Check whether a directory actually contains at least one parquet file
#' (recursively, to handle hive-partitioned subdirs)
.has_parquet_files <- function(path) {
  if (!fs::dir_exists(path)) return(FALSE)
  files <- tryCatch(
    fs::dir_ls(path, recurse = TRUE, glob = "*.parquet"),
    error = function(e) character(0)
  )
  length(files) > 0
}

#' Register a Hive-partitioned Parquet directory as a DuckDB view
#'
#' Skips silently if the directory does not exist OR contains no parquet files.
#' This prevents "No files found" IO errors when some sources haven't been
#' ingested yet (e.g. team_stats, participation, external_odds).
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
  if (!.has_parquet_files(parquet_path)) {
    logger::log_warn("No parquet files found, skipping view: {view_name} ({parquet_path})")
    return(invisible(NULL))
  }

  hp_flag <- if (hive_partition) "hive_partitioning=true" else ""

  sql <- glue::glue(
    "CREATE OR REPLACE VIEW {view_name} AS ",
    "SELECT * REPLACE (CAST(season AS INTEGER) AS season) ",
    "FROM read_parquet('{parquet_path}/**/*.parquet', {hp_flag});"
  )

  tryCatch(
    {
      DBI::dbExecute(con, sql)
      logger::log_info("Registered view: {view_name} -> {parquet_path}")
    },
    error = function(e) {
      logger::log_warn("Could not register view {view_name}: {e$message}")
    }
  )

  invisible(NULL)
}

#' Register all raw layer Parquet directories as DuckDB views
#' Only registers tables that actually have parquet files on disk.
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

  registered <- 0L
  skipped    <- 0L

  purrr::walk(raw_tables, function(tbl) {
    path <- file.path(cfg$paths$raw, tbl)
    if (.has_parquet_files(path)) {
      register_parquet_view(con, paste0("raw_", tbl), path)
      registered <<- registered + 1L
    } else {
      logger::log_warn("Skipping raw_{tbl}: no parquet files at {path}")
      skipped <<- skipped + 1L
    }
  })

  logger::log_info(
    "Raw views: {registered} registered, {skipped} skipped (not yet ingested)."
  )
  invisible(NULL)
}

#' Register all staging / intermediate / mart Parquet dirs as views
#' @param con DuckDB DBI connection
#' @param cfg Config list
#' @export
register_all_views <- function(con, cfg) {
  register_all_raw_views(con, cfg)

  layers <- list(
    staging      = cfg$paths$staging,
    intermediate = cfg$paths$intermediate,
    marts        = cfg$paths$marts
  )

  purrr::iwalk(layers, function(base_path, layer_name) {
    if (!fs::dir_exists(base_path)) return(invisible(NULL))
    subdirs <- fs::dir_ls(base_path, type = "directory")
    purrr::walk(subdirs, function(d) {
      if (.has_parquet_files(d)) {
        register_parquet_view(con, basename(d), d)
      }
    })
  })

  invisible(NULL)
}
