# =============================================================================
# MASTER PIPELINE ORCHESTRATOR
# Entry point for full backfill and daily incremental refresh.
# =============================================================================

# Load all modules
modules <- c(
  "R/utils/config_loader.R",
  "R/utils/logging.R",
  "R/utils/parquet_io.R",
  "R/utils/db_connection.R",
  "R/ingest/ingest_pbp.R",
  "R/ingest/ingest_all_sources.R",
  "R/transform/run_transforms.R",
  "R/utils/run_tests.R"
)

purrr::walk(modules, ~ source(here::here(.x)))

# =============================================================================
# BACKFILL PIPELINE
# Purpose: Load all historical seasons from scratch.
# Run once; subsequent runs are incremental unless force_full=TRUE.
# =============================================================================

#' Run full historical backfill
#'
#' @param cfg Config list
#' @param force_full Logical; if TRUE re-download all seasons even if they exist
#' @export
run_backfill <- function(cfg = load_config(), force_full = FALSE) {
  setup_logging(cfg, context = "backfill")
  ensure_directories(cfg)

  logger::log_info("============================================================")
  logger::log_info("STARTING BACKFILL PIPELINE")
  logger::log_info("Seasons: {cfg$seasons$historical_start} - {cfg$seasons$current_season}")
  logger::log_info("============================================================")

  t_start <- proc.time()

  # ---- 1. BRONZE: Ingest all source tables ------------------------------------
  log_step("ingest_schedules",      ingest_schedules(cfg = cfg, force_full = force_full))
  log_step("ingest_pbp",            ingest_pbp(cfg = cfg, force_full = force_full))
  log_step("ingest_player_stats",   ingest_player_stats(cfg = cfg, force_full = force_full))
  log_step("ingest_players",        ingest_players(cfg = cfg))
  log_step("ingest_rosters",        ingest_rosters(cfg = cfg, force_full = force_full))
  log_step("ingest_rosters_weekly", ingest_rosters_weekly(cfg = cfg, force_full = force_full))
  log_step("ingest_depth_charts",   ingest_depth_charts(cfg = cfg, force_full = force_full))
  log_step("ingest_injuries",       ingest_injuries(cfg = cfg, force_full = force_full))
  log_step("ingest_snap_counts",    ingest_snap_counts(cfg = cfg, force_full = force_full))
  log_step("ingest_nextgen_stats",  ingest_nextgen_stats(cfg = cfg, force_full = force_full))
  log_step("ingest_pfr_advstats",   ingest_pfr_advstats(cfg = cfg, force_full = force_full))
  log_step("ingest_ftn_charting",   ingest_ftn_charting(cfg = cfg, force_full = force_full))
  log_step("ingest_ff_opportunity", ingest_ff_opportunity(cfg = cfg, force_full = force_full))

  # ---- 2. SILVER -> GOLD: Full transforms -------------------------------------
  log_step("run_transforms", run_transforms(cfg = cfg, incremental = FALSE))

  # ---- 3. Validation ----------------------------------------------------------
  log_step("data_tests", run_all_tests(cfg = cfg, stop_on_failure = FALSE))

  elapsed <- round((proc.time() - t_start)[["elapsed"]] / 60, 1)
  logger::log_info("BACKFILL COMPLETE in {elapsed} minutes.")

  prune_logs(cfg)
  invisible(NULL)
}


# =============================================================================
# DAILY REFRESH PIPELINE
# Purpose: Update current season data (bronze + silver + gold incrementally).
# Designed to run nightly or after each game week.
# =============================================================================

#' Run daily incremental refresh
#'
#' Only processes the current season. Significantly faster than backfill.
#'
#' @param cfg Config list
#' @export
run_daily_refresh <- function(cfg = load_config()) {
  setup_logging(cfg, context = "daily_refresh")
  ensure_directories(cfg)

  current_season <- cfg$seasons$current_season

  logger::log_info("============================================================")
  logger::log_info("STARTING DAILY REFRESH — Season {current_season}")
  logger::log_info("============================================================")

  t_start <- proc.time()

  # ---- 1. BRONZE: Refresh current season only ---------------------------------
  seasons_to_load <- as.integer(current_season)

  log_step("ingest_schedules",      ingest_schedules(seasons = seasons_to_load, cfg = cfg, force_full = TRUE))
  log_step("ingest_pbp",            ingest_pbp(seasons = seasons_to_load, cfg = cfg, force_full = TRUE))
  log_step("ingest_player_stats",   ingest_player_stats(seasons = seasons_to_load, cfg = cfg, force_full = TRUE))
  log_step("ingest_players",        ingest_players(cfg = cfg))
  log_step("ingest_rosters_weekly", ingest_rosters_weekly(seasons = seasons_to_load, cfg = cfg, force_full = TRUE))
  log_step("ingest_depth_charts",   ingest_depth_charts(seasons = seasons_to_load, cfg = cfg, force_full = TRUE))
  log_step("ingest_injuries",       ingest_injuries(seasons = seasons_to_load, cfg = cfg, force_full = TRUE))
  log_step("ingest_snap_counts",    ingest_snap_counts(seasons = seasons_to_load, cfg = cfg, force_full = TRUE))
  log_step("ingest_nextgen_stats",  ingest_nextgen_stats(seasons = seasons_to_load, cfg = cfg, force_full = TRUE))
  log_step("ingest_pfr_advstats",   ingest_pfr_advstats(seasons = seasons_to_load, cfg = cfg, force_full = TRUE))
  log_step("ingest_ftn_charting",   ingest_ftn_charting(seasons = seasons_to_load, cfg = cfg, force_full = TRUE))
  log_step("ingest_ff_opportunity", ingest_ff_opportunity(seasons = seasons_to_load, cfg = cfg, force_full = TRUE))

  # ---- 2. SILVER -> GOLD: Incremental transforms (current season only) --------
  log_step("run_transforms_incremental",
    run_transforms(cfg = cfg, incremental = TRUE)
  )

  # ---- 3. Validation ----------------------------------------------------------
  log_step("data_tests", run_all_tests(cfg = cfg, stop_on_failure = TRUE))

  elapsed <- round((proc.time() - t_start)[["elapsed"]] / 60, 1)
  logger::log_info("DAILY REFRESH COMPLETE in {elapsed} minutes.")

  prune_logs(cfg)
  invisible(NULL)
}


# =============================================================================
# COMMAND LINE INTERFACE
# Usage:
#   Rscript pipeline.R backfill
#   Rscript pipeline.R daily
#   Rscript pipeline.R backfill --force-full
# =============================================================================

if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  mode <- args[1]
  force_flag <- "--force-full" %in% args

  cfg <- load_config()

  if (mode == "backfill") {
    run_backfill(cfg = cfg, force_full = force_flag)
  } else if (mode == "daily") {
    run_daily_refresh(cfg = cfg)
  } else {
    cat("Usage: Rscript pipeline.R [backfill|daily] [--force-full]\n")
    quit(status = 1)
  }
}
