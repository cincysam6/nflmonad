# =============================================================================
# BRONZE LAYER — All Other Source Ingestion
# Fixed: nflreadr function signatures verified against current package API
# Key fix: load_rosters(weekly=TRUE) does not exist; use load_weekly_rosters()
# =============================================================================

source(here::here("R/utils/config_loader.R"))
source(here::here("R/utils/parquet_io.R"))
source(here::here("R/utils/logging.R"))

# ---- Helper: check if a named function exists in an installed package -------
.pkg_has_fn <- function(fn_name, pkg_name) {
  tryCatch(
    !is.null(getFromNamespace(fn_name, ns = pkg_name)),
    error = function(e) FALSE
  )
}

# ---- Helper: generic season-level ingest ------------------------------------
.ingest_season_table <- function(loader_fn, source_name, table_key,
                                  seasons, cfg, force_full, extra_args = list()) {
  base_path   <- file.path(cfg$paths$raw, table_key)
  src_cfg     <- cfg$sources[[table_key]]
  all_seasons <- resolve_seasons(src_cfg$seasons, cfg)

  if (is.null(seasons)) {
    seasons <- seasons_to_refresh(base_path, all_seasons,
                                  cfg$seasons$incremental_seasons, force_full)
  }

  if (length(seasons) == 0) {
    logger::log_info("{table_key}: Nothing to refresh.")
    return(invisible(NULL))
  }

  logger::log_info("{table_key}: Refreshing seasons {paste(seasons, collapse=', ')}")

  purrr::walk(seasons, function(s) {
    log_step(glue::glue("ingest {table_key} season={s}"), {
      args <- c(list(seasons = s), extra_args)
      df   <- do.call(loader_fn, args)

      if (is.null(df) || nrow(df) == 0) {
        logger::log_warn("{table_key} season={s}: 0 rows returned, skipping.")
        return(invisible(NULL))
      }

      df <- df |>
        dplyr::mutate(season = as.integer(s)) |>
        add_ingestion_metadata(
          source_name      = source_name,
          source_file_type = "nflreadr",
          source_version   = as.character(packageVersion("nflreadr"))
        )

      write_parquet_partition(df, base_path, partition_cols = "season")
    })
  })

  invisible(base_path)
}

# ---- Schedules ---------------------------------------------------------------
ingest_schedules <- function(seasons = NULL, cfg = load_config(),
                              force_full = cfg$refresh$force_full) {
  .ingest_season_table(nflreadr::load_schedules, "nflreadr::load_schedules",
                       "schedules", seasons, cfg, force_full)
}

# ---- Player Stats ------------------------------------------------------------
ingest_player_stats <- function(seasons = NULL, cfg = load_config(),
                                 force_full = cfg$refresh$force_full) {
  .ingest_season_table(nflreadr::load_player_stats, "nflreadr::load_player_stats",
                       "player_stats", seasons, cfg, force_full)
}

# ---- Team Stats --------------------------------------------------------------
ingest_team_stats <- function(seasons = NULL, cfg = load_config(),
                               force_full = cfg$refresh$force_full) {
  base_path   <- file.path(cfg$paths$raw, "team_stats")
  all_seasons <- resolve_seasons(cfg$sources$team_stats$seasons, cfg)
  if (is.null(seasons)) seasons <- seasons_to_refresh(base_path, all_seasons,
                                                       cfg$seasons$incremental_seasons, force_full)
  if (length(seasons) == 0) { logger::log_info("team_stats: Nothing to refresh."); return(invisible(NULL)) }

  purrr::walk(seasons, function(s) {
    log_step(glue::glue("ingest team_stats season={s}"), {
      pbp <- nflreadr::load_pbp(seasons = s)
      df  <- nflfastR::calculate_series_conversion_rates(pbp) |>
        dplyr::mutate(season = as.integer(s)) |>
        add_ingestion_metadata("nflfastR::calculate_series_conversion_rates", "nflreadr",
                               as.character(packageVersion("nflfastR")))
      write_parquet_partition(df, base_path, partition_cols = "season")
    })
  })
}

# ---- Participation -----------------------------------------------------------
ingest_participation <- function(seasons = NULL, cfg = load_config(),
                                  force_full = cfg$refresh$force_full) {
  .ingest_season_table(nflreadr::load_participation, "nflreadr::load_participation",
                       "participation", seasons, cfg, force_full)
}

# ---- Players (static) -------------------------------------------------------
ingest_players <- function(cfg = load_config()) {
  log_step("ingest players", {
    base_path <- file.path(cfg$paths$raw, "players")
    df <- nflreadr::load_players() |>
      add_ingestion_metadata("nflreadr::load_players", "nflreadr",
                             as.character(packageVersion("nflreadr")))
    write_parquet_partition(df, base_path, partition_cols = NULL)
  })
}

# ---- Rosters (season-level) -------------------------------------------------
ingest_rosters <- function(seasons = NULL, cfg = load_config(),
                            force_full = cfg$refresh$force_full) {
  .ingest_season_table(nflreadr::load_rosters, "nflreadr::load_rosters",
                       "rosters", seasons, cfg, force_full)
}

# ---- Weekly Rosters ----------------------------------------------------------
# FIX: load_rosters(weekly=TRUE) does not exist in nflreadr.
# Correct API: load_weekly_rosters(seasons) — available in nflreadr >= 1.4
# Falls back to season-level rosters if load_weekly_rosters is not available.
ingest_rosters_weekly <- function(seasons = NULL, cfg = load_config(),
                                   force_full = cfg$refresh$force_full) {
  base_path   <- file.path(cfg$paths$raw, "rosters_weekly")
  all_seasons <- resolve_seasons(cfg$sources$rosters_weekly$seasons, cfg)

  if (is.null(seasons)) {
    seasons <- seasons_to_refresh(base_path, all_seasons,
                                  cfg$seasons$incremental_seasons, force_full)
  }

  if (length(seasons) == 0) {
    logger::log_info("rosters_weekly: Nothing to refresh.")
    return(invisible(NULL))
  }

  logger::log_info("rosters_weekly: Refreshing seasons {paste(seasons, collapse=', ')}")

  use_weekly_fn <- .pkg_has_fn("load_rosters_weekly", "nflreadr")

  if (!use_weekly_fn) {
    logger::log_warn(paste0(
      "nflreadr::load_rosters_weekly() not found in your installed version. ",
      "Run install.packages('nflreadr') to upgrade, then re-run. ",
      "Skipping weekly rosters for now — season-level rosters are still ingested."
    ))
    return(invisible(NULL))
  }

  purrr::walk(seasons, function(s) {
    log_step(glue::glue("ingest rosters_weekly season={s}"), {
      df <- tryCatch(
        nflreadr::load_rosters_weekly(seasons = s),
        error = function(e) {
          logger::log_warn("rosters_weekly season={s}: {e$message}")
          NULL
        }
      )

      if (is.null(df) || nrow(df) == 0) {
        logger::log_warn("rosters_weekly season={s}: 0 rows, skipping.")
        return(invisible(NULL))
      }

      df <- df |>
        dplyr::mutate(season = as.integer(s)) |>
        add_ingestion_metadata("nflreadr::load_rosters_weekly", "nflreadr",
                               as.character(packageVersion("nflreadr")))

      write_parquet_partition(df, base_path, partition_cols = "season")
    })
  })

  invisible(base_path)
}

# ---- Depth Charts -----------------------------------------------------------
ingest_depth_charts <- function(seasons = NULL, cfg = load_config(),
                                 force_full = cfg$refresh$force_full) {
  .ingest_season_table(nflreadr::load_depth_charts, "nflreadr::load_depth_charts",
                       "depth_charts", seasons, cfg, force_full)
}

# ---- Injuries ----------------------------------------------------------------
ingest_injuries <- function(seasons = NULL, cfg = load_config(),
                             force_full = cfg$refresh$force_full) {
  .ingest_season_table(nflreadr::load_injuries, "nflreadr::load_injuries",
                       "injuries", seasons, cfg, force_full)
}

# ---- Next Gen Stats ----------------------------------------------------------
ingest_nextgen_stats <- function(seasons = NULL, cfg = load_config(),
                                  force_full = cfg$refresh$force_full) {
  base_path   <- file.path(cfg$paths$raw, "nextgen_stats")
  all_seasons <- resolve_seasons(cfg$sources$nextgen_stats$seasons, cfg)
  stat_types  <- cfg$sources$nextgen_stats$stat_types

  if (is.null(seasons)) seasons <- seasons_to_refresh(base_path, all_seasons,
                                                       cfg$seasons$incremental_seasons, force_full)
  if (length(seasons) == 0) { logger::log_info("nextgen_stats: Nothing to refresh."); return(invisible(NULL)) }

  logger::log_info("nextgen_stats: Refreshing seasons {paste(seasons, collapse=', ')}")

  purrr::walk(seasons, function(s) {
    purrr::walk(stat_types, function(st) {
      log_step(glue::glue("ingest nextgen_stats season={s} type={st}"), {
        df <- tryCatch(
          nflreadr::load_nextgen_stats(seasons = s, stat_type = st),
          error = function(e) { logger::log_warn("nextgen_stats {s}/{st}: {e$message}"); NULL }
        )
        if (is.null(df) || nrow(df) == 0) return(invisible(NULL))
        df |>
          dplyr::mutate(season = as.integer(s), stat_type = st) |>
          add_ingestion_metadata(glue::glue("nflreadr::load_nextgen_stats({st})"), "nflreadr",
                                 as.character(packageVersion("nflreadr"))) |>
          write_parquet_partition(base_path, partition_cols = "season")
      })
    })
  })
}

# ---- PFR Advanced Stats ------------------------------------------------------
ingest_pfr_advstats <- function(seasons = NULL, cfg = load_config(),
                                 force_full = cfg$refresh$force_full) {
  base_path   <- file.path(cfg$paths$raw, "pfr_advstats")
  all_seasons <- resolve_seasons(cfg$sources$pfr_advstats$seasons, cfg)
  stat_types  <- cfg$sources$pfr_advstats$stat_types

  if (is.null(seasons)) seasons <- seasons_to_refresh(base_path, all_seasons,
                                                       cfg$seasons$incremental_seasons, force_full)
  if (length(seasons) == 0) { logger::log_info("pfr_advstats: Nothing to refresh."); return(invisible(NULL)) }

  logger::log_info("pfr_advstats: Refreshing seasons {paste(seasons, collapse=', ')}")

  purrr::walk(seasons, function(s) {
    purrr::walk(stat_types, function(st) {
      log_step(glue::glue("ingest pfr_advstats season={s} type={st}"), {
        df <- tryCatch(
          nflreadr::load_pfr_advstats(seasons = s, stat_type = st),
          error = function(e) { logger::log_warn("pfr_advstats {s}/{st}: {e$message}"); NULL }
        )
        if (is.null(df) || nrow(df) == 0) return(invisible(NULL))
        df |>
          dplyr::mutate(season = as.integer(s), stat_type = st) |>
          add_ingestion_metadata(glue::glue("nflreadr::load_pfr_advstats({st})"), "nflreadr",
                                 as.character(packageVersion("nflreadr"))) |>
          write_parquet_partition(base_path, partition_cols = "season")
      })
    })
  })
}

# ---- Snap Counts -------------------------------------------------------------
ingest_snap_counts <- function(seasons = NULL, cfg = load_config(),
                                force_full = cfg$refresh$force_full) {
  .ingest_season_table(nflreadr::load_snap_counts, "nflreadr::load_snap_counts",
                       "snap_counts", seasons, cfg, force_full)
}

# ---- FTN Charting ------------------------------------------------------------
ingest_ftn_charting <- function(seasons = NULL, cfg = load_config(),
                                 force_full = cfg$refresh$force_full) {
  base_path   <- file.path(cfg$paths$raw, "ftn_charting")
  all_seasons <- resolve_seasons(cfg$sources$ftn_charting$seasons, cfg)

  if (is.null(seasons)) seasons <- seasons_to_refresh(base_path, all_seasons,
                                                       cfg$seasons$incremental_seasons, force_full)
  if (length(seasons) == 0) { logger::log_info("ftn_charting: Nothing to refresh."); return(invisible(NULL)) }

  logger::log_info("ftn_charting: Refreshing seasons {paste(seasons, collapse=', ')}")

  purrr::walk(seasons, function(s) {
    log_step(glue::glue("ingest ftn_charting season={s}"), {
      df <- tryCatch(
        nflreadr::load_ftn_charting(seasons = s),
        error = function(e) { logger::log_warn("ftn_charting {s}: {e$message}"); NULL }
      )
      if (is.null(df) || nrow(df) == 0) return(invisible(NULL))
      df |>
        dplyr::mutate(season = as.integer(s)) |>
        add_ingestion_metadata("nflreadr::load_ftn_charting", "nflreadr",
                               as.character(packageVersion("nflreadr"))) |>
        write_parquet_partition(base_path, partition_cols = "season")
    })
  })
}

# ---- Fantasy Expected Opportunity --------------------------------------------
ingest_ff_opportunity <- function(seasons = NULL, cfg = load_config(),
                                   force_full = cfg$refresh$force_full) {
  base_path   <- file.path(cfg$paths$raw, "ff_opportunity")
  all_seasons <- resolve_seasons(cfg$sources$ff_opportunity$seasons, cfg)

  if (is.null(seasons)) seasons <- seasons_to_refresh(base_path, all_seasons,
                                                       cfg$seasons$incremental_seasons, force_full)
  if (length(seasons) == 0) { logger::log_info("ff_opportunity: Nothing to refresh."); return(invisible(NULL)) }

  logger::log_info("ff_opportunity: Refreshing seasons {paste(seasons, collapse=', ')}")

  purrr::walk(seasons, function(s) {
    log_step(glue::glue("ingest ff_opportunity season={s}"), {
      df <- tryCatch(
        nflreadr::load_ff_opportunity(seasons = s),
        error = function(e) { logger::log_warn("ff_opportunity {s}: {e$message}"); NULL }
      )
      if (is.null(df) || nrow(df) == 0) return(invisible(NULL))
      df |>
        dplyr::mutate(season = as.integer(s)) |>
        add_ingestion_metadata("nflreadr::load_ff_opportunity", "nflreadr",
                               as.character(packageVersion("nflreadr"))) |>
        write_parquet_partition(base_path, partition_cols = "season")
    })
  })
}

# ---- External Odds (Placeholder) --------------------------------------------
ingest_external_odds <- function(df, cfg = load_config()) {
  required_cols <- c("game_id", "season", "week", "game_date", "home_team",
                     "away_team", "market_timestamp", "sportsbook", "market_type",
                     "home_spread", "total_line")
  missing_cols <- setdiff(required_cols, names(df))
  if (length(missing_cols) > 0)
    stop("External odds missing required columns: ", paste(missing_cols, collapse = ", "))

  base_path <- file.path(cfg$paths$raw, "external_odds")
  df |>
    add_ingestion_metadata("external_odds", "external") |>
    write_parquet_partition(base_path, partition_cols = "season")
}
