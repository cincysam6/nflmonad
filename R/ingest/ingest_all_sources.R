# =============================================================================
# BRONZE LAYER — All Other Source Ingestion
# =============================================================================
# Each function follows the same pattern:
#   1. Determine which seasons to refresh
#   2. Load from nflreadr
#   3. Add ingestion metadata
#   4. Write partitioned Parquet
# =============================================================================

source(here::here("R/utils/config_loader.R"))
source(here::here("R/utils/parquet_io.R"))
source(here::here("R/utils/logging.R"))

# ---- Helper: generic season-level ingest ------------------------------------

.ingest_season_table <- function(loader_fn,
                                  source_name,
                                  table_key,
                                  seasons,
                                  cfg,
                                  force_full,
                                  extra_args = list()) {
  base_path   <- file.path(cfg$paths$raw, table_key)
  src_cfg     <- cfg$sources[[table_key]]
  all_seasons <- resolve_seasons(src_cfg$seasons, cfg)

  if (is.null(seasons)) {
    seasons <- seasons_to_refresh(
      base_path,
      all_seasons,
      cfg$seasons$incremental_seasons,
      force_full = force_full
    )
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

      if (nrow(df) == 0) {
        logger::log_warn("{table_key} season={s}: 0 rows returned, skipping write.")
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

#' Ingest NFL game schedules
#' Grain: game_id | Partition: season
#' @export
ingest_schedules <- function(seasons = NULL, cfg = load_config(),
                              force_full = cfg$refresh$force_full) {
  .ingest_season_table(
    loader_fn   = nflreadr::load_schedules,
    source_name = "nflreadr::load_schedules",
    table_key   = "schedules",
    seasons     = seasons,
    cfg         = cfg,
    force_full  = force_full
  )
}

# ---- Player Stats (weekly box score) ----------------------------------------

#' Ingest weekly player stats
#' Grain: season + week + player_id + stat_type | Partition: season
#' @export
ingest_player_stats <- function(seasons = NULL, cfg = load_config(),
                                 force_full = cfg$refresh$force_full) {
  .ingest_season_table(
    loader_fn   = nflreadr::load_player_stats,
    source_name = "nflreadr::load_player_stats",
    table_key   = "player_stats",
    seasons     = seasons,
    cfg         = cfg,
    force_full  = force_full
  )
}

# ---- Team Stats --------------------------------------------------------------

#' Ingest weekly team-level stats (from nflfastR)
#' Grain: season + week + team | Partition: season
#' @export
ingest_team_stats <- function(seasons = NULL, cfg = load_config(),
                               force_full = cfg$refresh$force_full) {
  base_path   <- file.path(cfg$paths$raw, "team_stats")
  all_seasons <- resolve_seasons(cfg$sources$team_stats$seasons, cfg)
  if (is.null(seasons)) {
    seasons <- seasons_to_refresh(base_path, all_seasons,
                                  cfg$seasons$incremental_seasons, force_full)
  }
  if (length(seasons) == 0) { logger::log_info("team_stats: Nothing to refresh."); return(invisible(NULL)) }

  purrr::walk(seasons, function(s) {
    log_step(glue::glue("ingest team_stats season={s}"), {
      # nflfastR computes team stats from PBP — we load PBP first and summarise
      # This uses nflreadr::load_pbp as source for consistency
      pbp <- nflreadr::load_pbp(seasons = s)

      df <- nflfastR::calculate_series_conversion_rates(pbp) |>
        dplyr::mutate(season = as.integer(s)) |>
        add_ingestion_metadata(
          source_name      = "nflfastR::calculate_series_conversion_rates",
          source_file_type = "nflreadr",
          source_version   = as.character(packageVersion("nflfastR"))
        )

      write_parquet_partition(df, base_path, partition_cols = "season")
    })
  })
}

# ---- Participation -----------------------------------------------------------

#' Ingest play participation data (player presence per play)
#' Grain: game_id + play_id + player_id | Partition: season
#' @export
ingest_participation <- function(seasons = NULL, cfg = load_config(),
                                  force_full = cfg$refresh$force_full) {
  .ingest_season_table(
    loader_fn   = nflreadr::load_participation,
    source_name = "nflreadr::load_participation",
    table_key   = "participation",
    seasons     = seasons,
    cfg         = cfg,
    force_full  = force_full
  )
}

# ---- Players (static registry) ----------------------------------------------

#' Ingest player registry (no season partition; full refresh only)
#' Grain: gsis_id | Partition: none
#' @export
ingest_players <- function(cfg = load_config()) {
  log_step("ingest players", {
    base_path <- file.path(cfg$paths$raw, "players")
    df <- nflreadr::load_players() |>
      add_ingestion_metadata(
        source_name      = "nflreadr::load_players",
        source_file_type = "nflreadr",
        source_version   = as.character(packageVersion("nflreadr"))
      )
    write_parquet_partition(df, base_path, partition_cols = NULL)
  })
}

# ---- Rosters (season-level) -------------------------------------------------

#' Ingest season rosters
#' Grain: season + team + gsis_id | Partition: season
#' @export
ingest_rosters <- function(seasons = NULL, cfg = load_config(),
                            force_full = cfg$refresh$force_full) {
  .ingest_season_table(
    loader_fn   = nflreadr::load_rosters,
    source_name = "nflreadr::load_rosters",
    table_key   = "rosters",
    seasons     = seasons,
    cfg         = cfg,
    force_full  = force_full
  )
}

# ---- Weekly Rosters ----------------------------------------------------------

#' Ingest weekly rosters (53-man roster by week)
#' Grain: season + week + team + gsis_id | Partition: season
#' @export
ingest_rosters_weekly <- function(seasons = NULL, cfg = load_config(),
                                   force_full = cfg$refresh$force_full) {
  .ingest_season_table(
    loader_fn   = nflreadr::load_rosters,
    source_name = "nflreadr::load_rosters (weekly)",
    table_key   = "rosters_weekly",
    seasons     = seasons,
    cfg         = cfg,
    force_full  = force_full,
    extra_args  = list(weekly = TRUE)
  )
}

# ---- Depth Charts -----------------------------------------------------------

#' Ingest depth charts
#' Grain: season + week + team + gsis_id + position + depth_team + formation | Partition: season
#' @export
ingest_depth_charts <- function(seasons = NULL, cfg = load_config(),
                                 force_full = cfg$refresh$force_full) {
  .ingest_season_table(
    loader_fn   = nflreadr::load_depth_charts,
    source_name = "nflreadr::load_depth_charts",
    table_key   = "depth_charts",
    seasons     = seasons,
    cfg         = cfg,
    force_full  = force_full
  )
}

# ---- Injuries ----------------------------------------------------------------

#' Ingest injury reports
#' Grain: season + week + team + gsis_id + report_primary_injury | Partition: season
#' @export
ingest_injuries <- function(seasons = NULL, cfg = load_config(),
                             force_full = cfg$refresh$force_full) {
  .ingest_season_table(
    loader_fn   = nflreadr::load_injuries,
    source_name = "nflreadr::load_injuries",
    table_key   = "injuries",
    seasons     = seasons,
    cfg         = cfg,
    force_full  = force_full
  )
}

# ---- Next Gen Stats ----------------------------------------------------------

#' Ingest Next Gen Stats (passing, rushing, receiving)
#' Grain: season + week + player_gsis_id + stat_type | Partition: season
#' @export
ingest_nextgen_stats <- function(seasons = NULL, cfg = load_config(),
                                  force_full = cfg$refresh$force_full) {
  base_path   <- file.path(cfg$paths$raw, "nextgen_stats")
  all_seasons <- resolve_seasons(cfg$sources$nextgen_stats$seasons, cfg)
  stat_types  <- cfg$sources$nextgen_stats$stat_types

  if (is.null(seasons)) {
    seasons <- seasons_to_refresh(base_path, all_seasons,
                                  cfg$seasons$incremental_seasons, force_full)
  }

  purrr::walk(seasons, function(s) {
    purrr::walk(stat_types, function(st) {
      log_step(glue::glue("ingest nextgen_stats season={s} type={st}"), {
        df <- nflreadr::load_nextgen_stats(seasons = s, stat_type = st) |>
          dplyr::mutate(season = as.integer(s), stat_type = st) |>
          add_ingestion_metadata(
            source_name      = glue::glue("nflreadr::load_nextgen_stats({st})"),
            source_file_type = "nflreadr",
            source_version   = as.character(packageVersion("nflreadr"))
          )

        if (nrow(df) == 0) {
          logger::log_warn("nextgen_stats season={s} type={st}: 0 rows, skipping.")
          return(invisible(NULL))
        }

        write_parquet_partition(df, base_path, partition_cols = "season")
      })
    })
  })
}

# ---- PFR Advanced Stats ------------------------------------------------------

#' Ingest Pro Football Reference advanced stats
#' Grain: season + week + pfr_id + stat_type | Partition: season
#' @export
ingest_pfr_advstats <- function(seasons = NULL, cfg = load_config(),
                                 force_full = cfg$refresh$force_full) {
  base_path   <- file.path(cfg$paths$raw, "pfr_advstats")
  all_seasons <- resolve_seasons(cfg$sources$pfr_advstats$seasons, cfg)
  stat_types  <- cfg$sources$pfr_advstats$stat_types

  if (is.null(seasons)) {
    seasons <- seasons_to_refresh(base_path, all_seasons,
                                  cfg$seasons$incremental_seasons, force_full)
  }

  purrr::walk(seasons, function(s) {
    purrr::walk(stat_types, function(st) {
      log_step(glue::glue("ingest pfr_advstats season={s} type={st}"), {
        df <- tryCatch(
          nflreadr::load_pfr_advstats(seasons = s, stat_type = st),
          error = function(e) {
            logger::log_warn("PFR advstats season={s} type={st} unavailable: {e$message}")
            NULL
          }
        )

        if (is.null(df) || nrow(df) == 0) return(invisible(NULL))

        df <- df |>
          dplyr::mutate(season = as.integer(s), stat_type = st) |>
          add_ingestion_metadata(
            source_name      = glue::glue("nflreadr::load_pfr_advstats({st})"),
            source_file_type = "nflreadr",
            source_version   = as.character(packageVersion("nflreadr"))
          )

        write_parquet_partition(df, base_path, partition_cols = "season")
      })
    })
  })
}

# ---- Snap Counts -------------------------------------------------------------

#' Ingest snap count data from PFR via nflreadr
#' Grain: season + week + pfr_player_id | Partition: season
#' @export
ingest_snap_counts <- function(seasons = NULL, cfg = load_config(),
                                force_full = cfg$refresh$force_full) {
  .ingest_season_table(
    loader_fn   = nflreadr::load_snap_counts,
    source_name = "nflreadr::load_snap_counts",
    table_key   = "snap_counts",
    seasons     = seasons,
    cfg         = cfg,
    force_full  = force_full
  )
}

# ---- FTN Charting ------------------------------------------------------------

#' Ingest FTN data charting (pre-snap motion, pass rush, coverage)
#' Grain: nflverse_game_id + nflverse_play_id | Partition: season
#' @export
ingest_ftn_charting <- function(seasons = NULL, cfg = load_config(),
                                 force_full = cfg$refresh$force_full) {
  .ingest_season_table(
    loader_fn   = nflreadr::load_ftn_charting,
    source_name = "nflreadr::load_ftn_charting",
    table_key   = "ftn_charting",
    seasons     = seasons,
    cfg         = cfg,
    force_full  = force_full
  )
}

# ---- Fantasy Expected Opportunity --------------------------------------------

#' Ingest expected fantasy opportunity data
#' Grain: season + week + player_id | Partition: season
#' @export
ingest_ff_opportunity <- function(seasons = NULL, cfg = load_config(),
                                   force_full = cfg$refresh$force_full) {
  base_path   <- file.path(cfg$paths$raw, "ff_opportunity")
  all_seasons <- resolve_seasons(cfg$sources$ff_opportunity$seasons, cfg)

  if (is.null(seasons)) {
    seasons <- seasons_to_refresh(base_path, all_seasons,
                                  cfg$seasons$incremental_seasons, force_full)
  }

  purrr::walk(seasons, function(s) {
    log_step(glue::glue("ingest ff_opportunity season={s}"), {
      df <- tryCatch(
        nflreadr::load_ff_opportunity(seasons = s),
        error = function(e) {
          logger::log_warn("ff_opportunity season={s} unavailable: {e$message}")
          NULL
        }
      )

      if (is.null(df) || nrow(df) == 0) return(invisible(NULL))

      df <- df |>
        dplyr::mutate(season = as.integer(s)) |>
        add_ingestion_metadata(
          source_name      = "nflreadr::load_ff_opportunity",
          source_file_type = "nflreadr",
          source_version   = as.character(packageVersion("nflreadr"))
        )

      write_parquet_partition(df, base_path, partition_cols = "season")
    })
  })
}

# ---- External Odds (Placeholder) --------------------------------------------

#' Placeholder for external sportsbook odds ingestion
#'
#' This function defines the expected schema and write path.
#' Actual implementation requires a separate odds data provider.
#'
#' Expected columns:
#'   game_id, season, week, game_date, home_team, away_team,
#'   market_timestamp, sportsbook, market_type,
#'   home_spread, away_spread, spread_juice_home, spread_juice_away,
#'   total_line, over_juice, under_juice,
#'   home_ml, away_ml,
#'   opening_spread, opening_total, closing_spread, closing_total
#'
#' @param df Data frame matching the schema above
#' @param cfg Config list
#' @export
ingest_external_odds <- function(df, cfg = load_config()) {
  required_cols <- c(
    "game_id", "season", "week", "game_date",
    "home_team", "away_team", "market_timestamp", "sportsbook",
    "market_type", "home_spread", "total_line"
  )

  missing_cols <- setdiff(required_cols, names(df))
  if (length(missing_cols) > 0) {
    stop("External odds data missing required columns: ",
         paste(missing_cols, collapse = ", "))
  }

  base_path <- file.path(cfg$paths$raw, "external_odds")

  df <- df |>
    add_ingestion_metadata(
      source_name      = "external_odds",
      source_file_type = "external"
    )

  write_parquet_partition(df, base_path, partition_cols = "season")
}
