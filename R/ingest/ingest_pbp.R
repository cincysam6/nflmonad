# =============================================================================
# BRONZE LAYER — Play-by-Play Ingestion
# Source: nflreadr::load_pbp()
# Grain: game_id + play_id
# Partition: season
# =============================================================================

source(here::here("R/utils/config_loader.R"))
source(here::here("R/utils/parquet_io.R"))
source(here::here("R/utils/logging.R"))

#' Ingest play-by-play data for one or more seasons
#'
#' Reads from nflreadr, attaches ingestion metadata, and writes Parquet.
#' Idempotent: calling again will overwrite the partition.
#'
#' @param seasons Integer vector of seasons to load
#' @param cfg Config list
#' @param force_full Logical; if TRUE overwrite existing seasons
#' @return Invisibly returns list of written paths
#' @export
ingest_pbp <- function(seasons = NULL,
                       cfg     = load_config(),
                       force_full = cfg$refresh$force_full) {
  base_path  <- file.path(cfg$paths$raw, "pbp")
  all_seasons <- resolve_seasons(cfg$sources$pbp$seasons, cfg)

  if (is.null(seasons)) {
    seasons <- seasons_to_refresh(
      base_path,
      all_seasons,
      cfg$seasons$incremental_seasons,
      force_full = force_full
    )
  }

  if (length(seasons) == 0) {
    logger::log_info("PBP: No seasons require refresh. Skipping.")
    return(invisible(NULL))
  }

  logger::log_info("PBP: Loading seasons {paste(seasons, collapse=', ')}")

  # nflreadr caches internally; we load one year at a time for memory safety
  paths <- purrr::map(seasons, function(s) {
    log_step(glue::glue("ingest_pbp season={s}"), {
      df <- nflreadr::load_pbp(seasons = s)

      # Light standardisation only — no business logic in raw layer
      df <- df |>
        dplyr::mutate(season = as.integer(season)) |>
        add_ingestion_metadata(
          source_name      = "nflreadr::load_pbp",
          source_file_type = "nflreadr",
          source_version   = as.character(packageVersion("nflreadr"))
        )

      write_parquet_partition(df, base_path, partition_cols = "season")
    })
  })

  invisible(paths)
}

#' Validate raw PBP partition for a given season
#'
#' Checks row counts and key uniqueness.
#'
#' @param season Integer season
#' @param cfg Config list
#' @return Logical TRUE if valid
#' @export
validate_raw_pbp <- function(season, cfg = load_config()) {
  ds <- read_parquet_partition(
    file.path(cfg$paths$raw, "pbp"),
    filter_expr = season == !!season
  ) |> dplyr::collect()

  n_rows <- nrow(ds)
  stopifnot("PBP: zero rows" = n_rows > 0)

  dups <- ds |>
    dplyr::count(game_id, play_id) |>
    dplyr::filter(n > 1)
  stopifnot("PBP: duplicate game_id+play_id" = nrow(dups) == 0)

  logger::log_info("PBP validate season={season}: {n_rows} rows, no duplicates.")
  invisible(TRUE)
}
