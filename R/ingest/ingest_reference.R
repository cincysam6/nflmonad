# =============================================================================
# R/ingest/ingest_reference.R
# Ingest static reference tables: stadium, timezone, coaching context.
# These are seeded from CSV/config, not external APIs.
# =============================================================================

source(here::here("R/utils/parquet_io.R"))
source(here::here("R/utils/logging.R"))
source(here::here("R/ingest/ingest_weather.R"))
source(here::here("config/config_loader_extended.R"))

#' Ingest and write stadium metadata to raw layer
#'
#' Grain: stadium_id + season_start
#' @export
ingest_stadium_metadata <- function(cfg = load_config_extended()) {
  log_step("ingest_stadium_metadata", {
    df <- load_stadium_metadata(cfg) |>
      add_ingestion_metadata("stadium_metadata", "csv")

    base_path <- file.path(cfg$paths$raw, "stadium_metadata")
    write_parquet_partition(df, base_path, partition_cols = NULL)
    logger::log_info("Stadium metadata written: {nrow(df)} rows")
  })
}

#' Ingest team timezone reference
#'
#' Grain: team + season
#' Derives timezone from stadium metadata for each team × season.
#' @export
ingest_team_timezone <- function(cfg = load_config_extended()) {
  log_step("ingest_team_timezone", {
    stadiums <- load_stadium_metadata(cfg)

    # Create team × season timezone lookup
    # Use season_start / season_end ranges
    all_seasons <- seq(cfg$seasons$historical_start, cfg$seasons$current_season)

    tz_ref <- purrr::map_dfr(all_seasons, function(s) {
      stadiums |>
        dplyr::filter(
          season_start <= s,
          season_end   >= s,
          is_neutral_site_default == 0
        ) |>
        dplyr::transmute(
          team      = team,
          season    = as.integer(s),
          stadium_id,
          home_timezone = timezone,
          latitude,
          longitude,
          elevation_m
        )
    })

    df <- tz_ref |>
      add_ingestion_metadata("team_timezone_reference", "csv")

    base_path <- file.path(cfg$paths$raw, "team_timezone_reference")
    write_parquet_partition(df, base_path, partition_cols = "season")
    logger::log_info("Team timezone reference written: {nrow(df)} rows")
  })
}

#' Ingest coaching context (manual/placeholder)
#'
#' Grain: team + season + coach_role
#' This is manually maintained. Populate the CSV and re-run to update.
#'
#' Expected CSV columns: team, season, coach_name, coach_role,
#'   first_season_with_team, is_rookie_hc, coordinator_change_flag
#'
#' @param file_path Path to coaching CSV (optional; uses default if not provided)
#' @export
ingest_coaching_context <- function(
    file_path = here::here("config/coaching_context.csv"),
    cfg = load_config_extended()
) {
  if (!file.exists(file_path)) {
    logger::log_warn("Coaching CSV not found at {file_path} — creating placeholder.")
    # Write empty placeholder
    placeholder <- tibble::tibble(
      team = character(),
      season = integer(),
      coach_name = character(),
      coach_role = character(),  # HC | OC | DC
      first_season_with_team = integer(),
      is_rookie_hc = integer(),
      coordinator_change_flag = integer()
    )
    readr::write_csv(placeholder, file_path)
    return(invisible(NULL))
  }

  log_step("ingest_coaching_context", {
    df <- readr::read_csv(file_path, show_col_types = FALSE) |>
      dplyr::mutate(season = as.integer(season)) |>
      add_ingestion_metadata("coaching_context", "csv")

    base_path <- file.path(cfg$paths$raw, "coach_reference")
    write_parquet_partition(df, base_path, partition_cols = "season")
  })
}

#' Compute travel distances between all stadium pairs
#'
#' Returns a matrix-style data frame: for each (home_team, away_team, season),
#' the great-circle distance from the away team's home stadium to the game venue.
#'
#' @param cfg Extended config
#' @return Data frame with travel_km and timezone information
#' @export
compute_travel_matrix <- function(cfg = load_config_extended()) {
  stadiums <- load_stadium_metadata(cfg) |>
    dplyr::filter(is_neutral_site_default == 0)

  all_seasons <- seq(cfg$seasons$historical_start, cfg$seasons$current_season)

  purrr::map_dfr(all_seasons, function(s) {
    # Active stadiums this season
    active <- stadiums |>
      dplyr::filter(season_start <= s, season_end >= s)

    # Cross-join: for each game (home_team, away_team)
    # we need: away team's HOME stadium → game venue (home_team's stadium)
    tidyr::crossing(
      home_team = active$team,
      away_team = active$team
    ) |>
      dplyr::filter(home_team != away_team) |>
      dplyr::left_join(
        active |> dplyr::select(home_team = team,
                                game_lat = latitude, game_lon = longitude,
                                game_tz  = timezone),
        by = "home_team"
      ) |>
      dplyr::left_join(
        active |> dplyr::select(away_team = team,
                                away_home_lat = latitude, away_home_lon = longitude,
                                away_home_tz  = timezone),
        by = "away_team"
      ) |>
      dplyr::mutate(
        season        = as.integer(s),
        travel_km     = purrr::pmap_dbl(
          list(away_home_lat, away_home_lon, game_lat, game_lon),
          haversine_km
        ),
        # UTC offset difference (positive = away team traveling east)
        # We use lutz::tz_offset for precise offsets (falls back to hour estimate)
        away_home_utc_offset = .tz_utc_offset(away_home_tz),
        game_utc_offset      = .tz_utc_offset(game_tz),
        timezone_shift_hours = game_utc_offset - away_home_utc_offset,
        east_to_west_flag    = as.integer(timezone_shift_hours < -1),
        west_to_east_flag    = as.integer(timezone_shift_hours > 1),
        international_travel_flag = as.integer(
          abs(game_lat - away_home_lat) > 20 | abs(game_lon - away_home_lon) > 50
        )
      )
  })
}

# Estimate UTC offset for a timezone string at a representative mid-season date
.tz_utc_offset <- function(tz_str) {
  ref_dt <- as.POSIXct("2023-11-01 12:00:00", tz = "UTC")
  tryCatch({
    local_dt <- lubridate::with_tz(ref_dt, tzone = tz_str)
    as.numeric(format(local_dt, "%z")) / 100
  }, error = function(e) NA_real_)
}
