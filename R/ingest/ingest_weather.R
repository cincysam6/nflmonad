# =============================================================================
# R/ingest/ingest_weather.R
# Weather ingestion adapters for Open-Meteo (free, no API key).
# Supports both historical ERA5 reanalysis and 7-day hourly forecast.
#
# LEAKAGE CONTRACT:
#   raw_weather_hourly_history  → ONLY safe for postgame retrospective analysis
#                                  or for "realized" backtest variants.
#   raw_weather_forecast_hourly → Safe for pregame prediction when
#                                  forecast_run_ts < prediction_cutoff_ts.
# =============================================================================

source(here::here("R/utils/parquet_io.R"))
source(here::here("R/utils/logging.R"))
source(here::here("R/ingest/source_contracts.R"))
source(here::here("config/config_loader_extended.R"))

# ---- Haversine travel distance (also used in travel features) ---------------

#' Compute great-circle distance between two lat/lon points in km
#'
#' @param lat1,lon1 Origin coordinates (degrees)
#' @param lat2,lon2 Destination coordinates (degrees)
#' @return Distance in kilometres
#' @export
haversine_km <- function(lat1, lon1, lat2, lon2) {
  R    <- 6371  # Earth radius km
  dlat <- (lat2 - lat1) * pi / 180
  dlon <- (lon2 - lon1) * pi / 180
  a    <- sin(dlat/2)^2 +
          cos(lat1 * pi/180) * cos(lat2 * pi/180) * sin(dlon/2)^2
  c    <- 2 * atan2(sqrt(a), sqrt(1 - a))
  R * c
}

# ---- Stadium geocode resolver -----------------------------------------------

#' Load stadium metadata and return as a data frame
#' @param cfg Extended config
#' @export
load_stadium_metadata <- function(cfg = load_config_extended()) {
  path <- here::here(cfg$stadium$source)
  stopifnot(file.exists(path))

  df <- readr::read_csv(path, show_col_types = FALSE) |>
    dplyr::mutate(
      latitude    = as.numeric(latitude),
      longitude   = as.numeric(longitude),
      elevation_m = as.numeric(elevation_m),
      roof_type   = tolower(roof_type),
      surface_type = tolower(surface_type),
      is_neutral_site_default  = as.integer(is_neutral_site_default),
      international_game_flag  = as.integer(international_game_flag),
      altitude_flag            = as.integer(altitude_flag)
    )

  validate_stadium_contract(df)
  df
}

#' Match a game_id to its stadium lat/lon
#'
#' Uses the stg_games table (home_team + season) to look up the
#' home team's stadium for that season.
#'
#' @param games_df Data frame with columns: game_id, season, home_team, location
#' @param stadium_df Stadium metadata data frame
#' @return games_df with latitude, longitude, stadium_id, roof_type, etc. joined
#' @export
join_stadium_to_games <- function(games_df, stadium_df) {
  # Filter active stadium rows for the relevant season
  # (stadium_metadata has season_start and season_end columns)
  games_df |>
    dplyr::left_join(
      stadium_df |>
        dplyr::select(team, season_start, season_end,
                      stadium_id, latitude, longitude, elevation_m,
                      roof_type, surface_type, timezone,
                      is_neutral_site_default, international_game_flag,
                      altitude_flag),
      by = dplyr::join_by(
        home_team == team,
        season >= season_start,
        season <= season_end
      )
    )
}

# ---- Open-Meteo Historical Weather ------------------------------------------

#' Ingest historical hourly weather for a set of games
#'
#' Calls the Open-Meteo Archive API for each unique stadium location × date
#' combination present in the games data frame.
#'
#' LEAKAGE NOTE: Historical (realized) weather is only safe for postgame
#' retrospective models. Use forecast weather for pregame prediction.
#'
#' @param games_df Data frame with: game_id, game_date, latitude, longitude,
#'   stadium_id, roof_type (from join_stadium_to_games output)
#' @param cfg Extended config
#' @param force_full Logical; re-fetch even if parquet exists
#' @export
ingest_weather_historical <- function(games_df,
                                       cfg        = load_config_extended(),
                                       force_full = FALSE) {
  src_cfg   <- cfg$weather_sources$open_meteo
  if (!isTRUE(src_cfg$enabled)) {
    logger::log_info("open_meteo weather disabled — skipping.")
    return(invisible(NULL))
  }

  base_path <- file.path(cfg$paths$raw, "weather_hourly_history")
  variables <- paste(src_cfg$hourly_variables, collapse = ",")

  # Identify unique location × date pairs to fetch
  # (Multiple games at same stadium same date = one API call)
  locations <- games_df |>
    dplyr::distinct(stadium_id, latitude, longitude, game_date = as.Date(game_date)) |>
    dplyr::mutate(season = as.integer(format(game_date, "%Y")))

  purrr::walk(seq_len(nrow(locations)), function(i) {
    loc  <- locations[i, ]
    date_str <- format(loc$game_date, "%Y-%m-%d")
    season   <- loc$season

    out_file <- file.path(
      base_path,
      glue::glue("season={season}"),
      glue::glue("stadium={loc$stadium_id}"),
      glue::glue("{date_str}.parquet")
    )

    if (!force_full && file.exists(out_file)) {
      logger::log_info("Weather history exists, skipping: {out_file}")
      return(invisible(NULL))
    }

    log_step(glue::glue("weather_history {loc$stadium_id} {date_str}"), {
      url <- glue::glue(
        "{src_cfg$historical_base_url}",
        "?latitude={loc$latitude}",
        "&longitude={loc$longitude}",
        "&start_date={date_str}",
        "&end_date={date_str}",
        "&hourly={variables}",
        "&timezone={utils::URLencode('America/Chicago', reserved=TRUE)}",
        "&wind_speed_unit=mph",
        "&temperature_unit=fahrenheit",
        "&precipitation_unit=inch"
      )

      resp <- tryCatch(
        httr2::request(url) |>
          httr2::req_timeout(30) |>
          httr2::req_retry(max_tries = 3, backoff = ~ 5) |>
          httr2::req_perform(),
        error = function(e) {
          logger::log_error("Open-Meteo historical error: {e$message}")
          NULL
        }
      )

      if (is.null(resp)) return(invisible(NULL))

      parsed <- httr2::resp_body_json(resp, simplifyVector = TRUE)
      hourly <- parsed$hourly

      df <- tibble::tibble(
        source_name             = "open_meteo",
        stadium_id              = loc$stadium_id,
        latitude                = loc$latitude,
        longitude               = loc$longitude,
        weather_type            = "historical",
        observation_or_forecast_ts = lubridate::as_datetime(hourly$time),
        forecast_run_ts         = NA_character_,
        game_date               = loc$game_date,
        season                  = season,
        temperature_2m          = as.numeric(hourly$temperature_2m),
        apparent_temperature    = as.numeric(hourly$apparent_temperature),
        precipitation           = as.numeric(hourly$precipitation),
        rain                    = as.numeric(hourly$rain),
        snowfall                = as.numeric(hourly$snowfall),
        snow_depth              = as.numeric(hourly$snow_depth),
        wind_speed_10m          = as.numeric(hourly$wind_speed_10m),
        wind_gusts_10m          = as.numeric(hourly$wind_gusts_10m),
        wind_direction_10m      = as.numeric(hourly$wind_direction_10m),
        relative_humidity_2m    = as.numeric(hourly$relative_humidity_2m),
        dew_point_2m            = as.numeric(hourly$dew_point_2m),
        surface_pressure        = as.numeric(hourly$surface_pressure),
        weather_code            = as.integer(hourly$weather_code),
        ingestion_ts            = lubridate::now("UTC")
      )

      validate_weather_contract(df, "open_meteo_historical")

      fs::dir_create(dirname(out_file))
      arrow::write_parquet(df, out_file, compression = "snappy")
      logger::log_info("Wrote weather history: {out_file} ({nrow(df)} hours)")
    })
  })
}

# ---- Open-Meteo Forecast Weather --------------------------------------------

#' Ingest forecast weather for upcoming games
#'
#' Records the forecast_run_ts so downstream leakage checks can validate
#' that only forecasts available before the prediction cutoff are used.
#'
#' @param games_df Data frame with upcoming games (joined to stadium coords)
#' @param cfg Extended config
#' @export
ingest_weather_forecast <- function(games_df, cfg = load_config_extended()) {
  src_cfg  <- cfg$weather_sources$open_meteo
  if (!isTRUE(src_cfg$enabled)) return(invisible(NULL))

  base_path     <- file.path(cfg$paths$raw, "weather_forecast_hourly")
  variables     <- paste(src_cfg$hourly_variables, collapse = ",")
  forecast_run  <- lubridate::now("UTC")
  current_season <- cfg$seasons$current_season

  # Only process games in the next 7 days
  upcoming <- games_df |>
    dplyr::filter(
      !is.na(game_date),
      as.Date(game_date) >= Sys.Date(),
      as.Date(game_date) <= Sys.Date() + 7
    ) |>
    dplyr::distinct(stadium_id, latitude, longitude, game_date = as.Date(game_date))

  if (nrow(upcoming) == 0) {
    logger::log_info("No upcoming games in 7-day window for forecast.")
    return(invisible(NULL))
  }

  purrr::walk(seq_len(nrow(upcoming)), function(i) {
    loc      <- upcoming[i, ]
    date_str <- format(loc$game_date, "%Y-%m-%d")

    log_step(glue::glue("weather_forecast {loc$stadium_id} {date_str}"), {
      url <- glue::glue(
        "{src_cfg$forecast_base_url}",
        "?latitude={loc$latitude}",
        "&longitude={loc$longitude}",
        "&hourly={variables}",
        "&timezone={utils::URLencode('America/Chicago', reserved=TRUE)}",
        "&wind_speed_unit=mph",
        "&temperature_unit=fahrenheit",
        "&forecast_days={src_cfg$forecast_days}"
      )

      resp <- tryCatch(
        httr2::request(url) |>
          httr2::req_timeout(30) |>
          httr2::req_perform(),
        error = function(e) { logger::log_error("Forecast error: {e$message}"); NULL }
      )

      if (is.null(resp)) return(invisible(NULL))

      parsed <- httr2::resp_body_json(resp, simplifyVector = TRUE)
      hourly <- parsed$hourly

      df <- tibble::tibble(
        source_name             = "open_meteo",
        stadium_id              = loc$stadium_id,
        latitude                = loc$latitude,
        longitude               = loc$longitude,
        weather_type            = "forecast",
        observation_or_forecast_ts = lubridate::as_datetime(hourly$time),
        forecast_run_ts         = as.character(forecast_run),
        game_date               = loc$game_date,
        season                  = current_season,
        temperature_2m          = as.numeric(hourly$temperature_2m),
        apparent_temperature    = as.numeric(hourly$apparent_temperature),
        precipitation           = as.numeric(hourly$precipitation),
        rain                    = as.numeric(hourly$rain),
        snowfall                = as.numeric(hourly$snowfall),
        snow_depth              = as.numeric(hourly$snow_depth),
        wind_speed_10m          = as.numeric(hourly$wind_speed_10m),
        wind_gusts_10m          = as.numeric(hourly$wind_gusts_10m),
        wind_direction_10m      = as.numeric(hourly$wind_direction_10m),
        relative_humidity_2m    = as.numeric(hourly$relative_humidity_2m),
        dew_point_2m            = as.numeric(hourly$dew_point_2m),
        surface_pressure        = as.numeric(hourly$surface_pressure),
        weather_code            = as.integer(hourly$weather_code),
        ingestion_ts            = lubridate::now("UTC")
      )

      validate_weather_contract(df, "open_meteo_forecast")

      out_path <- file.path(base_path,
                            glue::glue("season={current_season}"),
                            glue::glue("game_date={date_str}"),
                            glue::glue("forecast_run={format(forecast_run,'%Y%m%d_%H%M%S')}.parquet"))
      fs::dir_create(dirname(out_path))
      arrow::write_parquet(df, out_path, compression = "snappy")
      logger::log_info("Wrote forecast: {out_path} ({nrow(df)} hours)")
    })
  })
}

#' Select the hourly weather record nearest to kickoff time
#'
#' For historical weather: returns the record with the smallest absolute
#' time difference from the game kickoff datetime.
#'
#' @param weather_df Hourly weather data frame (must have observation_or_forecast_ts)
#' @param kickoff_ts POSIXct kickoff timestamp
#' @return Single-row data frame
#' @export
nearest_weather_to_kickoff <- function(weather_df, kickoff_ts) {
  weather_df |>
    dplyr::mutate(
      time_diff_min = abs(as.numeric(
        difftime(observation_or_forecast_ts, kickoff_ts, units = "mins")
      ))
    ) |>
    dplyr::arrange(time_diff_min) |>
    dplyr::slice(1) |>
    dplyr::select(-time_diff_min)
}

#' Select the latest valid forecast for a game given a prediction timestamp
#'
#' Leakage-safe: only uses forecast runs that completed BEFORE the
#' prediction cutoff.
#'
#' @param forecast_df All forecast rows for a game (must have forecast_run_ts)
#' @param kickoff_ts POSIXct kickoff timestamp
#' @param prediction_cutoff_ts POSIXct — latest allowable forecast run time
#' @return Single-row data frame (kickoff-hour forecast from the latest safe run)
#' @export
latest_safe_forecast <- function(forecast_df, kickoff_ts, prediction_cutoff_ts) {
  forecast_df |>
    dplyr::filter(
      lubridate::as_datetime(forecast_run_ts) < prediction_cutoff_ts
    ) |>
    dplyr::mutate(
      time_diff_min = abs(as.numeric(
        difftime(observation_or_forecast_ts, kickoff_ts, units = "mins")
      ))
    ) |>
    dplyr::arrange(dplyr::desc(lubridate::as_datetime(forecast_run_ts)),
                   time_diff_min) |>
    dplyr::slice(1) |>
    dplyr::select(-time_diff_min)
}
