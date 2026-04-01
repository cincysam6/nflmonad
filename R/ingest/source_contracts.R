# =============================================================================
# R/ingest/source_contracts.R
# Source contract definitions, quality registry, and validation helpers.
# A "contract" defines the expected schema for a source adapter output.
# Adapters must conform to their contract before data is written to raw layer.
# =============================================================================

# ---- Odds Source Contract ---------------------------------------------------

#' Validate a data frame against the odds source contract
#'
#' @param df Data frame from an odds adapter
#' @param source_name String identifier
#' @return df invisibly if valid; stops with informative error if not
#' @export
validate_odds_contract <- function(df, source_name) {
  required_cols <- c(
    "source_name", "sportsbook", "event_id",
    "market_timestamp", "game_datetime",
    "home_team", "away_team",
    "market_type",    # spread | total | moneyline | team_total | player_prop
    "selection",      # home | away | over | under | player_name
    "line",           # numeric spread or total value
    "price_american", # e.g. -110
    "snapshot_type",  # opening | intraday | closing
    "ingestion_ts"
  )

  missing <- setdiff(required_cols, names(df))
  if (length(missing) > 0) {
    stop(glue::glue(
      "Odds contract violation [{source_name}]: missing columns: ",
      paste(missing, collapse = ", ")
    ))
  }

  # Type checks
  stopifnot(
    "market_timestamp must be POSIXct" =
      inherits(df$market_timestamp, c("POSIXct","POSIXlt")),
    "line must be numeric" =
      is.numeric(df$line),
    "price_american must be numeric" =
      is.numeric(df$price_american)
  )

  valid_market_types <- c("spread","total","moneyline","team_total","player_prop")
  bad_types <- setdiff(unique(df$market_type), valid_market_types)
  if (length(bad_types) > 0) {
    warning(glue::glue(
      "Odds contract [{source_name}]: unknown market_type values: ",
      paste(bad_types, collapse = ", ")
    ))
  }

  valid_snapshots <- c("opening","intraday","closing")
  bad_snaps <- setdiff(unique(df$snapshot_type), valid_snapshots)
  if (length(bad_snaps) > 0) {
    warning(glue::glue(
      "Odds contract [{source_name}]: unknown snapshot_type values: ",
      paste(bad_snaps, collapse = ", ")
    ))
  }

  logger::log_info("Odds contract validated: {source_name} ({nrow(df)} rows)")
  invisible(df)
}

#' Validate a data frame against the weather source contract
#' @export
validate_weather_contract <- function(df, source_name) {
  required_cols <- c(
    "source_name", "stadium_id", "latitude", "longitude",
    "observation_or_forecast_ts", "weather_type",   # "historical" | "forecast"
    "temperature_2m", "wind_speed_10m", "ingestion_ts"
  )

  missing <- setdiff(required_cols, names(df))
  if (length(missing) > 0) {
    stop(glue::glue(
      "Weather contract violation [{source_name}]: missing columns: ",
      paste(missing, collapse = ", ")
    ))
  }

  stopifnot(
    "temperature_2m must be numeric"  = is.numeric(df$temperature_2m),
    "wind_speed_10m must be numeric"  = is.numeric(df$wind_speed_10m),
    "latitude must be numeric"        = is.numeric(df$latitude),
    "longitude must be numeric"       = is.numeric(df$longitude)
  )

  logger::log_info("Weather contract validated: {source_name} ({nrow(df)} rows)")
  invisible(df)
}

#' Validate stadium metadata against the stadium source contract
#' @export
validate_stadium_contract <- function(df) {
  required_cols <- c(
    "stadium_id", "stadium_name", "team",
    "latitude", "longitude", "elevation_m",
    "roof_type", "surface_type", "timezone"
  )

  missing <- setdiff(required_cols, names(df))
  if (length(missing) > 0) {
    stop("Stadium contract violation: missing columns: ",
         paste(missing, collapse = ", "))
  }

  valid_roofs <- c("open","dome","retractable")
  bad_roofs <- setdiff(tolower(unique(df$roof_type)), valid_roofs)
  if (length(bad_roofs) > 0) {
    warning("Stadium: unknown roof_type values: ", paste(bad_roofs, collapse=", "))
  }

  invisible(df)
}

# ---- Source Quality Registry ------------------------------------------------

#' Build source quality registry (static metadata about each source)
#'
#' This registry is used by diagnostic pipelines to report coverage gaps.
#'
#' @param cfg Extended config list
#' @return Data frame of source quality metadata
#' @export
build_source_registry <- function(cfg) {
  odds_rows <- purrr::imap_dfr(cfg$odds_sources, function(src, name) {
    tibble::tibble(
      source_category    = "odds",
      source_name        = name,
      enabled            = isTRUE(src$enabled),
      coverage_start     = src$coverage_start %||% NA_integer_,
      coverage_end       = src$coverage_end   %||% NA_character_,
      markets_available  = paste(src$markets   %||% character(0), collapse=","),
      timestamp_granularity = src$timestamp_granularity %||% NA_character_,
      reliability_score  = src$reliability_score %||% NA_real_,
      notes              = src$notes %||% NA_character_
    )
  })

  weather_rows <- purrr::imap_dfr(cfg$weather_sources, function(src, name) {
    tibble::tibble(
      source_category    = "weather",
      source_name        = name,
      enabled            = isTRUE(src$enabled),
      coverage_start     = src$coverage_start %||% NA_integer_,
      coverage_end       = NA_character_,
      markets_available  = NA_character_,
      timestamp_granularity = "hourly",
      reliability_score  = src$reliability_score %||% NA_real_,
      notes              = src$notes %||% NA_character_
    )
  })

  dplyr::bind_rows(odds_rows, weather_rows)
}

# Helper: null coalescing operator
`%||%` <- function(a, b) if (!is.null(a)) a else b
