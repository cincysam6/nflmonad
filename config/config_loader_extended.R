# =============================================================================
# config/config_loader_extended.R
# Loads and merges base config + external config.
# =============================================================================

source(here::here("R/utils/config_loader.R"))

#' Load merged base + external configuration
#'
#' @param base_path Path to config.yml
#' @param ext_path  Path to config_external.yml
#' @return Merged config list
#' @export
load_config_extended <- function(
    base_path = here::here("config/config.yml"),
    ext_path  = here::here("config/config_external.yml")
) {
  cfg     <- load_config(base_path)
  cfg_ext <- yaml::read_yaml(ext_path)

  # Deep merge: ext keys win on conflict
  cfg <- utils::modifyList(cfg, cfg_ext)

  # Validate required external keys
  stopifnot(
    "odds_sources missing"    = !is.null(cfg$odds_sources),
    "weather_sources missing" = !is.null(cfg$weather_sources),
    "stadium config missing"  = !is.null(cfg$stadium)
  )

  cfg
}

#' Get the active odds sources (enabled == true)
#'
#' @param cfg Merged config
#' @return Named list of enabled odds source configs
active_odds_sources <- function(cfg) {
  purrr::keep(cfg$odds_sources, ~ isTRUE(.x$enabled))
}

#' Get the active weather sources (enabled == true)
#'
#' @param cfg Merged config
#' @return Named list of enabled weather source configs
active_weather_sources <- function(cfg) {
  purrr::keep(cfg$weather_sources, ~ isTRUE(.x$enabled))
}

#' Convert mph thresholds from config to m/s for Open-Meteo API
#' (Open-Meteo returns wind_speed_10m in km/h by default)
mph_to_kmh <- function(mph) mph * 1.60934
