# =============================================================================
# Configuration Loader
# =============================================================================

#' Load platform configuration from YAML
#'
#' @param config_path Path to config.yml
#' @return Named list of configuration values
#' @export
load_config <- function(config_path = here::here("config/config.yml")) {
  if (!file.exists(config_path)) {
    stop("Config file not found: ", config_path)
  }
  cfg <- yaml::read_yaml(config_path)

  # Resolve season ranges expressed as "start:end" strings
  cfg$seasons$backfill_range <- seq(
    cfg$seasons$historical_start,
    cfg$seasons$current_season
  )

  cfg
}

#' Expand a season spec like "2016:current_season" into an integer vector
#'
#' @param spec Either an integer (single start), a list of two ints, or a
#'   string like "historical_start:current_season"
#' @param cfg Config list (needed to resolve symbolic names)
#' @return Integer vector of seasons
resolve_seasons <- function(spec, cfg) {
  if (is.numeric(spec)) return(as.integer(seq(spec, cfg$seasons$current_season)))
  if (is.list(spec))   return(as.integer(seq(spec[[1]], spec[[2]])))
  if (is.character(spec)) {
    parts <- strsplit(spec, ":")[[1]]
    start <- switch(parts[1],
      "historical_start" = cfg$seasons$historical_start,
      as.integer(parts[1])
    )
    end <- switch(parts[2],
      "current_season" = cfg$seasons$current_season,
      as.integer(parts[2])
    )
    return(as.integer(seq(start, end)))
  }
  stop("Cannot resolve season spec: ", spec)
}

#' Ensure all required directories exist
#'
#' @param cfg Config list
ensure_directories <- function(cfg) {
  dirs <- c(
    cfg$paths$raw,
    cfg$paths$staging,
    cfg$paths$intermediate,
    cfg$paths$marts,
    cfg$paths$logs,
    dirname(cfg$paths$db),
    file.path(cfg$paths$raw, c(
      "pbp","schedules","player_stats","team_stats","participation",
      "players","rosters","rosters_weekly","depth_charts","injuries",
      "nextgen_stats","pfr_advstats","snap_counts","ftn_charting",
      "ff_opportunity","external_odds"
    ))
  )
  purrr::walk(dirs, fs::dir_create)
  invisible(NULL)
}
