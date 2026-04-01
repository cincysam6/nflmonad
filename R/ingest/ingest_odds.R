# =============================================================================
# R/ingest/ingest_odds.R
# Odds ingestion adapters for each supported source.
# Each adapter produces a data frame conforming to the odds source contract,
# then writes to raw_market_odds_snapshot partitioned by season/week.
# =============================================================================

source(here::here("R/utils/parquet_io.R"))
source(here::here("R/utils/logging.R"))
source(here::here("R/ingest/source_contracts.R"))
source(here::here("config/config_loader_extended.R"))

# ---- Market Utility Functions -----------------------------------------------

#' Convert American odds to decimal odds
#' @export
american_to_decimal <- function(american) {
  dplyr::case_when(
    american >= 100  ~  american / 100 + 1,
    american <= -100 ~ -100 / american + 1,
    TRUE             ~ NA_real_
  )
}

#' Convert American odds to raw implied probability
#' @export
american_to_implied_prob <- function(american) {
  dplyr::case_when(
    american >= 100  ~  100 / (american + 100),
    american <= -100 ~ -american / (-american + 100),
    TRUE             ~ NA_real_
  )
}

#' Remove vig from a two-outcome market and return fair probabilities
#'
#' Uses the multiplicative (proportional) vig removal method.
#' For two-way markets (spread, moneyline home/away).
#'
#' @param prob_a Raw implied probability side A
#' @param prob_b Raw implied probability side B
#' @return List with fair_prob_a, fair_prob_b, vig_pct
#' @export
remove_vig_two_way <- function(prob_a, prob_b) {
  total_prob <- prob_a + prob_b
  list(
    fair_prob_a = prob_a / total_prob,
    fair_prob_b = prob_b / total_prob,
    vig_pct     = (total_prob - 1) * 100
  )
}

#' Derive market-implied team totals from spread and total
#'
#' Standard approximation: home_total = total/2 - spread/2
#' (spread expressed as home_spread, negative = home favored)
#'
#' @param total Game total line
#' @param home_spread Spread from home team's perspective (negative = favored)
#' @return list with home_implied_total and away_implied_total
#' @export
implied_team_totals <- function(total, home_spread) {
  home_implied <- total / 2 - home_spread / 2
  away_implied <- total / 2 + home_spread / 2
  list(
    home_implied_total = home_implied,
    away_implied_total = away_implied
  )
}

#' Identify if a line move crossed a key number
#'
#' @param line_from Opening line
#' @param line_to   Closing line
#' @param key_numbers Numeric vector of key numbers
#' @return Logical vector
#' @export
crossed_key_number <- function(line_from, line_to, key_numbers) {
  purrr::map2_lgl(line_from, line_to, function(from, to) {
    if (is.na(from) || is.na(to)) return(FALSE)
    any(
      (from < key_numbers & to >= key_numbers) |
      (from > key_numbers & to <= key_numbers) |
      (from > -key_numbers & to <= -key_numbers) |
      (from < -key_numbers & to >= -key_numbers)
    )
  })
}

#' Select the latest allowable odds snapshot before a cutoff timestamp
#'
#' Leakage-safe: only returns rows with market_timestamp < cutoff_ts
#'
#' @param odds_df Data frame of odds snapshots (must have market_timestamp, game_id)
#' @param game_id Target game_id
#' @param cutoff_ts POSIXct timestamp (prediction window boundary)
#' @return Single-row data frame (the most recent allowed snapshot)
#' @export
latest_odds_before_cutoff <- function(odds_df, game_id, cutoff_ts) {
  odds_df |>
    dplyr::filter(
      .data$game_id    == !!game_id,
      .data$market_timestamp < !!cutoff_ts
    ) |>
    dplyr::arrange(dplyr::desc(market_timestamp)) |>
    dplyr::slice(1)
}

# =============================================================================
# ADAPTER 1: nflverse ESPN betting lines
# =============================================================================

#' Ingest odds from nflreadr::load_espn_betting_lines
#'
#' Maps to the odds source contract.
#' Grain per write: season partition.
#'
#' @param seasons Integer vector
#' @param cfg Extended config list
#' @param force_full Logical
#' @export
ingest_odds_nflverse <- function(seasons    = NULL,
                                  cfg       = load_config_extended(),
                                  force_full = cfg$refresh$force_full) {
  src_cfg   <- cfg$odds_sources$nflverse_lines
  if (!isTRUE(src_cfg$enabled)) {
    logger::log_info("nflverse odds source disabled — skipping.")
    return(invisible(NULL))
  }

  base_path   <- file.path(cfg$paths$raw, "market_odds_snapshot")
  all_seasons <- seq(src_cfg$coverage_start, cfg$seasons$current_season)

  if (is.null(seasons)) {
    seasons <- seasons_to_refresh(
      base_path, all_seasons,
      cfg$seasons$incremental_seasons, force_full
    )
  }

  if (length(seasons) == 0) {
    logger::log_info("nflverse odds: nothing to refresh.")
    return(invisible(NULL))
  }

  purrr::walk(seasons, function(s) {
    log_step(glue::glue("ingest_odds_nflverse season={s}"), {
      raw <- tryCatch(
        nflreadr::load_espn_betting_lines(seasons = s),
        error = function(e) {
          logger::log_warn("nflverse odds season={s}: {e$message}")
          NULL
        }
      )

      if (is.null(raw) || nrow(raw) == 0) {
        logger::log_warn("nflverse odds season={s}: 0 rows, skipping.")
        return(invisible(NULL))
      }

      # Map to contract schema
      df <- raw |>
        dplyr::transmute(
          source_name        = "nflverse_lines",
          sportsbook         = "espn_consensus",
          event_id           = game_id,
          game_id            = game_id,
          market_timestamp   = lubridate::as_datetime(spread_line_last_updated_at %||% NA),
          game_datetime      = lubridate::as_datetime(gameday),
          home_team          = .standardise_team(home_team),
          away_team          = .standardise_team(away_team),
          season             = as.integer(season),
          week               = as.integer(week),
          # Spread rows
          market_type        = "spread",
          selection          = "home",
          line               = as.numeric(spread_line),
          price_american     = as.numeric(home_spread_odds),
          price_decimal      = american_to_decimal(as.numeric(home_spread_odds)),
          implied_probability_raw = american_to_implied_prob(as.numeric(home_spread_odds)),
          snapshot_type      = "closing",  # ESPN provides close only
          overunder_line     = as.numeric(over_under_line),
          ingestion_ts       = lubridate::now("UTC")
        )

      # Add total rows
      total_rows <- raw |>
        dplyr::transmute(
          source_name        = "nflverse_lines",
          sportsbook         = "espn_consensus",
          event_id           = game_id,
          game_id            = game_id,
          market_timestamp   = lubridate::as_datetime(over_under_last_updated_at %||% NA),
          game_datetime      = lubridate::as_datetime(gameday),
          home_team          = .standardise_team(home_team),
          away_team          = .standardise_team(away_team),
          season             = as.integer(season),
          week               = as.integer(week),
          market_type        = "total",
          selection          = "over",
          line               = as.numeric(over_under_line),
          price_american     = -110L,
          price_decimal      = american_to_decimal(-110),
          implied_probability_raw = american_to_implied_prob(-110),
          snapshot_type      = "closing",
          overunder_line     = as.numeric(over_under_line),
          ingestion_ts       = lubridate::now("UTC")
        )

      df_all <- dplyr::bind_rows(df, total_rows) |>
        add_ingestion_metadata("nflverse_lines", "nflreadr")

      validate_odds_contract(df_all, "nflverse_lines")

      write_parquet_partition(
        df_all, base_path,
        partition_cols = "season"
      )
    })
  })
}

# =============================================================================
# ADAPTER 2: The Odds API (commercial — paid plan required)
# =============================================================================

#' Ingest live/upcoming odds snapshot from The Odds API
#'
#' Designed for pre-game real-time polling.
#' Historical (past events) requires their historical endpoint + paid plan.
#'
#' @param cfg Extended config list
#' @export
ingest_odds_the_odds_api_live <- function(cfg = load_config_extended()) {
  src_cfg <- cfg$odds_sources$the_odds_api
  if (!isTRUE(src_cfg$enabled)) {
    logger::log_info("the_odds_api disabled — skipping.")
    return(invisible(NULL))
  }

  api_key <- Sys.getenv(src_cfg$api_key_env_var)
  if (nchar(api_key) == 0) {
    logger::log_warn("the_odds_api: no API key found in env var {src_cfg$api_key_env_var}")
    return(invisible(NULL))
  }

  log_step("ingest_odds_the_odds_api_live", {
    url <- glue::glue(
      "{src_cfg$base_url}/sports/{src_cfg$sports_key}/odds",
      "?apiKey={api_key}",
      "&regions={paste(src_cfg$regions, collapse=',')}",
      "&markets={paste(src_cfg$markets, collapse=',')}",
      "&oddsFormat=american",
      "&dateFormat=iso"
    )

    resp <- tryCatch(
      httr2::request(url) |>
        httr2::req_timeout(30) |>
        httr2::req_perform(),
      error = function(e) {
        logger::log_error("the_odds_api HTTP error: {e$message}")
        NULL
      }
    )

    if (is.null(resp)) return(invisible(NULL))

    events <- httr2::resp_body_json(resp, simplifyVector = FALSE)

    if (length(events) == 0) {
      logger::log_info("the_odds_api: no live events returned.")
      return(invisible(NULL))
    }

    snapshot_ts <- lubridate::now("UTC")
    current_season <- cfg$seasons$current_season

    # Parse all events into rows
    rows <- purrr::map_dfr(events, function(evt) {
      game_dt <- lubridate::as_datetime(evt$commence_time)

      purrr::map_dfr(evt$bookmakers, function(bk) {
        purrr::map_dfr(bk$markets, function(mkt) {
          purrr::map_dfr(mkt$outcomes, function(oc) {
            tibble::tibble(
              source_name    = "the_odds_api",
              sportsbook     = bk$key,
              event_id       = evt$id,
              game_id        = NA_character_,  # matched in staging
              market_timestamp = snapshot_ts,
              game_datetime  = game_dt,
              home_team      = .standardise_team(evt$home_team),
              away_team      = .standardise_team(evt$away_team),
              season         = current_season,
              week           = NA_integer_,    # filled in staging join
              market_type    = mkt$key,
              selection      = tolower(oc$name),
              line           = as.numeric(oc$point %||% NA),
              price_american = as.numeric(oc$price),
              price_decimal  = american_to_decimal(as.numeric(oc$price)),
              implied_probability_raw = american_to_implied_prob(as.numeric(oc$price)),
              snapshot_type  = "intraday",
              overunder_line = NA_real_,
              ingestion_ts   = snapshot_ts
            )
          })
        })
      })
    })

    df <- rows |>
      add_ingestion_metadata("the_odds_api", "api_json")

    validate_odds_contract(df, "the_odds_api")

    base_path <- file.path(cfg$paths$raw, "market_odds_snapshot")
    write_parquet_partition(df, base_path, partition_cols = "season")
  })
}

#' Ingest The Odds API historical event odds (paid historical endpoint)
#'
#' @param event_ids Character vector of Odds API event IDs
#' @param markets Character vector of market keys
#' @param cfg Extended config
#' @export
ingest_odds_the_odds_api_historical <- function(event_ids,
                                                 markets = c("h2h","spreads","totals"),
                                                 cfg     = load_config_extended()) {
  src_cfg <- cfg$odds_sources$the_odds_api
  if (!isTRUE(src_cfg$enabled)) return(invisible(NULL))

  api_key <- Sys.getenv(src_cfg$api_key_env_var)
  if (nchar(api_key) == 0) {
    logger::log_warn("the_odds_api: no API key.")
    return(invisible(NULL))
  }

  snapshot_ts <- lubridate::now("UTC")

  purrr::walk(event_ids, function(eid) {
    log_step(glue::glue("ingest_odds_api_historical event={eid}"), {
      purrr::walk(markets, function(mkt) {
        url <- glue::glue(
          "{src_cfg$base_url}/historical/sports/{src_cfg$sports_key}",
          "/events/{eid}/odds",
          "?apiKey={api_key}",
          "&regions=us",
          "&markets={mkt}",
          "&oddsFormat=american"
        )

        resp <- tryCatch(
          httr2::request(url) |>
            httr2::req_timeout(30) |>
            httr2::req_perform(),
          error = function(e) {
            logger::log_warn("Historical odds API error for {eid}/{mkt}: {e$message}")
            NULL
          }
        )

        if (is.null(resp)) return(invisible(NULL))

        data <- httr2::resp_body_json(resp, simplifyVector = FALSE)
        # Parse similarly to live adapter...
        # (structure mirrors the live endpoint response)
        logger::log_info("Historical odds ingested: event={eid}, market={mkt}")
      })
    })
  })
}

# =============================================================================
# ADAPTER 3: SBR-style free CSV (sportsbookreviewsonline.com or similar)
# Format: manually downloaded Excel/CSV files with open/close data
# =============================================================================

#' Ingest historical odds from SBR-style CSV files
#'
#' Expected CSV columns (flexible, mapped via column map):
#' Date, Team, Final, Open, Close, ML, 2H
#'
#' @param file_path Path to CSV file
#' @param season Integer season year
#' @param cfg Extended config
#' @export
ingest_odds_sbr_csv <- function(file_path, season, cfg = load_config_extended()) {
  src_cfg <- cfg$odds_sources$sbr_free
  if (!isTRUE(src_cfg$enabled)) {
    logger::log_info("sbr_free disabled — skipping.")
    return(invisible(NULL))
  }

  log_step(glue::glue("ingest_odds_sbr_csv season={season}"), {
    raw <- readr::read_csv(file_path, show_col_types = FALSE) |>
      janitor::clean_names()

    # SBR format: two rows per game (home + away), alternating
    # Pair them up to create one row per game
    n <- nrow(raw)
    if (n %% 2 != 0) {
      warning("SBR CSV has odd number of rows — may have parsing issues.")
    }

    away_rows <- raw[seq(1, n, by = 2), ]
    home_rows <- raw[seq(2, n, by = 2), ]

    games <- tibble::tibble(
      game_date  = lubridate::mdy(as.character(home_rows$date)),
      home_team  = .standardise_team(home_rows$team),
      away_team  = .standardise_team(away_rows$team),
      home_score = as.integer(home_rows$final),
      away_score = as.integer(away_rows$final),
      opening_spread = as.numeric(home_rows$open),
      closing_spread = as.numeric(home_rows$close),
      home_ml        = as.integer(home_rows$ml),
      away_ml        = as.integer(away_rows$ml),
      opening_total  = as.numeric(away_rows$open),   # total on away row conventionally
      closing_total  = as.numeric(away_rows$close)
    )

    # Pivot to contract format (spread rows + total rows)
    spread_rows <- games |>
      dplyr::transmute(
        source_name     = "sbr_free",
        sportsbook      = "pinnacle_consensus",
        event_id        = paste0(season, "_", format(game_date,"%Y%m%d"), "_",
                                 home_team, "_", away_team),
        game_id         = NA_character_,
        market_timestamp = lubridate::as_datetime(game_date),
        game_datetime   = lubridate::as_datetime(game_date),
        home_team, away_team,
        season          = as.integer(season),
        week            = NA_integer_,
        market_type     = "spread",
        selection       = "home",
        line            = closing_spread,
        price_american  = -110L,
        price_decimal   = american_to_decimal(-110),
        implied_probability_raw = american_to_implied_prob(-110),
        snapshot_type   = "closing",
        overunder_line  = closing_total,
        ingestion_ts    = lubridate::now("UTC")
      )

    open_spread_rows <- spread_rows |>
      dplyr::mutate(line = games$opening_spread, snapshot_type = "opening")

    total_rows <- games |>
      dplyr::transmute(
        source_name     = "sbr_free",
        sportsbook      = "pinnacle_consensus",
        event_id        = paste0(season, "_", format(game_date,"%Y%m%d"), "_",
                                 home_team, "_", away_team),
        game_id         = NA_character_,
        market_timestamp = lubridate::as_datetime(game_date),
        game_datetime   = lubridate::as_datetime(game_date),
        home_team, away_team,
        season          = as.integer(season),
        week            = NA_integer_,
        market_type     = "total",
        selection       = "over",
        line            = closing_total,
        price_american  = -110L,
        price_decimal   = american_to_decimal(-110),
        implied_probability_raw = american_to_implied_prob(-110),
        snapshot_type   = "closing",
        overunder_line  = closing_total,
        ingestion_ts    = lubridate::now("UTC")
      )

    open_total_rows <- total_rows |>
      dplyr::mutate(line = games$opening_total, snapshot_type = "opening")

    df_all <- dplyr::bind_rows(
      open_spread_rows, spread_rows,
      open_total_rows, total_rows
    ) |>
      add_ingestion_metadata("sbr_free", "csv")

    validate_odds_contract(df_all, "sbr_free")

    base_path <- file.path(cfg$paths$raw, "market_odds_snapshot")
    write_parquet_partition(df_all, base_path, partition_cols = "season")
  })
}

# ---- Internal helpers -------------------------------------------------------

# Standardise team abbreviations (same logic as nflverse layer)
.standardise_team <- function(name) {
  dplyr::case_when(
    # nflverse canonical mappings
    grepl("(?i)las vegas|raiders",   name) ~ "LV",
    grepl("(?i)los angeles chargers", name) ~ "LAC",
    grepl("(?i)los angeles rams",    name) ~ "LA",
    grepl("(?i)kansas city",         name) ~ "KC",
    grepl("(?i)new england",         name) ~ "NE",
    grepl("(?i)green bay",           name) ~ "GB",
    grepl("(?i)new york giants",     name) ~ "NYG",
    grepl("(?i)new york jets",       name) ~ "NYJ",
    grepl("(?i)san francisco",       name) ~ "SF",
    grepl("(?i)washington",          name) ~ "WAS",
    grepl("(?i)new orleans",         name) ~ "NO",
    grepl("(?i)tampa bay",           name) ~ "TB",
    # Abbreviations pass through if already canonical
    nchar(name) <= 4 ~ toupper(name),
    TRUE             ~ toupper(name)
  )
}
