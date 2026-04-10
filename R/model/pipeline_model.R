# =============================================================================
# pipeline_model.R
#
# MASTER MODEL PIPELINE — entry point for all modeling workflows
#
# Usage:
#   Rscript pipeline_model.R train          # train models on backtest data
#   Rscript pipeline_model.R validate       # validate on test split + save report
#   Rscript pipeline_model.R predict        # predict current week's games
#   Rscript pipeline_model.R full           # train + validate + predict
#
# Interactive usage:
#   source("pipeline_model.R")
#   results <- run_model_pipeline("full")
# =============================================================================

suppressPackageStartupMessages({
  library(here)
  library(dplyr)
  library(logger)
  library(arrow)
  library(purrr)
})

# Load all model modules
purrr::walk(c(
  "R/utils/config_loader.R",
  "R/utils/logging.R",
  "R/utils/db_connection.R",
  "R/utils/parquet_io.R",
  "R/models/train_game_model.R",
  "R/models/simulate_games.R",
  "R/models/validate_model.R"
), ~ source(here::here(.x)))

# =============================================================================
# PIPELINE MODES
# =============================================================================

#' Train mode: train models on backtest data and save artifacts
run_train <- function(cfg = load_config()) {
  logger::log_info("MODE: TRAIN")

  models <- train_game_models(cfg = cfg)

  logger::log_info("Training complete. Models saved to models/game/")
  logger::log_info("Next step: run validate or predict")

  invisible(models)
}

#' Validate mode: load trained models, run on test split, produce report
run_validate <- function(cfg = load_config(), cv_split = "test") {
  logger::log_info("MODE: VALIDATE (split={cv_split})")

  # Load models
  models <- load_game_models()

  # Load and engineer features
  raw_df <- load_training_data(cfg)
  df     <- engineer_features(raw_df)

  # Run full simulation pipeline
  logger::log_info("Running predict + simulate pipeline...")
  sim_df <- run_simulation_pipeline(df, models, n_sims = .N_SIMS)

  # Join actual outcomes back in
  actuals <- raw_df |>
    dplyr::select(game_id, home_margin, total_points, home_win_flag,
                  home_score, away_score,
                  dplyr::any_of(c("home_covered_close", "over_close_flag",
                                  "opening_spread", "opening_total",
                                  "closing_spread", "closing_total")))

  sim_with_actuals <- sim_df |>
    dplyr::left_join(actuals, by = "game_id", suffix = c("", "_actual"))

  # Run validation suite
  report <- run_validation_suite(
    sim_df          = sim_with_actuals,
    cv_split_filter = cv_split
  )

  # Print summary to console
  print_validation_summary(report)

  invisible(list(sim_df = sim_with_actuals, report = report))
}

#' Predict mode: predict upcoming games (not yet completed)
run_predict <- function(cfg = load_config(), season = NULL, week = NULL) {
  logger::log_info("MODE: PREDICT")

  # Load models
  models <- load_game_models()
  meta   <- models$metadata
  logger::log_info("Using models trained at {meta$trained_at}")

  # Load full mart (includes upcoming games with game_completed_flag = 0)
  path <- file.path(cfg$paths$marts, "mart_game_modeling")
  if (!fs::dir_exists(path)) {
    stop("mart_game_modeling not found. Run run_transforms() first.")
  }

  full_df <- arrow::open_dataset(path) |> dplyr::collect()

  # Filter to upcoming games
  upcoming <- full_df |>
    dplyr::filter(game_completed_flag == 0)

  # Override with specific week if provided
  if (!is.null(season) && !is.null(week)) {
    upcoming <- upcoming |>
      dplyr::filter(season == !!season, week == !!week)
  }

  if (nrow(upcoming) == 0) {
    logger::log_info("No upcoming games found.")
    return(invisible(tibble::tibble()))
  }

  logger::log_info("Predicting {nrow(upcoming)} upcoming games...")

  # Engineer features and simulate
  upcoming_eng <- engineer_features(upcoming)
  sim_df       <- run_simulation_pipeline(upcoming_eng, models)

  # Format output for consumption
  output <- sim_df |>
    dplyr::select(
      game_id, season, week, game_date,
      home_team, away_team,
      pred_margin, pred_total, pred_win_prob,
      p_home_win, p_away_win,
      p_home_cover, p_away_cover,
      p_over, p_under,
      margin_p25, margin_p50, margin_p75,
      ev_spread_home, ev_spread_away,
      ev_over, ev_under,
      dplyr::any_of(c("opening_spread", "opening_total", "opening_home_win_prob"))
    ) |>
    dplyr::arrange(game_date, game_id)

  # Save predictions
  pred_dir <- here::here("reports/predictions")
  fs::dir_create(pred_dir, recurse = TRUE)
  pred_path <- file.path(pred_dir,
    glue::glue("predictions_{format(Sys.Date(), '%Y%m%d')}.csv"))
  readr::write_csv(output, pred_path)
  logger::log_info("Predictions saved -> {pred_path}")

  # Print to console
  cat("\n===== GAME PREDICTIONS =====\n")
  output |>
    dplyr::mutate(
      matchup      = glue::glue("{away_team} @ {home_team}"),
      pred_line    = glue::glue("{round(pred_margin, 1)} (home)"),
      pred_total_r = round(pred_total, 1),
      p_home_pct   = glue::glue("{round(p_home_win*100, 1)}%"),
      cover_pct    = glue::glue("{round(p_home_cover*100, 1)}%"),
      over_pct     = glue::glue("{round(p_over*100, 1)}%")
    ) |>
    dplyr::select(matchup, pred_line, pred_total_r,
                  p_home_pct, cover_pct, over_pct) |>
    print(n = Inf)

  invisible(output)
}

#' Full pipeline: train + validate + predict
run_full_pipeline <- function(cfg = load_config()) {
  logger::log_info("MODE: FULL PIPELINE")

  models  <- run_train(cfg)
  results <- run_validate(cfg)
  preds   <- run_predict(cfg)

  invisible(list(models = models, validation = results, predictions = preds))
}

# =============================================================================
# MASTER DISPATCHER
# =============================================================================

#' Run model pipeline in specified mode
#'
#' @param mode One of: "train", "validate", "predict", "full"
#' @param cfg  Config list
#' @export
run_model_pipeline <- function(mode = "full", cfg = load_config()) {
  setup_logging(cfg, context = glue::glue("model_{mode}"))

  switch(mode,
    train    = run_train(cfg),
    validate = run_validate(cfg),
    predict  = run_predict(cfg),
    full     = run_full_pipeline(cfg),
    stop("Unknown mode: ", mode,
         ". Use one of: train, validate, predict, full")
  )
}

# =============================================================================
# CLI
# =============================================================================
if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  mode <- if (length(args) >= 1) args[1] else "full"
  cfg  <- load_config()
  run_model_pipeline(mode = mode, cfg = cfg)
}
