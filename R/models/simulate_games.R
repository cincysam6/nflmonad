# =============================================================================
# R/models/simulate_games.R
#
# PURPOSE: Monte Carlo simulation layer on top of trained point-estimate models.
#
# WHY SIMULATE?
#   XGBoost gives you a single predicted value (e.g., "home wins by 4.2").
#   But NFL games are noisy — a "4.2 point favorite" wins ~58% of the time,
#   not 100%. Simulation lets you:
#     1. Convert point estimates -> full probability distributions
#     2. Generate cover probabilities (not just predicted margins)
#     3. Quantify uncertainty (confidence intervals on predictions)
#     4. Run "what-if" scenarios (injury sims, weather sims)
#     5. Identify edges vs. the market (expected value calculations)
#
# HOW IT WORKS (conceptual):
#   Step 1: Model predicts E[home_margin] = 4.2 and E[total] = 45.1
#   Step 2: We know NFL game residuals have ~13-14 pt std dev
#   Step 3: Draw N=10,000 samples from Normal(4.2, 13.5)
#   Step 4: Each sample is one simulated game outcome
#   Step 5: Count: how many samples > 0? That's P(home wins)
#            how many samples > spread_line? That's P(covers)
#
# OUTPUT: Per-game probability distributions, not just point estimates
# =============================================================================

suppressPackageStartupMessages({
  library(here)
  library(dplyr)
  library(purrr)
  library(tidyr)
  library(logger)
  library(arrow)
  library(xgboost)
})

source(here::here("R/utils/config_loader.R"))
source(here::here("R/utils/logging.R"))
source(here::here("R/models/train_game_model.R"))

# =============================================================================
# SIMULATION CONSTANTS
# These are calibrated from historical NFL residuals.
# Update annually by running calibrate_residuals().
# =============================================================================

.NFL_MARGIN_SD    <- 13.5   # historical std dev of game margins
.NFL_TOTAL_SD     <- 11.0   # historical std dev of final totals
.NFL_MARGIN_CORR  <- 0.0    # margin and total are ~uncorrelated
.N_SIMS           <- 10000L  # number of Monte Carlo draws per game

# =============================================================================
# CALIBRATION
# Compute empirical residual std from held-out data.
# Run this once after training to update the constants above.
# =============================================================================

#' Calibrate simulation noise from historical residuals
#'
#' Computes the empirical standard deviation of model residuals on the
#' test set. Use this output to update .NFL_MARGIN_SD and .NFL_TOTAL_SD.
#'
#' @param predictions Data frame with columns: home_margin, pred_margin,
#'   total_points, pred_total
#' @return Named list with margin_sd, total_sd, margin_mae, total_mae
#' @export
calibrate_residuals <- function(predictions) {
  margin_resid <- predictions$home_margin - predictions$pred_margin
  total_resid  <- predictions$total_points - predictions$pred_total

  result <- list(
    margin_sd   = sd(margin_resid,  na.rm = TRUE),
    total_sd    = sd(total_resid,   na.rm = TRUE),
    margin_mae  = mean(abs(margin_resid), na.rm = TRUE),
    total_mae   = mean(abs(total_resid),  na.rm = TRUE),
    margin_bias = mean(margin_resid, na.rm = TRUE),  # systematic bias check
    total_bias  = mean(total_resid,  na.rm = TRUE),
    n           = sum(!is.na(margin_resid))
  )

  logger::log_info("Residual calibration results:")
  logger::log_info("  Margin SD:   {round(result$margin_sd, 2)} pts")
  logger::log_info("  Margin MAE:  {round(result$margin_mae, 2)} pts")
  logger::log_info("  Margin Bias: {round(result$margin_bias, 2)} pts (should be ~0)")
  logger::log_info("  Total SD:    {round(result$total_sd, 2)} pts")
  logger::log_info("  Total MAE:   {round(result$total_mae, 2)} pts")

  result
}

# =============================================================================
# POINT ESTIMATE PREDICTION
# Generate model predictions for a dataset.
# =============================================================================

#' Generate point-estimate predictions from trained models
#'
#' @param df Data frame (engineer_features() must have been called)
#' @param models Named list from load_game_models()
#' @return df with pred_margin, pred_total, pred_win_prob columns added
#' @export
predict_games <- function(df, models) {
  feature_cols <- .ENGINEERED_FEATURES
  available    <- intersect(feature_cols, names(df))

  X <- df |>
    dplyr::select(dplyr::all_of(available)) |>
    dplyr::mutate(dplyr::across(everything(), ~ as.numeric(.x))) |>
    as.matrix()

  dmat <- xgboost::xgb.DMatrix(data = X, missing = NA)

  df |>
    dplyr::mutate(
      pred_margin   = predict(models$spread$model,  dmat),
      pred_total    = predict(models$total$model,   dmat),
      pred_win_prob = predict(models$winprob$model, dmat)
    )
}

# =============================================================================
# MONTE CARLO SIMULATION ENGINE
# =============================================================================

#' Simulate a single game N times and return probability summaries
#'
#' @param pred_margin   Model's predicted home margin (point estimate)
#' @param pred_total    Model's predicted total points (point estimate)
#' @param spread_line   Market spread (home perspective, e.g. -3.5)
#' @param total_line    Market total line (e.g. 45.5)
#' @param margin_sd     Simulation noise std dev for margin
#' @param total_sd      Simulation noise std dev for total
#' @param n_sims        Number of Monte Carlo draws
#' @return Named list of probabilities and distribution summaries
simulate_one_game <- function(pred_margin,
                               pred_total,
                               spread_line   = NA_real_,
                               total_line    = NA_real_,
                               margin_sd     = .NFL_MARGIN_SD,
                               total_sd      = .NFL_TOTAL_SD,
                               n_sims        = .N_SIMS) {
  set.seed(NULL)  # allow different seed each call for true MC

  # Draw simulated margins and totals
  sim_margins <- rnorm(n_sims, mean = pred_margin, sd = margin_sd)
  sim_totals  <- rnorm(n_sims, mean = pred_total,  sd = total_sd)

  # Derive simulated scores (approximate split)
  # home_score ≈ (total + margin) / 2
  sim_home <- (sim_totals + sim_margins) / 2
  sim_away <- sim_totals - sim_home

  # Force non-negative (scores can't be negative)
  sim_home <- pmax(sim_home, 0)
  sim_away <- pmax(sim_away, 0)
  sim_margins_final <- sim_home - sim_away

  # Core probabilities
  p_home_win    <- mean(sim_margins_final > 0)
  p_away_win    <- mean(sim_margins_final < 0)
  p_push_win    <- mean(sim_margins_final == 0)

  # Cover probabilities (vs. market spread)
  p_home_cover  <- if (!is.na(spread_line)) mean(sim_margins_final > -spread_line) else NA_real_
  p_away_cover  <- if (!is.na(spread_line)) mean(sim_margins_final < -spread_line) else NA_real_
  p_over        <- if (!is.na(total_line))  mean(sim_home + sim_away > total_line)  else NA_real_
  p_under       <- if (!is.na(total_line))  mean(sim_home + sim_away < total_line)  else NA_real_

  # Distribution summaries
  list(
    # Point estimates
    pred_margin         = pred_margin,
    pred_total          = pred_total,

    # Win probabilities
    p_home_win          = p_home_win,
    p_away_win          = p_away_win,

    # Cover probabilities
    p_home_cover        = p_home_cover,
    p_away_cover        = p_away_cover,
    p_over              = p_over,
    p_under             = p_under,

    # Distribution of simulated margins
    margin_p10          = quantile(sim_margins_final, 0.10),
    margin_p25          = quantile(sim_margins_final, 0.25),
    margin_p50          = quantile(sim_margins_final, 0.50),
    margin_p75          = quantile(sim_margins_final, 0.75),
    margin_p90          = quantile(sim_margins_final, 0.90),
    margin_sim_sd       = sd(sim_margins_final),

    # Distribution of simulated totals
    total_p10           = quantile(sim_home + sim_away, 0.10),
    total_p50           = quantile(sim_home + sim_away, 0.50),
    total_p90           = quantile(sim_home + sim_away, 0.90),

    # Win probability confidence interval (binomial)
    p_home_win_ci_lo    = p_home_win - 1.96 * sqrt(p_home_win * (1 - p_home_win) / n_sims),
    p_home_win_ci_hi    = p_home_win + 1.96 * sqrt(p_home_win * (1 - p_home_win) / n_sims),

    n_sims              = n_sims
  )
}

#' Run Monte Carlo simulations for all games in a data frame
#'
#' @param predictions  Data frame from predict_games() — needs pred_margin,
#'   pred_total, and optionally opening_spread, opening_total
#' @param margin_sd    Margin simulation noise (use calibrate_residuals() output)
#' @param total_sd     Total simulation noise
#' @param n_sims       Number of draws per game
#' @return Data frame with one row per game + all simulation columns
#' @export
simulate_games <- function(predictions,
                            margin_sd = .NFL_MARGIN_SD,
                            total_sd  = .NFL_TOTAL_SD,
                            n_sims    = .N_SIMS) {
  logger::log_info("Simulating {nrow(predictions)} games ({n_sims} draws each)...")
  t0 <- proc.time()

  sim_results <- purrr::pmap_dfr(
    predictions |>
      dplyr::select(
        pred_margin,
        pred_total,
        spread_line  = dplyr::any_of(c("opening_spread", "spread_line")),
        total_line   = dplyr::any_of(c("opening_total",  "total_line"))
      ),
    function(pred_margin, pred_total,
             spread_line = NA_real_, total_line = NA_real_) {
      simulate_one_game(
        pred_margin = pred_margin,
        pred_total  = pred_total,
        spread_line = spread_line,
        total_line  = total_line,
        margin_sd   = margin_sd,
        total_sd    = total_sd,
        n_sims      = n_sims
      ) |> tibble::as_tibble()
    }
  )

  elapsed <- round((proc.time() - t0)[["elapsed"]], 1)
  logger::log_info("Simulation complete in {elapsed}s")

  # Bind identifiers back
  id_cols <- c("game_id", "season", "week", "game_date",
                "home_team", "away_team", "cv_split")
  available_ids <- intersect(id_cols, names(predictions))

  dplyr::bind_cols(
    predictions |> dplyr::select(dplyr::all_of(available_ids)),
    sim_results
  )
}

# =============================================================================
# EXPECTED VALUE CALCULATOR
# Core betting math: EV = (p_win * profit) - (p_lose * stake)
# =============================================================================

#' Calculate expected value for a bet given probability and American odds
#'
#' @param p_win        Simulated probability of winning the bet (0-1)
#' @param american_odds American odds (e.g. -110, +150)
#' @param stake        Bet size (default $100)
#' @return Named list: ev (expected profit), roi (return on investment)
#' @export
calc_ev <- function(p_win, american_odds, stake = 100) {
  # Convert American odds to profit on winning bet
  profit <- if (american_odds > 0) {
    stake * (american_odds / 100)
  } else {
    stake * (100 / abs(american_odds))
  }

  p_lose <- 1 - p_win
  ev     <- (p_win * profit) - (p_lose * stake)
  roi    <- ev / stake

  list(
    ev           = round(ev, 2),
    roi          = round(roi, 4),
    p_win        = round(p_win, 4),
    profit_if_win = round(profit, 2),
    edge_pct     = round(roi * 100, 2)
  )
}

#' Add EV columns to simulation results
#'
#' Assumes standard -110 juice unless odds columns are present.
#'
#' @param sim_df  Data frame from simulate_games()
#' @return sim_df with ev_spread_home, ev_spread_away, ev_over, ev_under added
#' @export
add_ev_columns <- function(sim_df) {
  sim_df |>
    dplyr::mutate(
      # Spread EV (standard -110 juice assumed)
      ev_spread_home = purrr::map_dbl(p_home_cover, ~ calc_ev(.x, -110)$ev),
      ev_spread_away = purrr::map_dbl(p_away_cover, ~ calc_ev(.x, -110)$ev),
      roi_spread_home = ev_spread_home / 100,
      roi_spread_away = ev_spread_away / 100,

      # Total EV
      ev_over  = purrr::map_dbl(p_over,  ~ calc_ev(.x, -110)$ev),
      ev_under = purrr::map_dbl(p_under, ~ calc_ev(.x, -110)$ev),
      roi_over  = ev_over  / 100,
      roi_under = ev_under / 100,

      # Edge vs. market (model win prob minus market implied prob)
      # opening_home_win_prob from market (if available)
      win_prob_edge = dplyr::if_else(
        !is.na(pred_win_prob) & "opening_home_win_prob" %in% names(.),
        pred_win_prob - opening_home_win_prob,
        NA_real_
      )
    )
}

# =============================================================================
# FULL SIMULATION PIPELINE
# =============================================================================

#' Run the full predict -> simulate -> EV pipeline
#'
#' @param df      Data frame (engineer_features() applied)
#' @param models  From load_game_models()
#' @param n_sims  Monte Carlo draws
#' @return Data frame with predictions, simulations, and EV columns
#' @export
run_simulation_pipeline <- function(df, models, n_sims = .N_SIMS) {
  df |>
    predict_games(models)     |>
    simulate_games(n_sims = n_sims) |>
    add_ev_columns()
}
