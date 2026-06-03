# =============================================================================
# R/models/validate_model.R
#
# PURPOSE: Full model validation and performance reporting suite.
#
# REPORTS PRODUCED:
#   1. Predictive accuracy    — RMSE, MAE, R², calibration
#   2. Betting performance    — ATS record, O/U record, ROI by season/week
#   3. Market comparison      — model vs. closing line (CLV analysis)
#   4. Calibration curves     — do predicted win probs match actual win rates?
#   5. Variance decomposition — how much variance does the model explain?
#   6. Segment analysis       — performance by div game, dome, primetime, etc.
#   7. Season-by-season       — temporal stability check
# =============================================================================

suppressPackageStartupMessages({
  library(here)
  library(dplyr)
  library(tidyr)
  library(purrr)
  library(logger)
  library(arrow)
  library(ggplot2)
  library(glue)
})

source(here::here("R/utils/config_loader.R"))
source(here::here("R/utils/logging.R"))
source(here::here("R/models/train_game_model.R"))
source(here::here("R/models/simulate_games.R"))

# =============================================================================
# CORE ACCURACY METRICS
# =============================================================================

#' Compute regression metrics for a numeric prediction
#'
#' @param actual  Numeric vector of actual values
#' @param pred    Numeric vector of predicted values
#' @param label   Label for logging
#' @return Named list of metrics
regression_metrics <- function(actual, pred, label = "") {
  resid   <- actual - pred
  ss_res  <- sum(resid^2, na.rm = TRUE)
  ss_tot  <- sum((actual - mean(actual, na.rm = TRUE))^2, na.rm = TRUE)

  list(
    label   = label,
    n       = sum(!is.na(resid)),
    rmse    = sqrt(mean(resid^2, na.rm = TRUE)),
    mae     = mean(abs(resid), na.rm = TRUE),
    bias    = mean(resid, na.rm = TRUE),        # positive = model over-predicts
    r2      = 1 - ss_res / ss_tot,
    sd_pred = sd(pred, na.rm = TRUE),
    sd_actual = sd(actual, na.rm = TRUE)
  )
}

#' Compute binary classification metrics for win probability model
#'
#' @param actual  Binary vector (0/1) of actual outcomes
#' @param prob    Numeric vector of predicted probabilities
#' @return Named list of metrics
classification_metrics <- function(actual, prob, label = "") {
  # Log loss
  eps     <- 1e-7
  prob_cl <- pmin(pmax(prob, eps), 1 - eps)
  logloss <- -mean(actual * log(prob_cl) + (1 - actual) * log(1 - prob_cl), na.rm = TRUE)

  # Brier score
  brier   <- mean((actual - prob)^2, na.rm = TRUE)

  # Accuracy at 50% threshold
  pred_class <- as.integer(prob >= 0.5)
  accuracy   <- mean(pred_class == actual, na.rm = TRUE)

  # Baseline (always predict majority class)
  base_acc <- max(mean(actual, na.rm = TRUE), 1 - mean(actual, na.rm = TRUE))

  list(
    label        = label,
    n            = sum(!is.na(actual)),
    logloss      = logloss,
    brier_score  = brier,
    accuracy     = accuracy,
    baseline_acc = base_acc,
    accuracy_lift = accuracy - base_acc,
    mean_pred_prob = mean(prob, na.rm = TRUE),
    actual_win_rate = mean(actual, na.rm = TRUE)
  )
}

# =============================================================================
# BETTING PERFORMANCE METRICS
# =============================================================================

#' Compute ATS (Against The Spread) record and ROI
#'
#' @param sim_df Data frame from simulate_games() with actual outcomes
#' @param min_edge_pct Minimum model edge % to count a bet (0 = all games)
#' @return Summary tibble with record, win_pct, roi, profit columns
#' @export
ats_performance <- function(sim_df, min_edge_pct = 0) {
  # Require actual outcomes and spread line
  df <- sim_df |>
    dplyr::filter(
      !is.na(home_margin),
      !is.na(opening_spread),
      !is.na(p_home_cover)
    )

  if (nrow(df) == 0) {
    logger::log_warn("No games with both actual margins and spread lines for ATS analysis")
    return(tibble::tibble())
  }

  # Actual cover results
  df <- df |>
    dplyr::mutate(
      # Actual cover: home margin > -spread_line (spread_line is home perspective)
      actual_home_covered = as.integer(home_margin > -opening_spread),
      actual_away_covered = as.integer(home_margin < -opening_spread),
      push                = as.integer(home_margin == -opening_spread),

      # Model-recommended side (bet the side with higher cover prob)
      bet_home  = as.integer(p_home_cover > 0.5 + min_edge_pct / 2),
      bet_away  = as.integer(p_away_cover > 0.5 + min_edge_pct / 2),

      # Did model-recommended bet win?
      bet_won = dplyr::case_when(
        bet_home == 1 & actual_home_covered == 1 ~ 1L,
        bet_away == 1 & actual_away_covered == 1 ~ 1L,
        push == 1 ~ NA_integer_,  # push = no result
        TRUE ~ 0L
      ),

      # P&L per game (standard -110 juice, $110 to win $100)
      pnl = dplyr::case_when(
        bet_won == 1L ~ 100,
        bet_won == 0L ~ -110,
        TRUE          ~ 0    # push
      )
    )

  bets <- df |> dplyr::filter(!is.na(bet_won))

  tibble::tibble(
    n_games      = nrow(df),
    n_bets       = nrow(bets),
    n_won        = sum(bets$bet_won, na.rm = TRUE),
    n_lost       = sum(1 - bets$bet_won, na.rm = TRUE),
    n_push       = sum(df$push),
    win_pct      = mean(bets$bet_won, na.rm = TRUE),
    profit       = sum(bets$pnl, na.rm = TRUE),
    roi          = sum(bets$pnl) / (nrow(bets) * 110),
    breakeven_wp = 110 / 210  # ~52.38% needed to break even at -110
  )
}

#' Compute O/U (over-under) record and ROI
#' @export
ou_performance <- function(sim_df, min_edge_pct = 0) {
  df <- sim_df |>
    dplyr::filter(
      !is.na(total_points),
      !is.na(opening_total),
      !is.na(p_over)
    ) |>
    dplyr::mutate(
      actual_over  = as.integer(total_points > opening_total),
      actual_under = as.integer(total_points < opening_total),
      push         = as.integer(total_points == opening_total),
      bet_over     = as.integer(p_over  > 0.5 + min_edge_pct / 2),
      bet_under    = as.integer(p_under > 0.5 + min_edge_pct / 2),
      bet_won = dplyr::case_when(
        bet_over  == 1 & actual_over  == 1 ~ 1L,
        bet_under == 1 & actual_under == 1 ~ 1L,
        push == 1 ~ NA_integer_,
        TRUE ~ 0L
      ),
      pnl = dplyr::case_when(
        bet_won == 1L ~ 100,
        bet_won == 0L ~ -110,
        TRUE          ~ 0
      )
    )

  bets <- df |> dplyr::filter(!is.na(bet_won))

  tibble::tibble(
    n_games   = nrow(df),
    n_bets    = nrow(bets),
    n_won     = sum(bets$bet_won, na.rm = TRUE),
    n_lost    = sum(1 - bets$bet_won, na.rm = TRUE),
    win_pct   = mean(bets$bet_won, na.rm = TRUE),
    profit    = sum(bets$pnl, na.rm = TRUE),
    roi       = sum(bets$pnl) / (nrow(bets) * 110),
    breakeven = 110 / 210
  )
}

# =============================================================================
# CALIBRATION ANALYSIS
# Do predicted probabilities match actual frequencies?
# =============================================================================

#' Compute calibration by probability bucket
#'
#' @param actual Actual binary outcomes (0/1)
#' @param prob   Predicted probabilities
#' @param n_bins Number of calibration buckets
#' @return Data frame with one row per bucket
#' @export
calibration_table <- function(actual, prob, n_bins = 10) {
  df <- tibble::tibble(actual = actual, prob = prob) |>
    dplyr::filter(!is.na(actual), !is.na(prob)) |>
    dplyr::mutate(
      bin = cut(prob, breaks = seq(0, 1, length.out = n_bins + 1),
                include.lowest = TRUE, labels = FALSE)
    ) |>
    dplyr::group_by(bin) |>
    dplyr::summarise(
      n           = dplyr::n(),
      mean_pred   = mean(prob),
      actual_rate = mean(actual),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      calibration_error = actual_rate - mean_pred,
      bin_label = glue::glue("{round(mean_pred*100)}%")
    )

  # Expected Calibration Error (ECE)
  ece <- sum(df$n * abs(df$calibration_error)) / sum(df$n)
  attr(df, "ece") <- ece
  logger::log_info("Expected Calibration Error (ECE): {round(ece, 4)}")

  df
}

# =============================================================================
# MARKET COMPARISON (CLV — Closing Line Value)
# =============================================================================

#' Compare model predictions to market closing line
#'
#' CLV = closing line value. If your model consistently predicts on the
#' correct side of where the line closes, you have an information edge.
#'
#' @param sim_df Data frame with pred_margin, opening_spread, closing_spread
#' @return Summary of model vs. market agreement
#' @export
clv_analysis <- function(sim_df) {
  df <- sim_df |>
    dplyr::filter(
      !is.na(pred_margin),
      !is.na(opening_spread),
      !is.na(closing_spread)  # need closing line — only available retrospectively
    ) |>
    dplyr::mutate(
      # Line movement: positive = line moved toward home team
      line_movement       = opening_spread - closing_spread,

      # Did the model agree with where the line moved?
      model_side_home     = pred_margin > -opening_spread,
      line_moved_home     = closing_spread < opening_spread,
      model_agreed_with_movement = model_side_home == line_moved_home,

      # Model's "edge" over opening line
      model_vs_opening    = pred_margin - (-opening_spread),

      # Magnitude of line movement
      abs_line_movement   = abs(line_movement)
    )

  list(
    n_games                = nrow(df),
    pct_agree_with_market  = mean(df$model_agreed_with_movement, na.rm = TRUE),
    mean_line_movement     = mean(df$line_movement, na.rm = TRUE),
    mean_abs_movement      = mean(df$abs_line_movement, na.rm = TRUE),
    mean_model_vs_opening  = mean(df$model_vs_opening, na.rm = TRUE),
    # CLV by quartile of movement magnitude
    by_movement_size = df |>
      dplyr::mutate(
        movement_quartile = dplyr::ntile(abs_line_movement, 4)
      ) |>
      dplyr::group_by(movement_quartile) |>
      dplyr::summarise(
        n = dplyr::n(),
        pct_agree = mean(model_agreed_with_movement, na.rm = TRUE),
        mean_movement = mean(abs_line_movement),
        .groups = "drop"
      )
  )
}

# =============================================================================
# SEGMENT ANALYSIS
# =============================================================================

#' Break down model performance by game segment
#' @export
segment_analysis <- function(sim_df, actual_col = "home_margin",
                              pred_col = "pred_margin") {
  compute_segment <- function(df, group_label) {
    regression_metrics(df[[actual_col]], df[[pred_col]], label = group_label) |>
      tibble::as_tibble()
  }

  bind_rows(
    # Overall
    compute_segment(sim_df, "overall"),

    # By season type
    sim_df |> dplyr::group_by(season_type) |>
      dplyr::group_modify(~ compute_segment(.x, .y$season_type)) |>
      dplyr::ungroup() |> dplyr::select(-season_type),

    # By CV split
    sim_df |> dplyr::filter(!is.na(cv_split)) |>
      dplyr::group_by(cv_split) |>
      dplyr::group_modify(~ compute_segment(.x, .y$cv_split)) |>
      dplyr::ungroup() |> dplyr::select(-cv_split),

    # Divisional vs. non-divisional
    sim_df |> dplyr::mutate(
      segment = dplyr::if_else(div_game == 1, "div_game", "non_div_game")
    ) |>
      dplyr::group_by(segment) |>
      dplyr::group_modify(~ compute_segment(.x, .y$segment)) |>
      dplyr::ungroup() |> dplyr::select(-segment),

    # Primetime
    sim_df |> dplyr::mutate(
      segment = dplyr::if_else(primetime_flag == 1, "primetime", "non_primetime")
    ) |>
      dplyr::group_by(segment) |>
      dplyr::group_modify(~ compute_segment(.x, .y$segment)) |>
      dplyr::ungroup() |> dplyr::select(-segment)
  )
}

#' Season-by-season performance breakdown
#' @export
season_performance <- function(sim_df) {
  sim_df |>
    dplyr::filter(!is.na(home_margin), !is.na(pred_margin)) |>
    dplyr::group_by(season) |>
    dplyr::summarise(
      n         = dplyr::n(),
      rmse      = sqrt(mean((home_margin - pred_margin)^2)),
      mae       = mean(abs(home_margin - pred_margin)),
      bias      = mean(home_margin - pred_margin),
      r2        = 1 - sum((home_margin - pred_margin)^2) /
                      sum((home_margin - mean(home_margin))^2),
      ats_win_pct = mean(
        dplyr::if_else(!is.na(opening_spread),
          as.numeric((home_margin > -opening_spread) == (pred_margin > -opening_spread)),
          NA_real_), na.rm = TRUE),
      .groups = "drop"
    )
}

# =============================================================================
# FULL VALIDATION REPORT
# =============================================================================

#' Run the complete validation suite and produce a report
#'
#' @param sim_df    Data frame from run_simulation_pipeline() with actual outcomes
#' @param report_dir Directory to save report outputs
#' @param cv_split_filter Which split to report on ("test", "validation", or "all")
#' @return Named list of all validation results
#' @export
run_validation_suite <- function(sim_df,
                                  report_dir       = here::here("reports/model_validation"),
                                  cv_split_filter  = "test") {
  fs::dir_create(report_dir, recurse = TRUE)

  logger::log_info("========================================")
  logger::log_info("MODEL VALIDATION SUITE — {Sys.Date()}")
  logger::log_info("Evaluating on: {cv_split_filter}")
  logger::log_info("========================================")

  # Filter to evaluation split
  eval_df <- if (cv_split_filter == "all") {
    sim_df
  } else {
    sim_df |> dplyr::filter(cv_split == cv_split_filter)
  }

  logger::log_info("Evaluation rows: {nrow(eval_df)}")

  # ---- 1. Accuracy metrics --------------------------------------------------
  logger::log_info("--- 1. Accuracy Metrics ---")

  acc_spread  <- regression_metrics(eval_df$home_margin,   eval_df$pred_margin, "spread")
  acc_total   <- regression_metrics(eval_df$total_points,  eval_df$pred_total,  "total")
  acc_winprob <- classification_metrics(eval_df$home_win_flag, eval_df$pred_win_prob, "winprob")

  logger::log_info("Spread  RMSE={round(acc_spread$rmse,2)} MAE={round(acc_spread$mae,2)} R2={round(acc_spread$r2,3)} Bias={round(acc_spread$bias,2)}")
  logger::log_info("Total   RMSE={round(acc_total$rmse,2)} MAE={round(acc_total$mae,2)} R2={round(acc_total$r2,3)} Bias={round(acc_total$bias,2)}")
  logger::log_info("WinProb LogLoss={round(acc_winprob$logloss,4)} Brier={round(acc_winprob$brier_score,4)} Acc={round(acc_winprob$accuracy,3)}")

  # ---- 2. Residual calibration ----------------------------------------------
  logger::log_info("--- 2. Residual Calibration ---")
  calib <- calibrate_residuals(eval_df)

  # ---- 3. Betting performance -----------------------------------------------
  logger::log_info("--- 3. Betting Performance ---")

  ats_all  <- ats_performance(eval_df, min_edge_pct = 0)
  ats_edge <- ats_performance(eval_df, min_edge_pct = 0.05)  # 5%+ edge games only
  ou_all   <- ou_performance(eval_df,  min_edge_pct = 0)
  ou_edge  <- ou_performance(eval_df,  min_edge_pct = 0.05)

  logger::log_info("ATS (all games): {ats_all$n_won}-{ats_all$n_lost} ({round(ats_all$win_pct*100,1)}%) ROI={round(ats_all$roi*100,1)}%")
  logger::log_info("ATS (5%%+ edge): {ats_edge$n_won}-{ats_edge$n_lost} ({round(ats_edge$win_pct*100,1)}%) ROI={round(ats_edge$roi*100,1)}%")
  logger::log_info("O/U  (all games): {ou_all$n_won}-{ou_all$n_lost} ({round(ou_all$win_pct*100,1)}%) ROI={round(ou_all$roi*100,1)}%")

  # ---- 4. Calibration curve -------------------------------------------------
  logger::log_info("--- 4. Win Probability Calibration ---")
  cal_table <- calibration_table(eval_df$home_win_flag, eval_df$pred_win_prob)

  # ---- 5. Segment analysis --------------------------------------------------
  logger::log_info("--- 5. Segment Analysis ---")
  segments <- segment_analysis(eval_df)
  print(segments |> dplyr::select(label, n, rmse, mae, r2, bias))

  # ---- 6. Season-by-season --------------------------------------------------
  logger::log_info("--- 6. Season-by-Season Performance ---")
  by_season <- season_performance(eval_df)
  print(by_season)

  # ---- 7. CLV analysis (if closing lines available) -------------------------
  clv <- NULL
  if ("closing_spread" %in% names(eval_df)) {
    logger::log_info("--- 7. CLV Analysis ---")
    clv <- clv_analysis(eval_df)
    logger::log_info("Model agrees with line movement: {round(clv$pct_agree_with_market*100,1)}%")
  }

  # ---- Assemble report ------------------------------------------------------
  report <- list(
    evaluated_at    = Sys.time(),
    cv_split        = cv_split_filter,
    n_games         = nrow(eval_df),

    accuracy = list(
      spread  = acc_spread,
      total   = acc_total,
      winprob = acc_winprob
    ),

    calibration      = calib,
    calibration_curve = cal_table,

    betting = list(
      ats_all_games     = ats_all,
      ats_edge_games    = ats_edge,
      ou_all_games      = ou_all,
      ou_edge_games     = ou_edge
    ),

    segments    = segments,
    by_season   = by_season,
    clv         = clv
  )

  # Save report as RDS
  report_path <- file.path(report_dir,
    glue::glue("validation_{cv_split_filter}_{format(Sys.Date(), '%Y%m%d')}.rds"))
  saveRDS(report, report_path)
  logger::log_info("Report saved -> {report_path}")

  # Save key metrics as CSV for easy inspection
  by_season |>
    readr::write_csv(file.path(report_dir, "season_performance.csv"))
  segments |>
    readr::write_csv(file.path(report_dir, "segment_performance.csv"))

  invisible(report)
}

# =============================================================================
# PRINT SUMMARY
# =============================================================================

#' Print a clean console summary of validation results
#' @export
print_validation_summary <- function(report) {
  cat("\n")
  cat("╔══════════════════════════════════════════════════════╗\n")
  cat("║          NFL MODEL VALIDATION SUMMARY                ║\n")
  cat(glue::glue("║  Split: {report$cv_split:<10} Games: {report$n_games:<6} {format(report$evaluated_at, '%Y-%m-%d'):<10}║\n"))
  cat("╠══════════════════════════════════════════════════════╣\n")
  cat("║  ACCURACY                                            ║\n")
  cat(glue::glue("║  Spread  RMSE: {round(report$accuracy$spread$rmse,2):<5}  MAE: {round(report$accuracy$spread$mae,2):<5}  R²: {round(report$accuracy$spread$r2,3):<5}  ║\n"))
  cat(glue::glue("║  Total   RMSE: {round(report$accuracy$total$rmse,2):<5}  MAE: {round(report$accuracy$total$mae,2):<5}  R²: {round(report$accuracy$total$r2,3):<5}  ║\n"))
  cat(glue::glue("║  WinProb LogLoss: {round(report$accuracy$winprob$logloss,4):<7}  Brier: {round(report$accuracy$winprob$brier_score,4):<7}  ║\n"))
  cat("╠══════════════════════════════════════════════════════╣\n")
  cat("║  BETTING PERFORMANCE (standard -110 juice)           ║\n")
  b <- report$betting
  cat(glue::glue("║  ATS all:  {b$ats_all_games$n_won}-{b$ats_all_games$n_lost} ({round(b$ats_all_games$win_pct*100,1)}%)  ROI: {round(b$ats_all_games$roi*100,1)}%          ║\n"))
  cat(glue::glue("║  ATS edge: {b$ats_edge_games$n_won}-{b$ats_edge_games$n_lost} ({round(b$ats_edge_games$win_pct*100,1)}%)  ROI: {round(b$ats_edge_games$roi*100,1)}%          ║\n"))
  cat(glue::glue("║  O/U  all: {b$ou_all_games$n_won}-{b$ou_all_games$n_lost} ({round(b$ou_all_games$win_pct*100,1)}%)  ROI: {round(b$ou_all_games$roi*100,1)}%          ║\n"))
  cat("╠══════════════════════════════════════════════════════╣\n")
  cat("║  RESIDUAL CALIBRATION                                ║\n")
  cat(glue::glue("║  Margin SD: {round(report$calibration$margin_sd,1):<5} pts  Bias: {round(report$calibration$margin_bias,2):<5} pts        ║\n"))
  cat(glue::glue("║  Total  SD: {round(report$calibration$total_sd,1):<5} pts  Bias: {round(report$calibration$total_bias,2):<5} pts        ║\n"))
  cat("╚══════════════════════════════════════════════════════╝\n")
}
