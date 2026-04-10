# =============================================================================
# R/models/train_game_model.R
#
# PURPOSE: Train NFL game prediction models (spread, total, win probability)
# INPUTS:  mart_backtest_game (parquet) — leakage-safe, cv_split pre-assigned
# OUTPUTS: models/ directory — saved model objects + feature importance
#
# THREE MODEL TARGETS:
#   1. home_margin       — continuous (spread model)
#   2. total_points      — continuous (total/over-under model)
#   3. home_win_flag     — binary (win probability model)
#
# CV STRATEGY: Temporal — never random split across seasons
#   train:      seasons < 2018
#   validation: 2018-2021  (hyperparameter tuning, early stopping)
#   test:       2022+       (held-out, final evaluation only)
# =============================================================================

suppressPackageStartupMessages({
  library(here)
  library(dplyr)
  library(tidyr)
  library(purrr)
  library(logger)
  library(arrow)
  library(xgboost)
  library(yaml)
})

source(here::here("R/utils/config_loader.R"))
source(here::here("R/utils/logging.R"))
source(here::here("R/utils/db_connection.R"))

# =============================================================================
# FEATURE REGISTRY
# Defines which columns are features vs. targets vs. identifiers.
# Edit this list to add/remove features — model re-trains automatically.
# =============================================================================

.GAME_IDENTIFIERS <- c(
  "game_id", "season", "week", "game_date",
  "home_team", "away_team", "cv_split",
  "game_completed_flag", "season_type"
)

.GAME_TARGETS <- c(
  "home_margin",       # spread target (home score - away score)
  "total_points",      # total target
  "home_win_flag",     # win prob target
  "home_score",        # raw scores (not used as features)
  "away_score",
  "home_covered_close",
  "over_close_flag"
)

.GAME_FEATURES <- c(
  # --- Team strength (lagged rolling — pregame safe) ---
  "home_epa_pp_blended", "away_epa_pp_blended",
  "home_def_epa_pp_blended", "away_def_epa_pp_blended",
  "home_epa_pp_std", "away_epa_pp_std",
  "home_pass_epa_std", "away_pass_epa_std",
  "home_rush_epa_std", "away_rush_epa_std",
  "home_def_epa_pp_std", "away_def_epa_pp_std",
  "home_def_pass_epa_std", "away_def_pass_epa_std",
  "home_def_rush_epa_std", "away_def_rush_epa_std",
  "home_success_rate_std", "away_success_rate_std",
  "home_def_success_rate_allowed_std", "away_def_success_rate_allowed_std",
  "home_explosive_pass_rate_std", "away_explosive_pass_rate_std",
  "home_pass_rate_std", "away_pass_rate_std",
  "home_turnovers_pg_std", "away_turnovers_pg_std",
  "home_def_to_forced_std", "away_def_to_forced_std",
  "home_rz_td_rate_std", "away_rz_td_rate_std",
  "home_pts_for_std", "away_pts_for_std",
  "home_pts_against_std", "away_pts_against_std",
  "home_sacks_allowed_std", "away_sacks_allowed_std",
  "home_def_sacks_std", "away_def_sacks_std",
  "home_epa_pp_l5", "away_epa_pp_l5",
  "home_epa_pp_l3", "away_epa_pp_l3",
  "home_games_played", "away_games_played",

  # --- Differentials (derived below) ---
  # epa_diff, def_epa_diff, etc. added in feature engineering

  # --- Schedule / rest ---
  "home_rest_days", "away_rest_days",
  "rest_days_advantage_home",
  "home_bye_prior", "away_bye_prior",
  "home_short_week", "away_short_week",

  # --- Game context ---
  "div_game", "conf_game", "primetime_flag",

  # --- Injury burden ---
  "home_injury_burden", "away_injury_burden",
  "home_qb_out", "away_qb_out",
  "home_wr_burden", "away_wr_burden",

  # --- Odds (opening only — pregame safe) ---
  "opening_spread",
  "opening_total",
  "opening_home_win_prob"
)

# =============================================================================
# FEATURE ENGINEERING
# Adds derived differential features and encodes categoricals.
# =============================================================================

engineer_features <- function(df) {
  df |>
    dplyr::mutate(
      # EPA differentials
      epa_diff            = home_epa_pp_blended    - away_epa_pp_blended,
      def_epa_diff        = home_def_epa_pp_blended - away_def_epa_pp_blended,
      net_epa_diff        = epa_diff - def_epa_diff,
      pass_epa_diff       = home_pass_epa_std      - away_pass_epa_std,
      rush_epa_diff       = home_rush_epa_std      - away_rush_epa_std,
      success_rate_diff   = home_success_rate_std  - away_success_rate_std,
      injury_burden_diff  = home_injury_burden     - away_injury_burden,
      pts_diff            = home_pts_for_std       - away_pts_for_std,

      # Recent form differentials
      epa_l5_diff         = home_epa_pp_l5 - away_epa_pp_l5,
      epa_l3_diff         = home_epa_pp_l3 - away_epa_pp_l3,

      # Home field advantage proxy (always home team in this dataset)
      home_field          = 1L,

      # Season progress (week normalized 0-1)
      week_normalized     = (week - 1) / 17,

      # Encode binary as integer
      dplyr::across(c(div_game, conf_game, primetime_flag,
                      home_bye_prior, away_bye_prior,
                      home_short_week, away_short_week,
                      home_qb_out, away_qb_out),
                    ~ as.integer(coalesce(.x, 0L)))
    )
}

# Full feature list including engineered differentials
.ENGINEERED_FEATURES <- c(
  .GAME_FEATURES,
  "epa_diff", "def_epa_diff", "net_epa_diff",
  "pass_epa_diff", "rush_epa_diff", "success_rate_diff",
  "injury_burden_diff", "pts_diff",
  "epa_l5_diff", "epa_l3_diff",
  "home_field", "week_normalized"
)

# =============================================================================
# DATA LOADING
# =============================================================================

load_training_data <- function(cfg = load_config()) {
  path <- file.path(cfg$paths$marts, "mart_backtest_game")

  if (!fs::dir_exists(path)) {
    stop("mart_backtest_game not found at: ", path,
         "\nRun run_transforms() first.")
  }

  df <- arrow::open_dataset(path) |>
    dplyr::collect()

  logger::log_info("Loaded mart_backtest_game: {nrow(df)} rows, {ncol(df)} cols")
  logger::log_info("Season range: {min(df$season)} - {max(df$season)}")
  logger::log_info("CV split distribution:")
  df |> dplyr::count(cv_split) |> print()

  df
}

# =============================================================================
# TRAIN / VALIDATION / TEST SPLIT
# =============================================================================

split_data <- function(df) {
  list(
    train = df |> dplyr::filter(cv_split == "train"),
    val   = df |> dplyr::filter(cv_split == "validation"),
    test  = df |> dplyr::filter(cv_split == "test")
  )
}

# =============================================================================
# XGBOOST MATRIX BUILDER
# =============================================================================

.build_xgb_matrix <- function(df, target_col, feature_cols) {
  # Keep only features that exist in this dataframe
  available_features <- intersect(feature_cols, names(df))
  missing <- setdiff(feature_cols, names(df))
  if (length(missing) > 0) {
    logger::log_warn("Missing features (will be ignored): {paste(missing, collapse=', ')}")
  }

  X <- df |>
    dplyr::select(dplyr::all_of(available_features)) |>
    dplyr::mutate(dplyr::across(everything(), ~ as.numeric(.x))) |>
    as.matrix()

  y <- as.numeric(df[[target_col]])

  xgboost::xgb.DMatrix(data = X, label = y, missing = NA)
}

# =============================================================================
# MODEL TRAINING
# =============================================================================

#' Train a single XGBoost model
#'
#' @param train_df Training data frame
#' @param val_df   Validation data frame
#' @param target   Target column name
#' @param objective XGBoost objective string
#' @param feature_cols Character vector of feature column names
#' @param params   Named list of XGBoost hyperparameters (optional overrides)
#' @return List with model, feature_names, importance, train_metrics, val_metrics
train_xgb_model <- function(train_df, val_df, target, objective,
                              feature_cols = .ENGINEERED_FEATURES,
                              params = list()) {
  logger::log_info("Training model: target={target}, objective={objective}")
  logger::log_info("Train rows: {nrow(train_df)} | Val rows: {nrow(val_df)}")

  dtrain <- .build_xgb_matrix(train_df, target, feature_cols)
  dval   <- .build_xgb_matrix(val_df,   target, feature_cols)

  # Default hyperparameters — tuned for NFL game-level data
  default_params <- list(
    objective        = objective,
    eval_metric      = if (grepl("binary", objective)) "logloss" else "rmse",
    eta              = 0.05,
    max_depth        = 4,
    subsample        = 0.8,
    colsample_bytree = 0.7,
    min_child_weight = 10,   # conservative — prevents overfitting on ~1k rows
    lambda           = 2.0,
    alpha            = 0.5,
    seed             = 42L
  )

  # User overrides win
  final_params <- utils::modifyList(default_params, params)

  # Train with early stopping on validation
  model <- xgboost::xgb.train(
    params            = final_params,
    data              = dtrain,
    nrounds           = 500,
    watchlist         = list(train = dtrain, val = dval),
    early_stopping_rounds = 30,
    verbose           = 0,
    print_every_n     = 50
  )

  logger::log_info("Best iteration: {model$best_iteration} | Best val score: {round(model$best_score, 4)}")

  # Feature importance
  importance <- xgboost::xgb.importance(model = model) |>
    dplyr::mutate(target = target)

  list(
    model         = model,
    target        = target,
    objective     = objective,
    feature_cols  = intersect(feature_cols, names(train_df)),
    importance    = importance,
    best_iter     = model$best_iteration,
    best_val_score = model$best_score,
    trained_at    = Sys.time()
  )
}

# =============================================================================
# TRAIN ALL THREE MODELS
# =============================================================================

#' Train the full game prediction model suite
#'
#' @param cfg Config list
#' @param save_dir Directory to save model artifacts
#' @return Named list of model objects
#' @export
train_game_models <- function(cfg      = load_config(),
                               save_dir = here::here("models/game")) {
  fs::dir_create(save_dir, recurse = TRUE)
  setup_logging(cfg, context = "model_train")

  t_start <- proc.time()
  logger::log_info("========================================")
  logger::log_info("GAME MODEL TRAINING — {Sys.Date()}")
  logger::log_info("========================================")

  # Load and engineer features
  raw_df <- load_training_data(cfg)
  df     <- engineer_features(raw_df)
  splits <- split_data(df)

  logger::log_info("Train: {nrow(splits$train)} | Val: {nrow(splits$val)} | Test: {nrow(splits$test)}")

  # ---- Model 1: Spread (home margin) ----------------------------------------
  m_spread <- train_xgb_model(
    train_df  = splits$train,
    val_df    = splits$val,
    target    = "home_margin",
    objective = "reg:squarederror"
  )

  # ---- Model 2: Total (combined score) --------------------------------------
  m_total <- train_xgb_model(
    train_df  = splits$train,
    val_df    = splits$val,
    target    = "total_points",
    objective = "reg:squarederror"
  )

  # ---- Model 3: Win probability (binary) ------------------------------------
  m_winprob <- train_xgb_model(
    train_df  = splits$train,
    val_df    = splits$val,
    target    = "home_win_flag",
    objective = "binary:logistic"
  )

  models <- list(
    spread  = m_spread,
    total   = m_total,
    winprob = m_winprob
  )

  # ---- Save artifacts -------------------------------------------------------
  purrr::iwalk(models, function(m, name) {
    xgboost::xgb.save(m$model, file.path(save_dir, glue::glue("{name}_model.ubj")))
    saveRDS(m$importance, file.path(save_dir, glue::glue("{name}_importance.rds")))
    logger::log_info("Saved {name} model -> {save_dir}")
  })

  # Save metadata
  metadata <- list(
    trained_at     = as.character(Sys.time()),
    season_range   = paste(range(df$season), collapse = "-"),
    train_rows     = nrow(splits$train),
    val_rows       = nrow(splits$val),
    test_rows      = nrow(splits$test),
    feature_count  = length(.ENGINEERED_FEATURES),
    spread_val_rmse  = round(m_spread$best_val_score, 4),
    total_val_rmse   = round(m_total$best_val_score, 4),
    winprob_val_loss = round(m_winprob$best_val_score, 4)
  )
  yaml::write_yaml(metadata, file.path(save_dir, "model_metadata.yml"))
  logger::log_info("Metadata: {yaml::as.yaml(metadata)}")

  elapsed <- round((proc.time() - t_start)[["elapsed"]] / 60, 1)
  logger::log_info("TRAINING COMPLETE in {elapsed} min.")

  invisible(models)
}

# =============================================================================
# LOAD SAVED MODELS
# =============================================================================

#' Load saved game models from disk
#' @export
load_game_models <- function(save_dir = here::here("models/game")) {
  if (!fs::dir_exists(save_dir)) {
    stop("No models found at: ", save_dir, "\nRun train_game_models() first.")
  }

  list(
    spread  = list(
      model      = xgboost::xgb.load(file.path(save_dir, "spread_model.ubj")),
      importance = readRDS(file.path(save_dir, "spread_importance.rds")),
      target     = "home_margin"
    ),
    total   = list(
      model      = xgboost::xgb.load(file.path(save_dir, "total_model.ubj")),
      importance = readRDS(file.path(save_dir, "total_importance.rds")),
      target     = "total_points"
    ),
    winprob = list(
      model      = xgboost::xgb.load(file.path(save_dir, "winprob_model.ubj")),
      importance = readRDS(file.path(save_dir, "winprob_importance.rds")),
      target     = "home_win_flag"
    ),
    metadata = yaml::read_yaml(file.path(save_dir, "model_metadata.yml"))
  )
}
