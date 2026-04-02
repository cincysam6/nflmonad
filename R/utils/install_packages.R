# =============================================================================
# NFL Analytics Platform — Package Setup
# Run once to install all dependencies
# =============================================================================

# ---- Core dependencies -------------------------------------------------------
packages <- c(
  # nflverse data access
  "nflreadr",
  "nflfastR",
  "nflplotR",

  # Data manipulation
  "dplyr",
  "tidyr",
  "purrr",
  "stringr",
  "lubridate",
  "forcats",
  "rlang",
  "glue",

  # Storage / query layer
  "duckdb",
  "DBI",
  "arrow",

  # Config / logging
  "yaml",
  "logger",
  "cli",
  "fs",

  # Parallelism / performance
  "furrr",
  "future",
  "progressr",

  # Testing
  "testthat",
  "checkmate",

  # Utilities
  "digest",       # record hashing
  "jsonlite",
  "withr"
)

missing_pkgs <- packages[!packages %in% rownames(installed.packages())]

if (length(missing_pkgs) > 0) {
  message("Installing missing packages: ", paste(missing_pkgs, collapse = ", "))
  install.packages(missing_pkgs, repos = "https://cloud.r-project.org")
}

# ---- Optional: renv snapshot -------------------------------------------------
# renv::init()
# renv::snapshot()

message("All packages available.")
