# =============================================================================
# Test Runner — Data Validation
# =============================================================================

source(here::here("R/utils/config_loader.R"))
source(here::here("R/utils/db_connection.R"))
source(here::here("R/utils/logging.R"))

#' Run all SQL data tests against the DuckDB instance
#'
#' Each SQL block in the test file is expected to return 0 rows on success.
#' Any rows returned = test failure.
#'
#' @param cfg Config list
#' @param stop_on_failure Logical; if TRUE, stop pipeline on first failure
#' @return Invisible data frame of failures (0 rows if all pass)
#' @export
run_all_tests <- function(cfg        = load_config(),
                           stop_on_failure = FALSE) {
  con <- db_connect(cfg, read_only = TRUE)
  on.exit(db_disconnect(con), add = TRUE)

  test_sql <- readr::read_file(here::here("sql/tests/data_tests.sql"))

  # Split on blank lines (each test is a standalone SELECT)
  test_blocks <- stringr::str_split(test_sql, "\n\n")[[1]] |>
    purrr::keep(~ stringr::str_detect(.x, "SELECT"))

  results <- purrr::map_dfr(test_blocks, function(block) {
    test_name <- stringr::str_extract(block, "'([^']+)'")
    test_name <- stringr::str_remove_all(test_name, "'")

    tryCatch({
      rows <- DBI::dbGetQuery(con, block)
      if (nrow(rows) > 0) {
        logger::log_warn("TEST FAILED: {test_name} — {nrow(rows)} row(s) returned")
        if (stop_on_failure) stop("Test failed: ", test_name)
        tibble::tibble(
          test_name = test_name,
          status    = "FAIL",
          rows      = nrow(rows),
          detail    = paste(capture.output(print(rows)), collapse = "\n")
        )
      } else {
        logger::log_info("TEST PASSED: {test_name}")
        tibble::tibble(
          test_name = test_name,
          status    = "PASS",
          rows      = 0L,
          detail    = ""
        )
      }
    }, error = function(e) {
      logger::log_error("TEST ERROR: {test_name} — {e$message}")
      tibble::tibble(
        test_name = test_name,
        status    = "ERROR",
        rows      = NA_integer_,
        detail    = e$message
      )
    })
  })

  n_fail  <- sum(results$status == "FAIL")
  n_error <- sum(results$status == "ERROR")
  n_pass  <- sum(results$status == "PASS")

  logger::log_info(
    "Test summary: {n_pass} passed | {n_fail} failed | {n_error} errors"
  )

  if ((n_fail + n_error) > 0 && stop_on_failure) {
    stop("Data validation failed. Check logs.")
  }

  invisible(results)
}


# ---- testthat unit tests (run with devtools::test()) ------------------------

library(testthat)

test_that("load_config returns a list", {
  cfg <- load_config()
  expect_type(cfg, "list")
  expect_true("seasons" %in% names(cfg))
  expect_true("paths"   %in% names(cfg))
})

test_that("resolve_seasons works for numeric start", {
  cfg <- load_config()
  seasons <- resolve_seasons(2020, cfg)
  expect_true(all(seasons >= 2020))
  expect_true(all(seasons <= cfg$seasons$current_season))
})

test_that("add_ingestion_metadata adds required columns", {
  df <- data.frame(x = 1:3)
  out <- add_ingestion_metadata(df, "test_source")
  expect_true("ingestion_ts"    %in% names(out))
  expect_true("source_name"     %in% names(out))
  expect_true("row_hash"        %in% names(out))
})

test_that("seasons_to_refresh returns missing seasons", {
  withr::with_tempdir({
    out <- seasons_to_refresh(
      base_path           = "nonexistent/path",
      all_seasons         = c(2020, 2021, 2022, 2023),
      incremental_seasons = c(2023),
      force_full          = FALSE
    )
    expect_equal(sort(out), c(2020, 2021, 2022, 2023))
  })
})
