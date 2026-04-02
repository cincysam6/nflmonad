# =============================================================================
# Logging Utility
# =============================================================================

#' Configure the logger for the platform
#'
#' @param cfg Config list
#' @param context Optional string appended to log file name
#' @export
setup_logging <- function(cfg, context = "pipeline") {
  log_dir <- cfg$paths$logs
  fs::dir_create(log_dir)

  date_str  <- format(Sys.time(), "%Y%m%d_%H%M%S")
  log_file  <- file.path(log_dir, glue::glue("{context}_{date_str}.log"))

  logger::log_threshold(cfg$logging$level)
  logger::log_appender(logger::appender_tee(log_file))
  logger::log_formatter(logger::formatter_glue_or_sprintf)
  logger::log_layout(logger::layout_glue_generator(
    format = "[{level}] {time} | {msg}"
  ))

  logger::log_info("Logger initialised. Log file: {log_file}")
  invisible(log_file)
}

#' Log pipeline step start/end with timing
#'
#' @param step_name Human-readable step name
#' @param expr Expression to execute
#' @return Result of expr
#' @export
log_step <- function(step_name, expr) {
  logger::log_info("START: {step_name}")
  t0 <- proc.time()
  result <- tryCatch(
    force(expr),
    error = function(e) {
      logger::log_error("FAILED: {step_name} — {conditionMessage(e)}")
      stop(e)
    }
  )
  elapsed <- round((proc.time() - t0)[["elapsed"]], 1)
  logger::log_info("DONE:  {step_name} ({elapsed}s)")
  invisible(result)
}

#' Prune log files older than max_log_files
#'
#' @param cfg Config list
prune_logs <- function(cfg) {
  log_files <- sort(fs::dir_ls(cfg$paths$logs, regexp = "\\.log$"), decreasing = TRUE)
  if (length(log_files) > cfg$logging$max_log_files) {
    old_files <- tail(log_files, -cfg$logging$max_log_files)
    fs::file_delete(old_files)
    logger::log_info("Pruned {length(old_files)} old log files.")
  }
}
