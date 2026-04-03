# =============================================================================
# Parquet I/O Utilities
# =============================================================================
# All writes go through write_parquet_partition() to ensure consistent
# metadata columns, partitioning, and idempotent overwrite behaviour.

library(arrow)
library(dplyr)
library(fs)
library(logger)
library(digest)

# ---- Metadata columns --------------------------------------------------------

#' Attach ingestion metadata columns to a data frame
#'
#' @param df Data frame
#' @param source_name String identifying the nflverse function/dataset
#' @param source_file_type e.g. "nflreadr", "csv", "parquet"
#' @param source_version nflverse data version string if available
#' @return df with added metadata columns
add_ingestion_metadata <- function(df,
                                   source_name,
                                   source_file_type = "nflreadr",
                                   source_version   = NA_character_) {
  df |>
    dplyr::mutate(
      ingestion_ts      = lubridate::now(tzone = "UTC"),
      source_name       = source_name,
      source_file_type  = source_file_type,
      source_version    = as.character(source_version),
      row_hash = purrr::pmap_chr(
        dplyr::across(dplyr::everything(), as.character),
        function(...) digest::digest(list(...), algo = "xxhash64")
      )
    )
}

# ---- Write -------------------------------------------------------------------

#' Write a data frame as partitioned Parquet
#'
#' Supports writing with Hive-style partition directories.
#' Idempotent: existing partition directories are overwritten.
#'
#' @param df Data frame to write
#' @param base_path Base directory for the table
#' @param partition_cols Character vector of column names to partition by
#'   e.g. c("season") or c("season","week"). NULL for no partitioning.
#' @param compression Parquet compression codec (default "snappy")
#' @export
write_parquet_partition <- function(df,
                                    base_path,
                                    partition_cols = NULL,
                                    compression    = "snappy") {
  fs::dir_create(base_path)
  
  if (is.null(partition_cols) || length(partition_cols) == 0) {
    out_path <- file.path(base_path, "data.parquet")
    arrow::write_parquet(df, out_path, compression = compression)
    logger::log_info("Wrote {nrow(df)} rows -> {out_path}")
    return(invisible(out_path))
  }
  
  # Split and write per partition combination
  # Force partition columns to integer to prevent Arrow reading them as DOUBLE
  for (col in partition_cols) {
    if (col %in% names(df) && is.numeric(df[[col]])) {
      df[[col]] <- as.integer(df[[col]])
    }
  }
  
  part_groups <- df |>
    dplyr::group_by(dplyr::across(dplyr::all_of(partition_cols))) |>
    dplyr::group_split()
  
  part_keys <- df |>
    dplyr::distinct(dplyr::across(dplyr::all_of(partition_cols)))
  
  purrr::walk2(part_groups, seq_len(nrow(part_keys)), function(part_df, i) {
    key_vals  <- part_keys[i, , drop = FALSE]
    dir_parts <- purrr::imap_chr(as.list(key_vals), ~ paste0(.y, "=", .x))
    out_dir   <- file.path(base_path, paste(dir_parts, collapse = "/"))
    fs::dir_create(out_dir)
    out_file  <- file.path(out_dir, "data.parquet")
    arrow::write_parquet(part_df, out_file, compression = compression)
  })
  
  logger::log_info(
    "Wrote {nrow(df)} rows across {nrow(part_keys)} partitions -> {base_path}"
  )
  invisible(base_path)
}

#' Read partitioned Parquet back into a data frame
#'
#' Uses arrow::open_dataset for lazy / efficient reading.
#'
#' @param base_path Base directory
#' @param filter_expr Optional dplyr filter expression (evaluated lazily)
#' @param hive_partition Logical
#' @return Arrow Table (call collect() to materialise)
#' @export
read_parquet_partition <- function(base_path,
                                   filter_expr   = NULL,
                                   hive_partition = TRUE) {
  if (!fs::dir_exists(base_path)) {
    stop("Parquet path does not exist: ", base_path)
  }

  ds <- arrow::open_dataset(base_path, hive_style = hive_partition)

  if (!is.null(filter_expr)) {
    ds <- dplyr::filter(ds, !!rlang::enquo(filter_expr))
  }

  ds
}

# ---- Incremental helpers -----------------------------------------------------

#' Determine which seasons need refreshing
#'
#' Returns seasons that either have no data on disk or are in the
#' incremental_seasons list from config.
#'
#' @param base_path Raw table base path
#' @param all_seasons Integer vector of all seasons to potentially load
#' @param incremental_seasons Integer vector of seasons always refreshed
#' @param force_full Logical; if TRUE return all_seasons
#' @return Integer vector of seasons to process
#' @export
seasons_to_refresh <- function(base_path,
                                all_seasons,
                                incremental_seasons,
                                force_full = FALSE) {
  if (force_full) return(all_seasons)

  existing_seasons <- character(0)
  if (fs::dir_exists(base_path)) {
    season_dirs <- fs::dir_ls(base_path, regexp = "season=\\d{4}$", type = "directory")
    existing_seasons <- stringr::str_extract(basename(season_dirs), "\\d{4}")
  }

  missing  <- setdiff(as.character(all_seasons), existing_seasons)
  refresh  <- as.character(incremental_seasons)
  unique(as.integer(c(missing, refresh)))
}
