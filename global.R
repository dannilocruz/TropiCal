options(shiny.maxRequestSize = 200 * 1024^2)

library(shiny)
library(DT)
library(DBI)
library(RSQLite)
library(blob)
library(dplyr)
library(purrr)
library(tibble)
library(RProtoBuf)
library(jsonlite)
library(writexl)
library(later)
library(compiler)
library(parallel)
library(pbapply)
library(xrftools)
source("xrf_functions.R")

Sys.setenv(OMP_NUM_THREADS="1", OPENBLAS_NUM_THREADS="1", MKL_NUM_THREADS="1", BLIS_NUM_THREADS="1")
get_available_cores <- function() {
  # Try cgroup v2: /sys/fs/cgroup/cpu.max = "<quota> <period>" or "max <period>"
  p <- "/sys/fs/cgroup/cpu.max"
  if (file.exists(p)) {
    line <- tryCatch(readLines(p, warn = FALSE)[1], error = function(e) "")
    parts <- strsplit(trimws(line), "\\s+")[[1]]
    if (length(parts) >= 2 && parts[1] != "max") {
      quota  <- suppressWarnings(as.numeric(parts[1]))
      period <- suppressWarnings(as.numeric(parts[2]))
      if (is.finite(quota) && is.finite(period) && period > 0) {
        n <- floor(quota / period)
        if (n >= 1) return(n)
      }
    }
  }
  
  # Fallback to detectCores (may overreport or be NA)
  n <- suppressWarnings(tryCatch(parallel::detectCores(logical = FALSE), error = function(e) NA_integer_))
  if (!is.finite(n) || n < 1) n <- 1L
  n
}

cores <- get_available_cores()

build_ncd_wide <- function(db_file) {
  con <- DBI::dbConnect(RSQLite::SQLite(), db_file)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  
  df_result <- DBI::dbGetQuery(con, "
    SELECT reading_number, element, result_ppm, error, countrate
    FROM reading_result
  ")
  if (!nrow(df_result)) stop("No rows in 'reading_result'.", call. = FALSE)
  
  df_basic <- DBI::dbGetQuery(con, "
    SELECT reading_number, units, sigma
    FROM basic_reading
  ") %>% dplyr::distinct(reading_number, .keep_all = TRUE)
  
  df_sample <- DBI::dbGetQuery(con, "
    SELECT reading_number, data AS sample_name
    FROM data_entry
    WHERE header = 'Sample'
  ") %>% dplyr::distinct(reading_number, .keep_all = TRUE)
  
  df_joined <- df_result %>%
    dplyr::left_join(df_basic,  by = "reading_number") %>%
    dplyr::left_join(df_sample, by = "reading_number") %>%
    dplyr::mutate(
      sample_name = ifelse(is.na(sample_name),
                           paste0("unknown_", reading_number),
                           sample_name),
      element     = as.character(element),
      result_ppm  = suppressWarnings(as.numeric(result_ppm)),
      error       = suppressWarnings(as.numeric(error)),
      countrate   = suppressWarnings(as.numeric(countrate)),
      units       = as.character(units),
      sigma       = suppressWarnings(as.numeric(sigma))
    )
  
  df_wide <- df_joined %>%
    tidyr::pivot_wider(
      id_cols     = c(sample_name, reading_number, units, sigma),
      names_from  = element,
      values_from = c(result_ppm, error, countrate),
      values_fn   = dplyr::first,
      names_glue  = "{element} {.value}"
    ) %>%
    dplyr::arrange(sample_name, reading_number)
  
  nm <- names(df_wide)
  nm <- sub(" result_ppm$", "", nm, perl = TRUE)
  names(df_wide) <- nm
  names(df_wide)[names(df_wide) == "sample_name"]    <- "Sample name"
  names(df_wide)[names(df_wide) == "reading_number"] <- "Reading number"
  names(df_wide)[names(df_wide) == "units"]          <- "Unit"
  names(df_wide)[names(df_wide) == "sigma"]          <- "Sigma"
  
  elements <- sort(unique(df_result$element))
  id_order <- c("Sample name","Reading number","Unit","Sigma")
  triplets <- unlist(lapply(elements, function(el)
    c(el, paste0(el, " error"), paste0(el, " countrate"))))
  final_order <- c(id_order, intersect(triplets, names(df_wide)))
  df_wide <- df_wide[, final_order, drop = FALSE]
  
  df_wide <- df_wide[order(as.integer(df_wide$`Reading number`)), , drop = FALSE]
  rownames(df_wide) <- NULL
  df_wide
}

geochem_table <- function(df,
                          total_iron = c("Fe2O3", "FeO"),
                          round_digits = 2,
                          zero_is_na = TRUE,
                          negatives_are_na = TRUE,
                          drop_all_na_cols = TRUE,
                          average_sequential_duplicates = FALSE) {
  total_iron <- match.arg(total_iron)
  
  req_cols <- c("Sample name", "Reading number", "Unit")
  stopifnot(all(req_cols %in% names(df)))
  
  majors_all <- c("Si","Ti","Al","Fe","Mn","Mg","Ca","K","P")
  traces_all <- c("Ag","As","Au","Bi","Cd","Co","Cr","Cu","Hf","Hg","Mo","Nb",
                  "Ni","Pb","Pd","Rb","Re","Sb","Se","Sn","Sr","Ta","Th","U",
                  "V","W","Y","Zn","Zr","Ba","Cl","S")
  majors <- intersect(majors_all, names(df))
  traces <- intersect(traces_all, names(df))
  
  fe_factor <- if (total_iron == "Fe2O3") 1.4297 else 1.2865
  factors <- c(Si = 2.1392, Ti = 1.6681, Al = 1.8895, Fe = fe_factor,
               Mn = 1.2912, Mg = 1.6582, Ca = 1.3992, K = 1.2046, P = 2.2916)
  oxide_names <- c(Si = "SiO2", Ti = "TiO2", Al = "Al2O3", Fe = total_iron,
                   Mn = "MnO", Mg = "MgO", Ca = "CaO", K = "K2O", P = "P2O5")
  
  clean_vec <- function(x) {
    x <- suppressWarnings(as.numeric(x))
    if (negatives_are_na) x[x < 0]  <- NA_real_
    if (zero_is_na)       x[x == 0] <- NA_real_
    x
  }
  df[majors] <- lapply(df[majors], clean_vec)
  df[traces] <- lapply(df[traces], clean_vec)
  
  maj_out_list <- lapply(majors, function(el) {
    val <- df[[el]]
    fac <- factors[[el]]
    ifelse(df$Unit == "%",  val * fac,
           ifelse(df$Unit == "ppm", val * fac / 10000, NA_real_))
  })
  names(maj_out_list) <- unname(oxide_names[majors])
  
  trace_out_list <- lapply(traces, function(el) {
    val <- df[[el]]
    ifelse(df$Unit == "%",  val * 10000,
           ifelse(df$Unit == "ppm", val, NA_real_))
  })
  names(trace_out_list) <- traces
  
  out <- dplyr::bind_cols(
    tibble(Sample = df[["Sample name"]],
           `Reading number` = df[["Reading number"]]),
    tibble::as_tibble(maj_out_list),
    tibble::as_tibble(trace_out_list)
  )
  
  if (drop_all_na_cols) {
    out <- dplyr::select(out, where(~ !all(is.na(.x))))
  }
  
  if (isTRUE(average_sequential_duplicates)) {
    run_id <- with(out, cumsum(c(TRUE, Sample[-1] != head(Sample, -1))))
    out <- out %>% mutate(.run_id = run_id)
    num_cols <- setdiff(names(out)[vapply(out, is.numeric, TRUE)],
                        c("Reading number", ".run_id"))
    out <- out %>%
      group_by(Sample, .run_id) %>%
      summarise(across(all_of(num_cols), ~ mean(.x, na.rm = TRUE)), .groups = "drop") %>%
      select(-.run_id)
  }
  
  num_cols_final <- names(out)[vapply(out, is.numeric, TRUE)]
  num_cols_final <- setdiff(num_cols_final, "Reading number")
  if (length(num_cols_final)) {
    out[num_cols_final] <- lapply(out[num_cols_final], round, digits = round_digits)
  }
  
  out
}

sanitize_filename <- function(x) gsub("[^A-Za-z0-9._-]", "_", x)

get_sample_names <- function(con) {
  df <- DBI::dbGetQuery(con, "
    SELECT reading_number, data
    FROM data_entry
    WHERE header = 'Sample'
  ")
  stats::setNames(df$data, as.character(df$reading_number))
}

spectra_number_to_string <- function(n) {
  n_int <- suppressWarnings(as.integer(n)); if (is.na(n_int)) n_int <- 0L
  mapping <- c("1" = "main", "2" = "low", "3" = "high", "4" = "light")
  out <- unname(mapping[as.character(n_int)])
  if (is.na(out)) paste0("unknown_", n_int) else out
}

load_blobs <- function(db_file, sanitize_names = TRUE, zero_pad = TRUE) {
  con <- DBI::dbConnect(RSQLite::SQLite(), db_file)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  
  samples_map <- get_sample_names(con)
  
  rows <- DBI::dbGetQuery(con, "
    SELECT reading_number, spectra_number, spectra_proto
    FROM spectra_data
  ")
  if (!nrow(rows)) stop("No rows found in spectra_data.", call. = FALSE)
  
  # Sample names from map (fallback to 'unknown_<reading_number>')
  rn_to_sample <- function(rn) {
    rn_chr <- as.character(rn)
    nm <- unname(samples_map[rn_chr])
    if (is.na(nm)) paste0("unknown_", rn_chr) else nm
  }
  sample <- vapply(rows$reading_number, rn_to_sample, character(1))
  if (sanitize_names) sample <- sanitize_filename(sample)
  
  filter <- vapply(rows$spectra_number, spectra_number_to_string, character(1))
  
  # Optional: zero-pad for nicer lexical ordering (≥3 digits)
  if (zero_pad) {
    width <- max(3L, nchar(as.character(max(rows$reading_number, na.rm = TRUE))))
    reading_id <- sprintf(paste0("%0", width, "d"), as.integer(rows$reading_number))
  } else {
    reading_id <- as.character(rows$reading_number)
  }
  
  id <- paste(sample, filter, reading_id, sep = "_")
  
  # DBI returns BLOBs as raw vectors; keep as a list-column
  spectra_raw <- rows$spectra_proto
  
  # Build tibble (no stringsAsFactors necessary)
  tibble::tibble(
    id              = id,
    sample          = sample,
    filter          = filter,
    reading_number  = as.integer(rows$reading_number),
    reading_id      = reading_id,
    spectra_raw     = spectra_raw
  )
}

readProtoFiles2("key.proto")
SpectralData <- P("SpectralData")

parse_protobuf <- function(blob_raw) SpectralData$read(blob_raw)

process_spectral_data <- function(spectral_raw, num_bins) {
  n <- if (!is.null(num_bins) && !is.na(num_bins)) as.integer(num_bins) else floor(length(spectral_raw) / 4L)
  out <- readBin(spectral_raw, what = "numeric", size = 4L, n = n, endian = "little")
  round(out, 6)
}

process_properties <- function(msg) {
  props <- msg$properties
  if (is.null(props) || length(props) == 0) {
    return(tibble(Property = character(), Value = character()))
  }
  out <- list()
  add_prop <- function(var, val) {
    # try JSON if val is a single string
    if (is.character(val) && length(val) == 1) {
      parsed <- tryCatch(jsonlite::fromJSON(val), error = function(e) NULL)
      if (!is.null(parsed)) {
        if (is.list(parsed) && !is.data.frame(parsed)) {
          for (nm in names(parsed)) out[[length(out) + 1]] <<- c(paste0(var, "_", nm), as.character(parsed[[nm]]))
          return(invisible())
        } else {
          out[[length(out) + 1]] <<- c(var, paste(as.character(parsed), collapse = ", "))
          return(invisible())
        }
      }
    }
    # fallback (vector/list -> comma-join)
    if (length(val) > 1 || is.list(val)) {
      out[[length(out) + 1]] <<- c(var, paste(as.character(val), collapse = ", "))
    } else {
      out[[length(out) + 1]] <<- c(var, as.character(val))
    }
  }
  for (pa in props) {
    if (!is.null(pa$type)) {
      for (p in pa$type) add_prop(p$variable, p$value)
    } else {
      # (defensive) if directly a Property
      add_prop(pa$variable, pa$value)
    }
  }
  if (length(out) == 0) tibble(Property = character(), Value = character())
  else as_tibble(do.call(rbind, out), .name_repair = ~ c("Property", "Value"))
}

scalars_from_msg <- function(msg) {
  tibble(
    Property = c(
      "reading","filter_order","num_bins","start","stop","live_time",
      "real_time","avg_current","ev_per_bin","escale","filter_type"
    ),
    Value = list(
      as.integer(msg$reading),
      as.integer(msg$filter_order),
      as.integer(msg$num_bins),
      as.integer(msg$start),   # uint32 -> R integer
      as.numeric(msg$stop),    # float
      as.numeric(msg$live_time),
      as.numeric(msg$real_time),
      as.numeric(msg$avg_current),
      as.numeric(msg$ev_per_bin),
      as.numeric(msg$escale),
      as.character(msg$filter_type)
    )
  )
}

convert_blob <- function(blob_raw) {
  msg <- parse_protobuf(blob_raw)
  
  scalar_df <- scalars_from_msg(msg)
  
  # spectrum
  num_bins <- as.integer(msg$num_bins)
  intensities <- process_spectral_data(msg$spectral_data, num_bins)
  
  start_bin <- round(as.numeric(msg$start))
  stop_bin  <- round(as.numeric(msg$stop))
  bins_py   <- if (!is.na(start_bin) && !is.na(stop_bin) && stop_bin > start_bin) {
    seq.int(start_bin, stop_bin - 1L)          # exclusive stop (Python range)
  } else {
    seq.int(0L, length(intensities) - 1L)      # fallback
  }
  # ensure same length as intensities
  if (length(bins_py) != length(intensities)) {
    bins_py <- seq.int(0L, length(intensities) - 1L)
  }
  spectral_df <- tibble(Bin = bins_py, Intensity = intensities)
  
  properties_df <- process_properties(msg)
  
  list(
    scalar_df     = scalar_df,
    spectral_df   = spectral_df,
    properties_df = properties_df
  )
}

.flatten_scalar_value <- function(df) {
  if (is.null(df) || nrow(df) == 0) return(df)
  if (!"Value" %in% names(df)) return(df)
  df$Value <- vapply(df$Value, function(v) {
    if (is.list(v)) {
      if (length(v) == 0) "" else paste(as.character(unlist(v, use.names = FALSE)), collapse = ", ")
    } else {
      as.character(v)
    }
  }, FUN.VALUE = character(1))
  df$Property <- as.character(df$Property)
  df
}


.write_one_csv <- function(path, scalar_df, spectral_df, properties_df) {
  scalar_df <- .flatten_scalar_value(scalar_df)
  
  # (re)create file
  if (file.exists(path)) file.remove(path)
  
  # 1) scalars (no header)
  readr::write_delim(scalar_df, path, delim = ",", col_names = FALSE)
  readr::write_lines("", path, append = TRUE)
  
  # 2) spectral with header
  readr::write_lines("Bin,Intensity", path, append = TRUE)
  readr::write_delim(spectral_df, path, delim = ",", col_names = FALSE, append = TRUE)
  readr::write_lines("", path, append = TRUE)
  
  # 3) properties with header
  readr::write_lines("Property,Value", path, append = TRUE)
  readr::write_delim(properties_df, path, delim = ",", col_names = FALSE, append = TRUE)
  invisible(path)
}


write_spectra_csvs <- function(spectra_df_parsed,
                               id_col = "id",
                               output_dir = NULL,
                               use_temp = FALSE,
                               ext = ".csv",
                               enforce_unique = FALSE) {
  stopifnot(all(c("scalar_df","spectral_df","properties_df") %in% names(spectra_df_parsed)))
  if (!id_col %in% names(spectra_df_parsed)) stop(sprintf("Column '%s' not found.", id_col))
  
  ids <- as.character(spectra_df_parsed[[id_col]])
  if (enforce_unique) ids <- make.unique(ids, sep = "_")
  file_names <- paste0(ids, ext)
  
  # Choose destination directory
  dest_dir <- if (!is.null(output_dir)) {
    output_dir
  } else if (isTRUE(use_temp)) {
    tempdir()
  } else {
    stop("Provide output_dir or set use_temp = TRUE.", call. = FALSE)
  }
  dir.create(dest_dir, showWarnings = FALSE, recursive = TRUE)
  
  file_paths <- file.path(dest_dir, file_names)
  
  purrr::walk2(seq_len(nrow(spectra_df_parsed)), file_paths, function(i, path) {
    .write_one_csv(
      path = path,
      scalar_df     = spectra_df_parsed$scalar_df[[i]],
      spectral_df   = spectra_df_parsed$spectral_df[[i]],
      properties_df = spectra_df_parsed$properties_df[[i]]
    )
  })
  
  tibble::tibble(!!id_col := ids, file_path = file_paths)
}

.load_cal <- function(path) {
  if (!file.exists(path)) stop(sprintf("Calibration file not found:\n  %s", path), call. = FALSE)
  calRDS(path)
}

make_infile <- function(paths) {
  paths <- as.character(paths)
  tibble::tibble(
    name     = basename(paths),
    size     = unname(file.info(paths)$size),
    type     = NA_character_,
    datapath = normalizePath(paths)
  )
}

process_filter_eqm <- function(inFile, cal, filter_name,
                               cores = 1, rounding = 4, multiplier = 1,
                               add_filter_col = TRUE) {
  
  # 1) Load & clean
  spectra <- readXL5Data(inFile = inFile)
  spectra$CPS     <- as.numeric(spectra$CPS)
  spectra$Energy  <- as.numeric(spectra$Energy)
  spectra         <- spectra[stats::complete.cases(spectra), ]
  spectra$Spectrum <- gsub("\\.(csv|CSV)$", "", spectra$Spectrum)
  
  # 2) Deconvolution
  deconvoluted <- spectra_gls_deconvolute(spectra, cores = cores)
  
  # 3) Calibration bits
  calValHold          <- cal[["calList"]]
  calVariables        <- cal$Intensities[, !colnames(cal$Intensities) %in% "Spectrum", drop = FALSE]
  calValElements      <- order_elements(names(calValHold))
  calVariableElements <- order_elements(names(calVariables))
  calDefinitions      <- cal$Definitions
  
  # 4) Spectra summaries & count frames
  spectra_stuff       <- totalCountsGen(spectra)
  other_spectra_stuff <- merge(spectra_stuff, deconvoluted$Areas[, c("Spectrum", "Baseline")],
                               by = "Spectrum", all = TRUE, sort = TRUE)
  
  val.line.table      <- narrowLineTable(spectra = spectra, definition.table = calDefinitions,
                                         elements = calVariableElements)
  fullInputValCounts  <- merge(val.line.table, other_spectra_stuff, by = "Spectrum", all = TRUE, sort = TRUE)
  
  val.line.table2     <- wideLineTable(spectra = spectra, definition.table = calDefinitions,
                                       elements = calVariableElements)
  fullInputValCountsWide <- merge(val.line.table2, other_spectra_stuff, by = "Spectrum", all = TRUE, sort = TRUE)
  
  counts <- fullInputValCounts[, !colnames(fullInputValCounts) %in%
                                 c(names(other_spectra_stuff[, -1, drop = FALSE]), "Total"), drop = FALSE]
  val.line.table3 <- deconvolutionIntensityFrame(deconvoluted$Areas, counts)
  fullInputValCountsDeconvoluted <- merge(val.line.table3, other_spectra_stuff, by = "Spectrum", all = TRUE, sort = TRUE)
  
  countList <- list(
    Narrow = fullInputValCounts,
    Wide   = fullInputValCountsWide,
    Area   = fullInputValCountsDeconvoluted
  )
  
  # 5) Predictions (needed inputs for EQM)
  preds <- cloudCalPredict(
    Calibration          = cal,
    count.list           = countList,
    elements.cal         = calValElements,
    variables            = calVariableElements,
    valdata              = spectra,
    deconvoluted_valdata = deconvoluted,
    rounding             = rounding,
    multiplier           = multiplier
  )
  
  # 6) EQM — return as data.frame (optionally with Filter column)
  eqm <- cloudCalPredictErrorEQM(
    Calibration          = cal,
    predictions          = preds,
    count.list           = countList,
    elements.cal         = calValElements,
    variables            = calVariableElements,
    valdata              = spectra,
    deconvoluted_valdata = deconvoluted,
    rounding             = rounding,
    multiplier           = multiplier
  )
  
  eqm_df <- as.data.frame(eqm)
  if (add_filter_col) {
    eqm_df <- tibble::as_tibble(eqm_df) %>% dplyr::mutate(Filter = filter_name, .before = 1)
  }
  eqm_df
}


run_eqm_by_filter <- function(main_df, low_df, high_df, light_df,
                              main_cal, low_cal, high_cal, light_cal,
                              cores = 1, rounding = 4, multiplier = 1,
                              add_filter_col = TRUE) {
  
  inputs <- list(
    main  = list(df = main_df,  cal = main_cal),
    low   = list(df = low_df,   cal = low_cal),
    high  = list(df = high_df,  cal = high_cal),
    light = list(df = light_df, cal = light_cal)
  )
  
  imap(inputs, function(x, nm) {
    df_ok  <- !is.null(x$df)  && length(x$df)  > 0
    cal_ok <- !is.null(x$cal)
    if (!df_ok || !cal_ok) return(NULL)
    
    process_filter_eqm(
      inFile         = x$df,
      cal            = x$cal,
      filter_name    = nm,
      cores          = cores,
      rounding       = rounding,
      multiplier     = multiplier,
      add_filter_col = add_filter_col
    )
  }) %>% purrr::compact() 
}

strip_filter_token <- function(df) {
  stopifnot(!is.null(df), "Spectrum" %in% names(df))
  df$Spectrum <- as.character(df$Spectrum)
  df$Spectrum <- gsub("_(light|low|high|main)_", "_", df$Spectrum, ignore.case = TRUE)
  df
}

drop_filter_col <- function(df) {
  if (!is.null(df) && "Filter" %in% names(df)) df$Filter <- NULL
  df
}

rename_dot_spectral <- function(df, keep = c("Spectrum")) {
  new_names <- vapply(names(df), function(col) {
    if (col %in% keep) return(col)
    had_err <- grepl("\\s+Error$", col)
    core    <- sub("\\s+Error$", "", col)
    elem    <- sub("^([A-Z][a-z]?)\\..*$", "\\1", core)
    if (identical(elem, core)) return(col)
    paste0(elem, if (had_err) " Error" else "")
  }, character(1))
  names(df) <- new_names
  df
}

MAJOR_OXIDES <- c(Si="SiO2",Ti="TiO2",Al="Al2O3",Fe="Fe2O3",Mn="MnO",
                  Mg="MgO",Ca="CaO",K="K2O",P="P2O5")

rename_to_major_oxides <- function(df, keep=c("Spectrum"), oxide_map=MAJOR_OXIDES) {
  new_names <- vapply(names(df), function(col) {
    if (col %in% keep) return(col)
    had_err <- grepl("\\s+Error$", col); core <- sub("\\s+Error$", "", col)
    elem <- if (grepl("^([A-Z][a-z]?)\\..*$", core)) sub("^([A-Z][a-z]?)\\..*$", "\\1", core)
    else if (grepl("^[A-Z][a-z]?$", core)) core else NULL
    if (!is.null(elem)) {
      base <- if (elem %in% names(oxide_map)) oxide_map[[elem]] else elem
      return(paste0(base, if (had_err) " Error" else ""))
    }
    col
  }, character(1))
  names(df) <- new_names
  df
}

reorder_with_oxides_first <- function(df,
                                      oxide_order=c("SiO2","TiO2","Al2O3","Fe2O3","MnO","MgO","CaO","K2O","P2O5"),
                                      key="Spectrum") {
  nms <- names(df)
  base_of <- function(x) sub(" Error$", "", x, perl=TRUE)
  bases <- unique(base_of(setdiff(nms, key)))
  out <- character(0)
  if (key %in% nms) out <- c(out, key)
  add_pair <- function(base){c(base[base %in% nms], paste0(base," Error")[paste0(base," Error") %in% nms])}
  present_oxides <- intersect(oxide_order, bases)
  for (ox in present_oxides) out <- c(out, add_pair(ox))
  remaining_bases <- sort(setdiff(bases, c(present_oxides, key)))
  for (b in remaining_bases) out <- c(out, add_pair(b))
  leftover <- setdiff(nms, out)
  out <- intersect(unique(c(out, leftover)), nms)
  df[, out, drop=FALSE]
}

order_by_last_underscore_number <- function(df, key="Spectrum",
                                            decreasing=FALSE, na.last=TRUE) {
  stopifnot(key %in% names(df))
  sp <- as.character(df[[key]])
  idx <- sub("^.*_(\\d+)$", "\\1", sp)
  num <- suppressWarnings(as.integer(idx))
  df[order(num, decreasing=decreasing, na.last=na.last), , drop=FALSE]
}

set_negatives_to_na_pairs <- function(df) {
  nms <- names(df); is_num <- vapply(df, is.numeric, logical(1))
  val_cols <- nms[is_num & !grepl("\\s+Error$", nms)]
  for (vc in val_cols) {
    v <- df[[vc]]; mask <- !is.na(v) & v < 0
    if (any(mask)) {
      df[[vc]][mask] <- NA_real_
      erc <- paste0(vc, " Error")
      if (erc %in% nms && is.numeric(df[[erc]])) df[[erc]][mask] <- NA_real_
    }
  }
  df
}

average_consecutive_duplicates <- function(df, key = "Spectrum") {
  stopifnot(key %in% names(df))
  sp <- as.character(df[[key]])
  m <- regexec("^(.*)_(\\d+)$", sp)
  parts <- regmatches(sp, m)
  ok <- lengths(parts) == 3
  df_ok   <- df[ok, , drop = FALSE]
  df_rest <- df[!ok, , drop = FALSE]
  if (!nrow(df_ok)) return(df)
  
  base <- vapply(parts[ok], function(p) p[2], character(1))
  idx  <- suppressWarnings(as.integer(vapply(parts[ok], function(p) p[3], character(1))))
  df_ok$.__base__ <- base
  df_ok$.__idx__  <- idx
  
  df_ok <- df_ok[order(df_ok$.__base__, df_ok$.__idx__), , drop = FALSE]
  split_runs <- function(v) if (!length(v)) list(integer(0)) else split(v, cumsum(c(1, diff(v) != 1)))
  
  by_base <- split(df_ok, df_ok$.__base__)
  num_cols <- names(df_ok)[vapply(df_ok, is.numeric, logical(1))]
  num_cols <- setdiff(num_cols, c(".__idx__", ".__base__"))
  
  averaged <- lapply(by_base, function(dfb) {
    runs <- split_runs(dfb$.__idx__)
    do.call(rbind, lapply(runs, function(runv) {
      block <- dfb[dfb$.__idx__ %in% runv, , drop = FALSE]
      if (nrow(block) >= 2) {
        out <- block[1, , drop = FALSE]
        if (length(num_cols)) {
          out[, num_cols] <- lapply(block[, num_cols, drop = FALSE],
                                    function(x) suppressWarnings(mean(as.numeric(x), na.rm = TRUE)))
        }
        out[[key]] <- block$.__base__[1]
        out
      } else block
    }))
  })
  
  res_ok <- do.call(rbind, averaged)
  res_ok$.__base__ <- NULL; res_ok$.__idx__ <- NULL
  res <- rbind(res_ok, df_rest); rownames(res) <- NULL; res
}

maybe_filter_errors <- function(df, include_errors = TRUE) {
  if (include_errors) return(df)
  keep <- !grepl("\\s+Error$", names(df))
  keep[names(df) == "Spectrum"] <- TRUE
  df[, keep, drop = FALSE]
}

round_numeric_cols <- function(df, digits = 2L) {
  num_idx <- vapply(df, is.numeric, logical(1))
  if (any(num_idx)) df[num_idx] <- lapply(df[num_idx], round, digits = as.integer(digits))
  df
}

prepare_spectra_tbl <- function(db_path) {
  sdf <- load_blobs(db_path)
  
  sdf_parsed <- sdf %>%
    dplyr::mutate(
      parsed        = purrr::map(spectra_raw, convert_blob),
      scalar_df     = purrr::map(parsed, "scalar_df"),
      spectral_df   = purrr::map(parsed, "spectral_df"),
      properties_df = purrr::map(parsed, "properties_df")
    ) %>%
    dplyr::select(-parsed)
  
  write_spectra_csvs(
    spectra_df_parsed = sdf_parsed,
    id_col            = "id",
    use_temp          = TRUE,
    ext               = ".csv",
    enforce_unique    = TRUE
  )
}