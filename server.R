shinyServer(function(input, output, session) {
  
  `%||%` <- function(a, b) if (is.null(a)) b else a
  HRule <- function() tags$hr(style = "margin: 20px 0; border: 0; border-top: 0.5px solid #909090;")
  

  db_file <- reactive({
    req(input$db_file)
    input$db_file$datapath
  })
  
  base_upload_name <- reactive({
    if (is.null(input$db_file)) "export" else tools::file_path_sans_ext(basename(input$db_file$name))
  })
  
  safe_slug <- function(x) gsub("[^A-Za-z0-9._-]+", "_", x)
  
  
  export_name <- function(suffix = "", ext = NULL) {
    base <- safe_slug(base_upload_name())
    if (!is.null(ext)) paste0(base, suffix, ".", ext) else paste0(base, suffix)
  }
  
  start_empirical_prep <- function() {
    if (empirical_initialized() || is.null(input$db_file)) return(invisible(NULL))
    empirical_initialized(TRUE)
    spectra_ready(FALSE)
    spectra_paths(NULL)
    raw_status("Processing raw spectra…")
    
    ok <- FALSE
    tryCatch({
      withProgress(message = "Preparing raw spectra…", value = 0, {
        incProgress(0.3, detail = "Reading database")
        db_path <- isolate(input$db_file$datapath)
        
        incProgress(0.6, detail = "Decoding blobs")
        tbl <- prepare_spectra_tbl(db_path)  # writes per-spectrum CSVs to temp
        
        incProgress(0.1, detail = "Finalizing")
        spectra_paths(tbl)
        spectra_ready(TRUE)
        ok <- TRUE
      })
    }, error = function(e) {
      showNotification(paste("Raw spectra prep failed:", conditionMessage(e)),
                       type = "error", duration = 8)
    })
    
    if (ok) {
      raw_status("Raw spectra ready.")
    } else {
      raw_status("Processing failed. Check your file and try again.")
      empirical_initialized(FALSE)  # allow retry
    }
    invisible(NULL)
  }
  

  wide_df <- reactive({
    req(db_file())
    build_ncd_wide(db_file())
  })
  
  values_only <- reactive({
    df <- wide_df()
    drop_idx <- grepl(" (error|countrate)$", names(df))
    dplyr::select(df, -which(drop_idx))
  })
  

  geo_tbl <- reactive({
    geochem_table(
      df = values_only(),
      total_iron = input$iron_builtin %||% "Fe2O3",
      round_digits = input$digits_builtin %||% 2,
      drop_all_na_cols = isTRUE(input$dropna_builtin %||% TRUE),
      average_sequential_duplicates = isTRUE(input$avg_builtin %||% TRUE)
    )
  })
  


  empirical_eqm_list <- reactiveVal(NULL)
  empirical_eqm_tbl  <- reactiveVal(NULL)
  spectra_ready  <- reactiveVal(FALSE)  
  spectra_paths  <- reactiveVal(NULL) 
  empirical_initialized <- reactiveVal(FALSE)
  raw_status <- reactiveVal(NULL)

  empirical_tbl <- reactive({
    values_only()
  })
  
  
  empirical_merged <- reactive({
    lst <- empirical_eqm_list()
    req(!is.null(lst))
    
    take  <- function(name) if (!is.null(lst[[name]])) lst[[name]] else NULL
    main  <- take("main")
    low   <- take("low")
    high  <- take("high")
    light <- take("light")
    
    clean <- function(x) if (is.null(x)) NULL else x |> strip_filter_token() |> drop_filter_col()
    main  <- clean(main); low <- clean(low); high <- clean(high); light <- clean(light)
    
    dfs <- Filter(Negate(is.null), list(main=main, light=light, low=low, high=high))
    if (length(dfs) == 0) return(NULL) 
    
    merged <- if (length(dfs) == 1) {
      dfs[[1]]
    } else {
      Reduce(function(x, y) merge(x, y, by = "Spectrum"), dfs)
    }
    
    merged |>
      rename_dot_spectral() |>
      rename_to_major_oxides() |>
      reorder_with_oxides_first() |>
      order_by_last_underscore_number() |>
      set_negatives_to_na_pairs()
  })
  

  empirical_display <- reactive({
    req(empirical_merged()) 
    df <- empirical_merged()
    

    avg_flag <- input$emp_avg_dups
    if (is.null(avg_flag)) avg_flag <- TRUE
    if (isTRUE(avg_flag)) {
      df <- average_consecutive_duplicates(df, key = "Spectrum")
    }
    

    df <- maybe_filter_errors(df, include_errors = FALSE)
    

    digits <- input$emp_digits
    if (is.null(digits)) digits <- 2L
    df <- round_numeric_cols(df, digits = digits)
    
    df
  })
  
  is_uf     <- reactive({ identical(input$calib_mode, "uf") })
  is_custom <- reactive({ identical(input$calib_mode, "custom") })


  cal_file_any_paths <- reactive({
    if (!is_custom()) return(NULL)  # <- radio, not old checkboxes
    
    paths <- list()
    if (!is.null(input$file_light)) paths$light <- input$file_light$datapath
    if (!is.null(input$file_main))  paths$main  <- input$file_main$datapath
    if (!is.null(input$file_low))   paths$low   <- input$file_low$datapath
    if (!is.null(input$file_high))  paths$high  <- input$file_high$datapath
    
    if (!length(paths)) return(NULL)
    
    paths <- lapply(paths, function(p) {
      if (is.null(p)) return(NULL)
      p <- tryCatch(normalizePath(p, mustWork = FALSE), error = function(e) p)
      if (!file.exists(p)) return(NULL)
      if (isTRUE(file.info(p)$size == 0)) return(NULL)
      p
    })
    paths <- paths[!vapply(paths, is.null, logical(1))]
    if (!length(paths)) return(NULL)
    
    paths
  })
  
  
  
  
  render_dt_with_buttons <- function(data_fun, filename_suffix) {
    DT::datatable(
      data_fun(),
      extensions = "Buttons",
      options = list(
        dom = "Bfrtipl",  # Buttons top; info/pager/length bottom (no CSS)
        buttons = list(
          list(extend = "copy",  title = NULL,
               exportOptions = list(modifier = list(page = "all", search = "none", order = "current"))),
          list(extend = "csv",   title = NULL, filename = export_name(filename_suffix),
               exportOptions = list(modifier = list(page = "all", search = "none", order = "current"),
                                    columns = ":visible")),
          list(extend = "excel", title = NULL, filename = export_name(filename_suffix),
               exportOptions = list(modifier = list(page = "all", search = "none", order = "current"),
                                    columns = ":visible"))
        ),
        pageLength = 10,
        lengthMenu = list(c(10, 50, 100, -1), c("10", "50", "100", "All")),
        scrollX = TRUE
      ),
      rownames = FALSE
    )
  }
  
  output$tab_controls <- renderUI({
    req(input$main_tabs)
    
    # has the user uploaded a DB yet?
    has_db <- !is.null(input$db_file)
    # have we already produced EQM results?
    has_eqm <- !is.null(empirical_eqm_list())
    
    if (input$main_tabs == "Built-in Calibration") {
      
      if (!has_db) return(NULL) 

      # After upload: show built-in controls
      tagList(
        radioButtons(
          "iron_builtin", "Total iron",
          choiceNames  = list(HTML("Fe<sub>2</sub>O<sub>3</sub>"), HTML("FeO")),
          choiceValues = c("Fe2O3", "FeO"),
          selected     = "Fe2O3",
          inline       = TRUE
        ),
        numericInput("digits_builtin", "Rounding digits", value = 2, min = 0, max = 10, step = 1),
        checkboxInput("avg_builtin",    "Average sequential duplicates", TRUE),
        checkboxInput("dropna_builtin", "Drop all-NA columns", TRUE)
      )
      
    } else {

      if (!has_db) return(NULL)
      

      base_top <- tagList(
        p(HTML("<strong>Save raw spectra with metadata?</strong><br>
         Download it if you plan to use CloudCal-XL5 to perform your own calibration.")),
        uiOutput("raw_dl_block")  # renders the button + status
      )
      

      cal_and_run <- if (isTRUE(spectra_ready())) {
        tagList(
          HRule(),
          
          radioButtons(
            "calib_mode", NULL,
            choiceNames = list(
              HTML("<b>Default</b><br><small>Use the latest calibration performed at UFL. (Only select this if your data is from UFL's analyzer.)</small>"),
              HTML("<b>Custom file(s)</b><br><small>Use your own .quant files from CloudCal-XL5.</small>")
            ),
            choiceValues = c("uf", "custom"),
            selected = isolate(input$calib_mode) %||% "uf",   # <- key change
            inline = TRUE
          ),
          
          uiOutput("calib_status", inline = TRUE),
          
          conditionalPanel(
            condition = "input.calib_mode == 'custom'",
            tags$div(
              style = "margin-top: 8px;",
              fileInput("file_main",  "Main filter (.quant)",  accept = ".quant"),
              fileInput("file_low",   "Low filter (.quant)",   accept = ".quant"),
              fileInput("file_high",  "High filter (.quant)",  accept = ".quant"),
              fileInput("file_light", "Light filter (.quant)", accept = ".quant")
            )
          ),
          
          HRule(),
          actionButton("run_emp", "Run quantification", class = "btn-primary btn-wide")
          
        )
      } else {
        NULL
      }
      

      post_run <- if (has_eqm) {
        tagList(
          HRule(),
          checkboxInput("emp_avg_dups", "Average consecutive duplicates", value = TRUE),
          numericInput("emp_digits", "Rounding digits", value = 2, min = 0, max = 10, step = 1)
        )
      } else {
        NULL
      }
      
      tagList(base_top, cal_and_run, post_run)
    }
    
  })
  
  
  observeEvent(input$run_emp, {
    req(input$db_file)
    
    if (!isTRUE(spectra_ready())) {
      showNotification("Raw spectra are not ready yet.", type = "warning", duration = 4)
      return(invisible(NULL))
    }
    
    paths_tbl <- spectra_paths()
    if (is.null(paths_tbl) || nrow(paths_tbl) == 0) {
      showNotification("No prepared spectra files were found.", type = "error", duration = 6)
      return(invisible(NULL))
    }
    
    get_infile <- function(pattern) {
      files <- paths_tbl %>%
        dplyr::filter(grepl(pattern, id)) %>%
        dplyr::pull(file_path)
      if (length(files)) make_infile(files) else NULL
    }
    
    main_df  <- get_infile("_main_")
    low_df   <- get_infile("_low_")
    high_df  <- get_infile("_high_")
    light_df <- get_infile("_light_")
    

    get_cals <- function() {
      # Prefer custom files (any subset is OK) if user chose Custom
      if (is_custom()) {
        custom <- cal_file_any_paths()
        if (!is.null(custom) && length(custom) > 0) {
          filters <- c("main","low","high","light")
          out <- setNames(vector("list", length(filters)), filters)
          for (f in filters) {
            p <- custom[[f]]
            if (!is.null(p) && nzchar(p) && file.exists(p)) {
              out[[f]] <- .load_cal(p)
            }
          }
          if (any(!vapply(out, is.null, TRUE))) return(out)
          showNotification("Custom calibration file(s) could not be loaded.", type = "error", duration = 8)
          return(NULL)
        }
        # If user chose Custom but we have zero usable files
        showNotification("No usable custom calibration files detected.", type = "warning", duration = 6)
        return(NULL)
      }
      
      # UF default (any subset is OK)
      paths <- list(
        main  = file.path("calibration", "mainCalibration6.quant"),
        low   = file.path("calibration", "lowCalibration5.quant"),
        high  = file.path("calibration", "highCalibration5.quant"),
        light = file.path("calibration", "lightCalibration7.quant")
      )
      out <- lapply(paths, function(p) if (file.exists(p)) .load_cal(p) else NULL)
      if (any(!vapply(out, is.null, TRUE))) return(out)
      
      showNotification("No UF calibration files found in ./calibration/.", type = "error", duration = 8)
      NULL
    }
    
    
    cals_now <- get_cals()
    
    if (is.null(cals_now)) return(invisible(NULL)) 
    

    withProgress(message = "Running empirical calibration…", value = 0, {
      inputs  <- list(main = main_df, low = low_df, high = high_df, light = light_df)
      calobjs <- list(main = cals_now$main, low = cals_now$low, high = cals_now$high, light = cals_now$light)
      
      valid <- vapply(names(inputs), function(nm) {
        df_ok  <- !is.null(inputs[[nm]]) && NROW(inputs[[nm]]) > 0
        cal_ok <- !is.null(calobjs[[nm]])
        df_ok && cal_ok
      }, logical(1))
      
      n_steps <- max(1L, sum(valid))
      i <- 0L
      
      res_list <- purrr::imap(inputs, function(df_in, nm) {
        df_ok  <- !is.null(df_in) && NROW(df_in) > 0
        cal_ok <- !is.null(calobjs[[nm]])
        if (!df_ok || !cal_ok) return(NULL)
        
        i <<- i + 1L
        incProgress(1 / n_steps, detail = paste("Filter:", nm))
        
        process_filter_eqm(
          inFile         = df_in,
          cal            = calobjs[[nm]],
          filter_name    = nm,
          cores          = cores,
          rounding       = 4,
          multiplier     = 1,
          add_filter_col = TRUE
        )
      })
      
      res_list <- purrr::compact(res_list)
      empirical_eqm_list(res_list)
      
      combined <- dplyr::bind_rows(res_list, .id = "Filter_list_name") %>%
        dplyr::select(-Filter_list_name)
      empirical_eqm_tbl(combined)
    })
    
    showNotification("Empirical calibration completed.", type = "message")
  })
 

  
  observeEvent(empirical_eqm_list(), {
    if (!is.null(empirical_eqm_list())) {
      updateCheckboxInput(session, "emp_avg_dups", value = TRUE)
      updateNumericInput(session, "emp_digits", value = 2)
    }
  })
  
  
  observeEvent(input$db_file, {
    empirical_eqm_list(NULL)
    empirical_eqm_tbl(NULL)
  })
  
  observeEvent(input$db_file, {
    spectra_ready(FALSE)
    spectra_paths(NULL)
    empirical_initialized(FALSE)
    raw_status(NULL)
    
  
    if (identical(input$main_tabs, "Empirical Calibration")) start_empirical_prep()
  }, ignoreInit = TRUE)
  

  observeEvent(input$main_tabs, {
    if (identical(input$main_tabs, "Empirical Calibration")) start_empirical_prep()
  }, ignoreInit = TRUE)
  
  output$calib_status <- renderUI({
    req(input$main_tabs == "Empirical Calibration")
    if (identical(input$calib_mode, "custom")) {
      paths <- cal_file_any_paths()
      if (is.null(paths) || !length(paths)) {
        tags$div(class = "text-muted", "No custom calibration files detected yet.")
      } else {
        tags$div(
          class = "text-muted",
          sprintf(
            "Detected %d custom file%s: %s",
            length(paths),
            ifelse(length(paths) == 1, "", "s"),
            paste(names(paths), collapse = ", ")
          )
        )
      }
    } else {
      tags$div(class = "text-muted", "The default calibration will be used.")
    }
  })
  

  output$dl_raw_zip <- downloadHandler(
    filename = function() export_name("_raw_spectra", "zip"),
    content = function(file) {
      req(isTRUE(spectra_ready()))
      paths_tbl <- spectra_paths(); req(!is.null(paths_tbl), nrow(paths_tbl) > 0)
      paths <- paths_tbl$file_path; paths <- paths[file.exists(paths)]
      req(length(paths) > 0)
      
      # Build the zip now (blocks during click)
      zipfile <- tempfile(fileext = ".zip")
      if (requireNamespace("zip", quietly = TRUE)) {
        zip::zipr(zipfile, files = paths, include_directories = FALSE)
      } else {
        oldwd <- getwd(); on.exit(setwd(oldwd), add = TRUE)
        common_dir <- normalizePath(dirname(paths[1L])); setwd(common_dir)
        utils::zip(zipfile, files = basename(paths))
      }
      file.copy(zipfile, file, overwrite = TRUE)
    }
  )
  
  output$tbl_builtin <- DT::renderDT({
    req(input$db_file)                 # hide until a file is uploaded
    render_dt_with_buttons(geo_tbl, "_recalculated")
  }, server = FALSE)
  
  output$tbl_empirical <- DT::renderDT({
    req(empirical_merged())
    render_dt_with_buttons(empirical_display, "_empirical")
  }, server = FALSE)
  
  output$raw_dl_block <- renderUI({
    if (is.null(input$db_file)) {
      return(tags$small(class = "text-muted rawzip-status",
                        "Upload a database to enable processing."))
    }
    
    ready <- isTRUE(spectra_ready())  
    
    if (ready) {
      tags$div(class = "rawzip-block",
               downloadButton("dl_raw_zip", "Download raw spectra (CSV ZIP)"),
               tags$small(class = "text-muted rawzip-status", role = "status", `aria-live` = "polite",
                          raw_status() %||% "Raw spectra ready.")
      )
    } else {
      tags$div(class = "rawzip-block",
               actionButton("dl_raw_zip_fake", "Download raw spectra (CSV ZIP)",
                            class = "btn-default",
                            disabled = "disabled", `aria-disabled` = "true", tabindex = "-1"),
               tags$small(class = "text-muted rawzip-status", role = "status", `aria-live` = "polite",
                          raw_status() %||% "Processing raw spectra…")
      )
    }
  })
  
  
  
  
})
