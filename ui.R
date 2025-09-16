shinyUI(
  fluidPage(
    tags$head(
      # Inline CSS to align the calibration mode radios + sticky footer layout
      tags$style(HTML("
        /* ---- layout for sticky footer ---- */
        html, body { height: 100%; }
        .container-fluid { min-height: 100%; display: flex; flex-direction: column; }
        .app-main { flex: 1 0 auto; }
        .app-footer { flex-shrink: 0; padding: 8px 0; color: #6c757d; font-size: 12px; }
        
        /* Make the calibration radios sit side-by-side with nice spacing */
        #calib_mode .shiny-options-group { display: flex; gap: 16px; flex-wrap: wrap; }

        /* BS3 (classic Shiny) */
        #calib_mode .radio-inline { display: inline-block; margin: 0; }

        /* BS5 (bslib themes) */
        #calib_mode .form-check-inline { display: inline-flex; margin: 0; }
        
        /* Make disabled buttons feel disabled across themes */
        .btn[disabled], .btn.disabled {
          pointer-events: none;
          opacity: 0.6;
          cursor: not-allowed;
        }
            
        /* Force vertical layout for the raw spectra block */
        .rawzip-block {
          display: flex;
          flex-direction: column;
          align-items: stretch;
          gap: 6px;
          max-width: 100%;
        }
        /* Button fills sidebar, wraps nicely, never overflows */
        .rawzip-block .btn {
          width: 100%;
          max-width: 100%;
          white-space: normal;
          word-break: break-word;
          line-height: 1.25;
        }
        /* Status is always below the button */
        .rawzip-status { display: block !important; }
  
        .btn-wide {
          display: block;
          width: 100%;
          max-width: 100%;
          white-space: normal;
          word-break: break-word;
          line-height: 1.25;
        }
      ")),
      
      # --- Favicon links (put files in ./www/) ---
      tags$link(rel = "icon", type = "image/svg+xml", href = "favicon.svg"),
      tags$link(rel = "icon", type = "image/png", sizes = "32x32", href = "favicon-32.png"),
      tags$link(rel = "icon", type = "image/png", sizes = "16x16", href = "favicon-16.png"),
      tags$link(rel = "shortcut icon", href = "favicon.ico"),
      tags$link(rel = "apple-touch-icon", sizes = "180x180", href = "favicon-180.png"),
      tags$link(rel = "icon", sizes = "192x192", href = "favicon-192.png")
    ),
    
    titlePanel(
      HTML("TropiCal v1.0<br><small class='text-muted'>A data processing tool for the Thermo Scientific&trade; Niton&trade; XL5 Plus handheld XRF analyzer</small>"),
      windowTitle = "TropiCal"
    ),
    
    # ---- main content wrapper (for sticky footer) ----
    div(class = "app-main",
        sidebarLayout(
          sidebarPanel(
            # shared across both tabs
            fileInput("db_file", "Upload database (.ncd / .sqlite / .db)",
                      accept = c(".ncd", ".sqlite", ".db")),
            uiOutput("tab_controls")
          ),
          mainPanel(
            tabsetPanel(
              id = "main_tabs",
              tabPanel("Built-in Calibration",   DTOutput("tbl_builtin")),
              tabPanel("Empirical Calibration",  DTOutput("tbl_empirical"))
            )
          )
        )
    ),
    
    # ---- footer ----
    tags$footer(class = "app-footer",
                HTML(
                  "&copy; ", format(Sys.Date(), "%Y"),
                  " TropiCal — Independent &amp; unofficial. ",
                  "Thermo Scientific&trade; and Niton&trade; are trademarks of Thermo Fisher Scientific Inc. &middot; ",
                  "<a href='https://github.com/dannilocruz/TropiCal' target='_blank' rel='noopener'>GitHub</a>",
                )
    )
  )
)
