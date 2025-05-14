# Load required packages
library(httr)
library(jsonlite)
library(dplyr)
library(purrr)
library(tidycensus)
library(writexl)
library(tidygeocoder)
library(sf)
library(leaflet)
library(shiny)
library(shinydashboard)
library(DT)
library(ggplot2)
library(stringr)

# Set your Census API key (replace with your real key if needed)
census_api_key("YOUR_CENSUS_API_KEY", install = TRUE, overwrite = TRUE)

# Define base URL for NPI Registry
base_url <- "https://npiregistry.cms.hhs.gov/api/?version=2.1"

# List of all organizations to include
organizations <- c(
  "Catholic Charities",
  "Lutheran Services",
  "Salvation Army",
  "Volunteers of America",
  "YMCA",
  "YWCA"
)

# Function to get NPI data for an organization with a skip value (pagination)
get_npi_data <- function(org_name, skip_val = 0) {
  # Format the organization name for the API query
  # Adding wildcard to capture variations in name
  formatted_org_name <- paste0(org_name, "*")
  
  # Set parameters for the API request
  params <- list(
    organization_name = formatted_org_name,
    enumeration_type = "NPI-2",  # Organization NPI type
    limit = 200,                 # Maximum records per request
    skip = skip_val              # Pagination offset
  )
  
  cat(paste0("  Querying API with: ", formatted_org_name, " (skip=", skip_val, ")\n"))
  
  # Make the API request
  response <- GET(base_url, query = params)
  
  # Check if the request was successful
  if (status_code(response) != 200) {
    warning(paste("Failed to retrieve data for", org_name, "with skip value", skip_val))
    return(NULL)
  }
  
  # Parse the response
  content_data <- content(response, as = "text", encoding = "UTF-8")
  parsed_json <- fromJSON(content_data, flatten = TRUE)
  
  # Check if results exist and are not empty
  if (!is.null(parsed_json$results) && length(parsed_json$results) > 0) {
    # Add the organization name as an identifier
    results <- parsed_json$results
    
    # Only proceed if results is a data frame
    if (is.data.frame(results)) {
      results$source_organization <- org_name
      return(results)
    }
  }
  
  return(NULL)
}

# Function to collect NPI data for a single organization (handling pagination properly)
collect_org_data <- function(org_name) {
  cat(paste0("Collecting data for: ", org_name, "\n"))
  
  # Start with empty results and first page
  total_results <- data.frame()
  current_skip <- 0
  more_results <- TRUE
  
  # Loop until no more results or reached a reasonable limit
  max_pages <- 50  # Safety limit to prevent infinite loops
  page_count <- 0
  
  while(more_results && page_count < max_pages) {
    # Get the current batch of results
    results <- get_npi_data(org_name, current_skip)
    
    # If results exist and contain rows, add them to total
    if(!is.null(results) && nrow(results) > 0) {
      total_results <- bind_rows(total_results, results)
      
      # Check if we likely have more results
      if(nrow(results) == 200) {
        # Move to next page
        current_skip <- current_skip + 200
        cat(paste0("  - Retrieved page ", page_count + 1, 
                   " (", nrow(results), " records). Fetching more...\n"))
      } else {
        # We got less than 200 results, so we've reached the end
        more_results <- FALSE
        cat(paste0("  - Retrieved page ", page_count + 1, 
                   " (", nrow(results), " records). No more results.\n"))
      }
    } else {
      # No results returned, end loop
      more_results <- FALSE
      if(page_count == 0) {
        cat(paste0("No results found for: ", org_name, "\n"))
      }
    }
    
    page_count <- page_count + 1
    
    # Add a small delay to avoid overloading the API
    Sys.sleep(1)
  }
  
  if(page_count >= max_pages) {
    warning(paste0("Reached maximum page limit (", max_pages, 
                   ") for ", org_name, ". There may be more results."))
  }
  
  cat(paste0("Found ", nrow(total_results), " results for: ", org_name, 
             " across ", page_count, " pages\n"))
  return(total_results)
}

# Function to process the raw NPI data into a clean format
process_npi_data <- function(raw_data) {
  if (nrow(raw_data) == 0) {
    return(data.frame())
  }
  
  # Add error handling to the data processing
  tryCatch({
    # First, examine the structure to help debug
    cat("Column names in raw data:\n")
    print(names(raw_data))
    
    # Find important fields
    cat("\nLooking for important fields...\n")
    potential_npi_fields <- c("number", "basic.number", "npi", "npi_number")
    for (field in potential_npi_fields) {
      if (field %in% names(raw_data)) {
        cat("  Found NPI field: ", field, "\n")
      }
    }
    
    potential_org_fields <- c("organization_name", "basic.organization_name", "name")
    for (field in potential_org_fields) {
      if (field %in% names(raw_data)) {
        cat("  Found organization name field: ", field, "\n")
      }
    }
    
    # Print first row to see structure
    if (nrow(raw_data) > 0) {
      cat("\nExamining first row to understand structure:\n")
      first_row_str <- capture.output(str(raw_data[1,]))
      cat(paste(first_row_str[1:min(20, length(first_row_str))], collapse = "\n"))
      cat("\n... (truncated for brevity) ...\n\n")
    }
    
    # Check specifically for NPI number field and organization name with glimpse
    if ("basic" %in% names(raw_data) && is.list(raw_data$basic)) {
      cat("\nExamining 'basic' field structure:\n")
      if (!is.null(raw_data$basic[[1]])) {
        basic_fields <- names(raw_data$basic[[1]])
        cat("Fields in 'basic': ", paste(basic_fields, collapse = ", "), "\n")
        
        if ("number" %in% basic_fields) {
          cat("  Found 'number' in basic field\n")
        }
        
        if ("organization_name" %in% basic_fields) {
          cat("  Found 'organization_name' in basic field\n")
        }
      }
    }
    
    processed_data <- raw_data %>%
      mutate(
        # Extract NPI number from basic field if it exists that way
        npi_value = if ("basic" %in% names(.) && is.list(.$basic)) {
          map_chr(basic, function(b) {
            if (!is.null(b) && "number" %in% names(b)) {
              return(b$number)
            } else {
              return(NA_character_)
            }
          })
        } else {
          NA_character_
        },
        
        # Extract organization name from basic field if it exists that way
        org_name_value = if ("basic" %in% names(.) && is.list(.$basic)) {
          map_chr(basic, function(b) {
            if (!is.null(b) && "organization_name" %in% names(b)) {
              return(b$organization_name)
            } else {
              return(NA_character_)
            }
          })
        } else {
          NA_character_
        },
        
        practice_address = map(addresses, ~ {
          # Extra safety check
          if (is.null(.x) || !is.data.frame(.x) || nrow(.x) == 0) {
            return(NA)
          }
          
          address <- .x %>%
            filter(address_purpose == "LOCATION") %>%
            slice(1)
          if (nrow(address) == 0) return(NA) else return(address)
        }),
        street_address = map_chr(practice_address, ~ {
          if (is.data.frame(.x) && "address_1" %in% names(.x)) {
            return(.x$address_1)
          } else {
            return(NA_character_)
          }
        }),
        city = map_chr(practice_address, ~ {
          if (is.data.frame(.x) && "city" %in% names(.x)) {
            return(.x$city)
          } else {
            return(NA_character_)
          }
        }),
        state = map_chr(practice_address, ~ {
          if (is.data.frame(.x) && "state" %in% names(.x)) {
            return(.x$state)
          } else {
            return(NA_character_)
          }
        }),
        postal_code = map_chr(practice_address, ~ {
          if (is.data.frame(.x) && "postal_code" %in% names(.x)) {
            return(.x$postal_code)
          } else {
            return(NA_character_)
          }
        }),
        # Extract all taxonomies and check which ones can bill Medicaid
        taxonomies_list = map(taxonomies, function(tax) {
          # Extra safety check
          if (is.null(tax) || length(tax) == 0 || !is.data.frame(tax)) {
            return(data.frame())
          }
          
          # Handle the can_bill_medicaid logic more carefully
          can_bill <- logical(length(tax$code))
          for (i in seq_along(tax$code)) {
            # Check if either license or taxonomy_group exists for this taxonomy
            has_license <- FALSE
            has_taxonomy_group <- FALSE
            
            if (!is.null(tax$license) && length(tax$license) >= i) {
              has_license <- !is.null(tax$license[[i]]) && !all(is.na(tax$license[[i]]))
            }
            
            if (!is.null(tax$taxonomy_group) && length(tax$taxonomy_group) >= i) {
              has_taxonomy_group <- !is.null(tax$taxonomy_group[[i]]) && !all(is.na(tax$taxonomy_group[[i]]))
            }
            
            can_bill[i] <- has_license || has_taxonomy_group
          }
          
          return(data.frame(
            code = tax$code,
            desc = tax$desc,
            primary = tax$primary,
            can_bill_medicaid = can_bill,
            stringsAsFactors = FALSE
          ))
        }),
        # Extract the primary taxonomy description
        primary_taxonomy = map_chr(taxonomies_list, ~ {
          if (is.data.frame(.x) && nrow(.x) > 0) {
            primary_tax <- .x %>% filter(primary == TRUE)
            if (nrow(primary_tax) > 0) {
              return(primary_tax$desc[1])
            } else if (length(.x$desc) > 0) {
              return(.x$desc[1])
            }
          }
          return(NA_character_)
        }),
        # Check if any taxonomy can bill Medicaid
        can_bill_medicaid = map_lgl(taxonomies_list, ~ {
          if (is.data.frame(.x) && nrow(.x) > 0 && "can_bill_medicaid" %in% names(.x)) {
            return(any(.x$can_bill_medicaid, na.rm = TRUE))
          } else {
            return(FALSE)
          }
        }),
        # Create a consolidated list of all taxonomy descriptions
        all_taxonomies = map_chr(taxonomies_list, ~ {
          if (is.data.frame(.x) && nrow(.x) > 0 && "desc" %in% names(.x)) {
            return(paste(.x$desc, collapse = "; "))
          } else {
            return(NA_character_)
          }
        })
      )
    
    # Now use transmute with safer field access
    processed_data <- processed_data %>%
      transmute(
        organization = source_organization,  # Add the organization identifier
        
        # Handle NPI number field using the extracted value or fallbacks
        npi = if (!all(is.na(npi_value))) {
          npi_value
        } else if ("number" %in% names(.)) {
          number
        } else {
          NA_character_
        },
        
        # Handle organization name field using the extracted value or fallbacks
        organization_name = if (!all(is.na(org_name_value))) {
          org_name_value
        } else if ("organization_name" %in% names(.)) {
          organization_name
        } else {
          NA_character_
        },
        
        street_address,
        city,
        state,
        postal_code,
        full_address = paste(street_address, city, state, postal_code),
        primary_taxonomy,
        all_taxonomies,
        can_bill_medicaid
      ) %>%
      filter(!is.na(street_address))
    
    cat(paste0("Successfully processed ", nrow(processed_data), " records\n"))
    return(processed_data)
  }, error = function(e) {
    # Print the error for debugging
    cat("Error in process_npi_data:", conditionMessage(e), "\n")
    if (!is.null(e$call$i)) {
      cat("Error occurred at row index:", e$call$i, "\n")
    }
    # Return empty data frame on error
    return(data.frame())
  })
}

# Function to collect and process data for all organizations
collect_all_organizations <- function() {
  all_data <- data.frame()
  
  for (org in organizations) {
    # Collect raw data for the organization
    raw_data <- collect_org_data(org)
    
    # Process the raw data
    if (!is.null(raw_data) && nrow(raw_data) > 0) {
      cat(paste0("\nProcessing data for: ", org, " (", nrow(raw_data), " records)\n"))
      
      # Try to process the data, but continue even if it fails
      tryCatch({
        processed_data <- process_npi_data(raw_data)
        
        # Add to the combined data frame
        if (!is.null(processed_data) && nrow(processed_data) > 0) {
          cat(paste0("Successfully processed ", nrow(processed_data), " records for ", org, "\n"))
          all_data <- bind_rows(all_data, processed_data)
        } else {
          cat(paste0("Warning: No processed records for ", org, "\n"))
        }
      }, error = function(e) {
        cat(paste0("Error processing data for ", org, ": ", conditionMessage(e), "\n"))
      })
    } else {
      cat(paste0("No raw data found for: ", org, "\n"))
    }
    
    # Add a small delay to avoid overloading the API
    Sys.sleep(1)
  }
  
  # Check if we have any data
  if (nrow(all_data) > 0) {
    cat(paste0("\nTotal records collected and processed: ", nrow(all_data), "\n"))
    return(all_data)
  } else {
    cat("\nNo data was successfully processed for any organization.\n")
    return(NULL)
  }
}

# Function to geocode the addresses
geocode_organizations <- function(org_data) {
  # Split the data into chunks to avoid overloading the geocoder
  chunk_size <- 100
  num_rows <- nrow(org_data)
  num_chunks <- ceiling(num_rows / chunk_size)
  
  geocoded_data <- data.frame()
  
  for (i in 1:num_chunks) {
    start_idx <- (i - 1) * chunk_size + 1
    end_idx <- min(i * chunk_size, num_rows)
    
    cat(paste0("Geocoding chunk ", i, " of ", num_chunks, 
               " (rows ", start_idx, " to ", end_idx, ")\n"))
    
    # Extract the current chunk
    chunk <- org_data[start_idx:end_idx, ]
    
    # Geocode the chunk
    geocoded_chunk <- chunk %>%
      geocode(
        address = full_address,
        method = "census",  # Use census method for geocoding
        lat = latitude,
        long = longitude
      )
    
    # Add to the combined data frame
    geocoded_data <- bind_rows(geocoded_data, geocoded_chunk)
    
    # Add a small delay to avoid overloading the geocoder
    Sys.sleep(2)
  }
  
  return(geocoded_data)
}

# Main function to execute the data collection and processing
collect_and_process_data <- function() {
  # Collect and process data for all organizations
  all_org_data <- collect_all_organizations()
  
  # Check if any data was collected and processed
  if (!is.null(all_org_data) && nrow(all_org_data) > 0) {
    cat(paste0("\nCollected and processed ", nrow(all_org_data), " total records.\n"))
    
    # Save the raw data before geocoding
    raw_output_file <- "all_organizations_npi_raw.xlsx"
    write_xlsx(all_org_data, raw_output_file)
    cat(paste0("Raw data exported to '", raw_output_file, "'\n"))
    
    # Geocode the data
    cat("\nGeocoding addresses...\n")
    tryCatch({
      geocoded_data <- geocode_organizations(all_org_data)
      
      # Filter for only valid geocoded results
      valid_geocoded <- geocoded_data %>%
        filter(!is.na(latitude) & !is.na(longitude))
      
      cat(paste0("Successfully geocoded ", nrow(valid_geocoded), " out of ", 
                 nrow(all_org_data), " records.\n"))
      
      # Write the data to an Excel file
      output_file <- "all_organizations_npi_with_lat_lon.xlsx"
      write_xlsx(valid_geocoded, output_file)
      cat(paste0("Geocoded data exported to '", output_file, "'\n"))
      
      return(valid_geocoded)
    }, error = function(e) {
      cat(paste0("Error during geocoding: ", conditionMessage(e), "\n"))
      cat("Returning non-geocoded data.\n")
      return(all_org_data)
    })
  } else {
    cat("No data found for any organizations.\n")
    return(NULL)
  }
}

# Function to create a spatial object from the geocoded data
create_spatial_data <- function(geocoded_data) {
  if (is.null(geocoded_data) || nrow(geocoded_data) == 0) {
    return(NULL)
  }
  
  # Create an sf object
  spatial_data <- geocoded_data %>%
    st_as_sf(coords = c("longitude", "latitude"), crs = 4326)
  
  return(spatial_data)
}

# Save the data for later use
save_data <- function(data, file_path = "organizations_medicaid_data.rds") {
  saveRDS(data, file_path)
  cat(paste0("Data saved to '", file_path, "'\n"))
}

# Load the saved data
load_data <- function(file_path = "organizations_medicaid_data.rds") {
  if (file.exists(file_path)) {
    return(readRDS(file_path))
  } else {
    cat(paste0("File not found: '", file_path, "'\n"))
    return(NULL)
  }
}

# UI for the Shiny dashboard
create_dashboard_ui <- function() {
  dashboardPage(
    dashboardHeader(title = "Organization Medicaid Billing Locations"),
    dashboardSidebar(
      sidebarMenu(
        menuItem("Dashboard", tabName = "dashboard", icon = icon("dashboard")),
        menuItem("Data Table", tabName = "data", icon = icon("table")),
        menuItem("Taxonomy Analysis", tabName = "taxonomy", icon = icon("chart-bar"))
      ),
      selectInput("org_filter", "Select Organization:", 
                  choices = c("All Organizations")),  # Will be populated in server
      selectInput("taxonomy_filter", "Select Service Type:", 
                  choices = c("All Services")),  # Will be populated in server
      checkboxInput("medicaid_only", "Show only Medicaid billing locations", value = TRUE),
      
      # Add a slider for the number of top taxonomies to display
      conditionalPanel(
        condition = "input.tabName == 'taxonomy'",
        sliderInput("top_n_taxonomies", "Number of top taxonomies to show:",
                    min = 5, max = 20, value = 10, step = 1)
      )
    ),
    dashboardBody(
      tabItems(
        # Dashboard tab
        tabItem(tabName = "dashboard",
                fluidRow(
                  box(width = 12, leafletOutput("map", height = 600))
                ),
                fluidRow(
                  box(width = 6, valueBoxOutput("location_count", width = NULL)),
                  box(width = 6, valueBoxOutput("org_count", width = NULL))
                )
        ),
        
        # Data table tab
        tabItem(tabName = "data",
                fluidRow(
                  box(width = 12, DT::dataTableOutput("locations_table"))
                )
        ),
        
        # Taxonomy analysis tab
        tabItem(tabName = "taxonomy",
                fluidRow(
                  box(width = 12, title = "Most Common Taxonomies", status = "primary", solidHeader = TRUE,
                      plotOutput("taxonomy_plot", height = 500))
                ),
                fluidRow(
                  box(width = 12, title = "Taxonomy Distribution Table", status = "info",
                      DT::dataTableOutput("taxonomy_table"))
                )
        )
      )
    )
  )
}

# Server for the Shiny dashboard
create_dashboard_server <- function(data) {
  function(input, output, session) {
    # Set active tab
    observeEvent(input$sidebarItemExpanded, {
      updateTabItems(session, "tabName", input$sidebarItemExpanded)
    })
    
    # Convert data to sf if needed
    data_sf <- if ("sf" %in% class(data)) data else create_spatial_data(data)
    
    # Extract all unique organizations and taxonomies
    all_orgs <- c("All Organizations", sort(unique(data$organization)))
    updateSelectInput(session, "org_filter", choices = all_orgs)
    
    # Extract all taxonomies from the combined string
    all_tax_combined <- unique(data$primary_taxonomy)
    all_tax_combined <- all_tax_combined[!is.na(all_tax_combined)]
    updateSelectInput(session, "taxonomy_filter", choices = c("All Services", sort(all_tax_combined)))
    
    # Reactive filtered data
    filtered_data <- reactive({
      # Start with all data
      result <- data
      
      # Filter by organization if not "All Organizations"
      if (input$org_filter != "All Organizations") {
        result <- result %>% filter(organization == input$org_filter)
      }
      
      # Filter by taxonomy if not "All Services"
      if (input$taxonomy_filter != "All Services") {
        result <- result %>% filter(grepl(input$taxonomy_filter, primary_taxonomy))
      }
      
      # Filter by Medicaid billing if checked
      if (input$medicaid_only) {
        result <- result %>% filter(can_bill_medicaid == TRUE)
      }
      
      return(result)
    })
    
    # Convert filtered data to spatial
    filtered_spatial <- reactive({
      create_spatial_data(filtered_data())
    })
    
    # Create a reactive for taxonomy distribution
    taxonomy_distribution <- reactive({
      req(filtered_data())
      
      # Extract and count taxonomies
      taxonomy_counts <- filtered_data() %>%
        filter(!is.na(primary_taxonomy)) %>%
        group_by(primary_taxonomy) %>%
        summarize(
          count = n(),
          pct = n() / nrow(filtered_data()) * 100,
          .groups = 'drop'
        ) %>%
        arrange(desc(count)) %>%
        mutate(
          formatted_pct = sprintf("%.1f%%", pct),
          taxonomy_short = ifelse(nchar(primary_taxonomy) > 50, 
                                  paste0(substr(primary_taxonomy, 1, 47), "..."), 
                                  primary_taxonomy)
        )
      
      return(taxonomy_counts)
    })
    
    # Map output
    output$map <- renderLeaflet({
      # Get filtered data
      map_data <- filtered_spatial()
      
      if (is.null(map_data) || nrow(map_data) == 0) {
        return(leaflet() %>% 
                 addTiles() %>% 
                 setView(lng = -98, lat = 39.5, zoom = 4) %>%
                 addControl("No locations match your selection", position = "topright"))
      }
      
      # Create base map
      map <- leaflet() %>%
        addTiles() %>%
        setView(lng = -98, lat = 39.5, zoom = 4)  # Center on US
      
      # Add markers for each location
      map %>% addMarkers(
        data = map_data,
        popup = ~paste0(
          "<b>", organization_name, "</b><br>",
          "Organization: ", organization, "<br>",
          street_address, "<br>",
          city, ", ", state, " ", postal_code, "<br>",
          "Service: ", primary_taxonomy, "<br>",
          "Medicaid Billing: ", ifelse(can_bill_medicaid, "Yes", "No")
        ),
        clusterOptions = markerClusterOptions()
      )
    })
    
    # Count box outputs
    output$location_count <- renderValueBox({
      valueBox(
        nrow(filtered_data()),
        "Locations",
        icon = icon("map-marker"),
        color = "blue"
      )
    })
    
    output$org_count <- renderValueBox({
      valueBox(
        length(unique(filtered_data()$organization)),
        "Organizations",
        icon = icon("building"),
        color = "green"
      )
    })
    
    # Data table output
    output$locations_table <- DT::renderDataTable({
      filtered_data() %>%
        select(organization, organization_name, street_address, city, state, 
               postal_code, primary_taxonomy, can_bill_medicaid)
    })
    
    # Taxonomy distribution plot
    output$taxonomy_plot <- renderPlot({
      # Get taxonomy distribution data
      tax_data <- taxonomy_distribution()
      
      # Check if we have data
      req(nrow(tax_data) > 0)
      
      # Get the top N taxonomies based on the slider
      top_n_tax <- head(tax_data, input$top_n_taxonomies)
      
      # Create a horizontal bar chart
      ggplot(top_n_tax, aes(x = reorder(taxonomy_short, count), y = count, fill = count)) +
        geom_col() +
        geom_text(aes(label = formatted_pct), hjust = -0.1, size = 3.5) +
        scale_fill_gradient(low = "skyblue", high = "darkblue") +
        coord_flip() +
        labs(
          title = paste("Top", input$top_n_taxonomies, "Taxonomies", 
                        ifelse(input$org_filter != "All Organizations", 
                               paste("for", input$org_filter), "")),
          subtitle = ifelse(input$medicaid_only, "Medicaid Billing Enabled Only", "All Locations"),
          x = "",
          y = "Number of Locations"
        ) +
        theme_minimal() +
        theme(
          plot.title = element_text(size = 16, face = "bold"),
          plot.subtitle = element_text(size = 12),
          axis.text.y = element_text(size = 10),
          legend.position = "none"
        )
    })
    
    # Taxonomy table output
    output$taxonomy_table <- DT::renderDataTable({
      # Get taxonomy distribution data
      tax_data <- taxonomy_distribution()
      
      # Create a formatted table
      tax_data %>%
        select(
          Taxonomy = primary_taxonomy,
          Count = count,
          Percentage = formatted_pct
        )
    }, options = list(pageLength = 10, scrollX = TRUE))
  }
}

# Function to run the Shiny dashboard
run_dashboard <- function(data_path = "organizations_medicaid_data.rds") {
  # Load data
  data <- load_data(data_path)
  
  if (is.null(data)) {
    stop("Could not load data. Please run data collection first.")
  }
  
  # Launch the app
  shinyApp(
    ui = create_dashboard_ui(),
    server = create_dashboard_server(data)
  )
}

# Main execution

# Uncomment and run these steps in sequence:

# Step 1: Collect and process the data (this may take a while)
# npi_data <- collect_and_process_data()

# Step 2: Save the data for later use
if (!is.null(npi_data)) {
   save_data(npi_data)
 }

# Step 3: Run the dashboard
 run_dashboard()