###################################################################################################
## Load / install packages:
required_pkgs <- c("lubridate", "terra", "dplyr", "data.table", "ecmwfr", "crayon")
missing_pkgs <- required_pkgs[!sapply(required_pkgs, requireNamespace, quietly = TRUE)]

if (length(missing_pkgs) > 0) {
  stop(
    "The following packages are required but not installed: ",
    paste(missing_pkgs, collapse = ", "),
    call. = FALSE 
  )
}

###################################################################################################
## Download a specific batch of ERA5 data from the CDS

library(ecmwfr)
#' This function builds and executes a request to the CDS API. It is designed
#' to be robust, with error handling and checks for existing files. It can
#' handle both single-level and pressure-level data.
#'
#' @param year The year to download (as a string, e.g., "2019").
#' @param variables A character vector of variables to download.
#' @param dataset The official short name of the CDS dataset.
#' @param dataset_type_name A short, descriptive name for this batch (e.g., "sl_wind", "pl_geopotential"). Used for the filename.
#' @param pressure_levels Optional. A character vector of pressure levels. If NULL, a single-level request is made.
#' @param output_dir The directory where the downloaded file will be saved.
#' @param area The geographic bounding box vector (N, W, S, E).
#'
#' @return Nothing. The function downloads a file as a side effect.
#'

library(ecmwfr)

download_era5_data <- function(year,
                               variables,
                               dataset,
                               dataset_type_name,
                               pressure_levels = NULL,
                               output_dir,
                               area) {
  
  cat(paste("\n=======================================================\n"))
  cat(paste("--- Processing Year:", year, "| Batch:", dataset_type_name, "---\n"))
  cat(paste("=======================================================\n"))
  
  target_filename <- paste0("ERA5_", dataset_type_name, "_", year, ".nc")
  full_output_path <- file.path(output_dir, target_filename)
  
  if (file.exists(full_output_path)) {
    cat("File already exists, SKIPPING download:\n  ", full_output_path, "\n")
    return(TRUE) # Return TRUE because, for our purposes, the job is "successful".
  }
  
  request <- list(
    dataset_short_name = dataset,
    product_type       = "reanalysis",
    variable           = variables,
    year               = year,
    month              = sprintf("%02d", 1:12),
    day                = sprintf("%02d", 1:31),
    time               = sprintf("%02d:00", 0:23),
    format             = "netcdf",
    area               = area,
    target             = target_filename
  )
  
  if (!is.null(pressure_levels)) {
    request$pressure_level <- pressure_levels
  }
  
  success <- tryCatch({
    cat("Submitting request to CDS...\n")
    wf_request(
      request  = request,
      transfer = TRUE,
      path     = output_dir
    )
    cat("\nSUCCESS: Download complete for", target_filename, "at", paste0(Sys.time()), "\n")
    TRUE # If wf_request completes, return TRUE
    
  }, error = function(e) {
    cat("\nERROR: Failed to download data for this batch.\n")
    cat("  Year:", year, "\n")
    cat("  Batch:", dataset_type_name, "\n")
    cat("The error message was:", e$message, "\n")
    FALSE # If an error occurs, return FALSE
  })
  
  return(success)
}



###################################################################################################
## Extract location data for specific dates from ERA5 files
## Functions for ERA5 annotation
#'
#' This functions extracts matching data from tracking data and ERA5 climate files
#' based on date. They support data frames as input ONLY (no move or track objects).
#' Function's job is to calculate the weather value at the exact location and timestamp for each GPS point by interpolating between the two nearest hourly values
#' The function support only yearly data (ERA5 data for whole years)
#'
#' @param track A data frame containing tracking data
#' @param era5_file File name and path to the ERA5 file (the idea is to to loop through all the files)
#' @param eventCol Column name containing event IDs (default: NULL)
#' @param timeCol Column name containing timestamps (as POSIXct) in the data.frame (default: "timestamp")
#' @param coordsCol Column names containing coordinates (in EPSG 4326) in the data.frame, vector of two strings (default: NULL)
#' @param heightCol Column name containing height data  in the data.frame (default: NULL)
#' NOT needed for pure weather data annotation
#' @param chunk_size Size of chunks of data, that is processed in the terra:extraction process. If datasets (track data) or rastersets (ERA5 data) 
#' gets to big, the annotation would crash due to limited memory. Default is chunk size = 10000
#' @param extracted_dataA Build via function "extract_locations_and_era5_data". A list containing:
#' \itemize{
#' \item \code{era5_file_name}: Name of the ERA5 file that should be processed
#' \item \code{dataset}: Dataset name extracted from file name e.g. ""ERA5_pl_geopot_2018.nc"
#' \item \code{location_data}: gps tracking data, reduced to the year of the data set
#'
#' @return A list containing:
#' \itemize{
#' \item \code{era5_file_name}: Name of the ERA5 file
#' \item \code{dataset}: Dataset name extracted from file name e.g. ""ERA5_pl_geopot_2018.nc"
#' \item \code{era5_date_start}: Start date of the ERA5 file
#' \item \code{era5_date_end}: End date of the ERA5 file
#' \item \code{file_type}: Type of ERA5 file (ONLY yearly)
#' \item \code{location_data}: Filtered tracking data with columns:
#'    * event_id
#'    * Long (longitude)
#'    * Lat (latitude)
#'    * timestamp
#'    * heights (optional)
#' }

  # Load packages
  # The main script should also load these
  library(lubridate)
  library(terra)
  library(dplyr)
  library(data.table)
  
  
  # -------------------------------------------------------------------------
  # FUNCTION 1: Extract and Match GPS data to a weather file
  # -------------------------------------------------------------------------
  extract_locations_and_era5_data <- function(track, era5_file,
                                              eventCol = "individual_local_identifier",
                                              timeCol = "timestamp",
                                              #heightCol = "height_above_ellipsoid",
                                              coordsCol = c("location_long", "location_lat")) {
    
    # 1. Parse filename to get date range
    era5_filename <- basename(era5_file)
    if (grepl("_\\d{4}\\.nc$", era5_filename)) {
      file_type <- "yearly"
      year_str <- sub("^.*_(\\d{4})\\.nc$", "\\1", era5_filename)
      year <- as.numeric(year_str)
      date_start <- ymd(paste0(year, "-01-01"))
      date_end <- ymd(paste0(year, "-12-31"))
    } else {
      stop(paste0("Unknown ERA5 file format. Expected a yearly pattern like '..._YYYY.nc': ", era5_filename))
    }
    
    # 2. Filter GPS data.frame to match date range
    track[[timeCol]] <- as.POSIXct(track[[timeCol]], tz = "UTC")
    matching_track <- track %>%
      filter(as.Date(!!sym(timeCol)) >= date_start & as.Date(!!sym(timeCol)) <= date_end)
    if (nrow(matching_track) == 0) return(NULL)
    
    # 3. Prepare standardized data
    event_id <- if (eventCol %in% names(matching_track)) matching_track[[eventCol]] else 1:nrow(matching_track)
    coords <- as.matrix(matching_track[, coordsCol])
    times <- matching_track[[timeCol]]
    #heights <- if (heightCol %in% names(matching_track)) matching_track[[heightCol]] else rep(NA, nrow(matching_track))
    
    location_data <- data.frame(
      event_id = event_id,
      Long = coords[, 1],
      Lat = coords[, 2],
      #heights = heights,
      timestamp = times
    )
    
    # 4. Bundle and return
    list(
      era5_file_name = era5_file,
      dataset = era5_filename,
      location_data = location_data
    )
  }
  
  
  # -------------------------------------------------------------------------
  # FUNCTION 2:  Temporal Interpolation
  # -------------------------------------------------------------------------
  ## The function will be called in the mail annotation function (see further down)
  ## the element "interpolated_inSpace" comes from annotation function
  ## interpolated_inSpace: A wide data frame where each row is a GPS point and each column is 
  ## the weather value at a specific hour (e.g., u10_14:00, u10_15:00)
.interpolate_in_time <- function(interpolated_inSpace, extracted_data) {
  
  # Safety check
  if (is.null(interpolated_inSpace) || nrow(interpolated_inSpace) == 0) return(NULL)
  # Get the single variable name from the first column
  variable_name <- gsub("_valid_time=.*", "", names(interpolated_inSpace)[1]) # like "u100"
  
  ## 1. Data prep:
  # Get the timestamps for each layer (column)
  layer_times <- as.POSIXct(
    as.numeric(gsub(".*valid_time=([0-9]+).*", "\\1", names(interpolated_inSpace))),
    origin = "1970-01-01", tz = "UTC"
  )
  
  # Create a long-format data.table of the weather data
  weather_dt <- data.table(
    point_id = rep(1:nrow(interpolated_inSpace), times = ncol(interpolated_inSpace)),
    time = rep(layer_times, each = nrow(interpolated_inSpace)),
    weather_value = as.vector(as.matrix(interpolated_inSpace))
  )
  
  # Create a data.table of the GPS data to interpolate to
  gps_data <- data.table(
    point_id = 1:nrow(extracted_data$location_data),
    timestamp = extracted_data$location_data$timestamp
  )
  
  ## 2. Perform Rolling Joins to Find Neighbors:
  setkey(weather_dt, point_id, time)
  setkey(gps_data, point_id, timestamp)
  
  # For each GPS point, find the weather data point immediately before it
  joined_data <- weather_dt[gps_data, roll = -Inf] # = join function; for each gps data, it finds the nearest, previous matching point in the weather_dt (e.g. for a GPS point at 14:25, it finds the weather data for 14:00)
  setnames(joined_data, c("time", "weather_value"), c("prev_time", "prev_value"))
  
  # Find the weather data point immediately after it
  joined_data_next <- weather_dt[gps_data, roll = Inf] # same for the nearest, following datapoint (e.g. For a GPS point at 14:25, it finds the weather data for 15:00)
  joined_data[, `:=`(next_time = joined_data_next$time, next_value = joined_data_next$weather_value)]
  
  ## 3. Perform Vectorized Interpolation:
  # interpolate between following and previous hourly weather data
  # Ensure time columns are POSIXct
  time_cols <- c("timestamp", "prev_time", "next_time")
  for (col in time_cols) {
    set(joined_data, j = col, value = as.POSIXct(joined_data[[col]], origin = "1970-01-01", tz = "UTC"))
  }
  
  # Calculate time differences for the entire dataset at once
  joined_data[, total_diff := as.numeric(next_time - prev_time, units = "secs")]
  joined_data[, part_diff := as.numeric(timestamp - prev_time, units = "secs")]
  
  # Initialize the final column
  joined_data[, interpolated_value := as.numeric(NA)]
  
  # Handle points that fall exactly on a time step
  joined_data[total_diff == 0, interpolated_value := prev_value]
  
  # Perform linear interpolation for all other points in a single vectorized calculation
  joined_data[total_diff > 0, interpolated_value := prev_value + (next_value - prev_value) * (part_diff / total_diff)]
  
  ## 4. Prepare and Return the Final Data Frame:
  # Create the final data frame with just the one column of results
  final_df <- data.frame(interpolated_values = joined_data$interpolated_value)
  
  # Name the column correctly
  names(final_df) <- variable_name
  
  return(final_df)
}


  # -------------------------------------------------------------------------
  # FUNCTION 3: The Main Annotation Function
  # -------------------------------------------------------------------------
  ## The main function "annotate_era5_data" uses harddisc-space to temporarily store intermediate results
  ## This should prevent the function to crash due to memory limitation 
  ## If the ERA5 files and GPS data gets to big, processing the data will not work without this step
  ## This function is optimized to be run in parallel, e.g. with the "future_lapply" function to speed things up

annotate_era5_data <- function(extracted_data, chunk_size = 5000) { 
  
  # Safety check
  if (is.null(extracted_data) || is.null(extracted_data$era5_file_name)) {
    warning("annotate_era5_data received a NULL or invalid input. Skipping.")
    return(NULL)
  }
  
  ## Create a unique temporary directory for the element of "extracted data" (list of era5 filename and data-chunk of gps data)
  base_filename <- tools::file_path_sans_ext(basename(extracted_data$era5_file_name))
  worker_temp_dir <- file.path(tempdir(), paste0("chunks_", base_filename, "_", Sys.getpid()))
  dir.create(worker_temp_dir, showWarnings = FALSE, recursive = TRUE)
  
  # Use tryCatch with a `finally` expression to guarantee cleanup of temp data
  final_result <- tryCatch({
    # Load ERA5 raster data and prepare tracking data:
    cat("\nLoading ERA5 raster file:", crayon::blue(basename(extracted_data$era5_file_name)), "\n")
    era5_raster <- terra::rast(extracted_data$era5_file_name)
    location_data <- extracted_data$location_data
    coords <- location_data[, c("Long", "Lat")]
    n_points <- nrow(coords)
    
    cat("Extracting data for", crayon::yellow(n_points), "points in chunks of", crayon::yellow(chunk_size), "...\n")
    chunk_starts <- seq(1, n_points, by = chunk_size) # cut into chunks
    
    # loop over all chunks of tracking data, annotate ERA5 data and save this in a temp data file
    # This step is needed because of memory limitations
    for (i in 1:length(chunk_starts)) {
      start_index <- chunk_starts[i]
      end_index <- min(start_index + chunk_size - 1, n_points)
      
      cat("  Processing chunk", crayon::bold(i), "of", length(chunk_starts), "...\n")
      
      # Subset the location and coordinate data for this chunk
      chunk_location_data <- location_data[start_index:end_index, ]
      chunk_coords <- coords[start_index:end_index, ] # This was also missing
      
      # Spatially interpolate for this chunk
      # interpolated_inSpace: A wide data frame where each row is a GPS point and each column 
      # is the weather value at a specific hour (e.g., u10_14:00, u10_15:00)
      interpolated_inSpace <- terra::extract(era5_raster, chunk_coords, method = "bilinear")
      interpolated_inSpace <- interpolated_inSpace[, -1]
      
      # Temporally interpolate
      chunk_extracted_data <- list(location_data = chunk_location_data)
      interpolated_inTime <- .interpolate_in_time(interpolated_inSpace, chunk_extracted_data)
      if (is.null(interpolated_inTime)) { next }
      
      # Combine the results for this chunk
      chunk_annotated_data <- cbind(chunk_location_data, interpolated_inTime)
      
      # Write the processed chunk to a temporary file. Will be removed again later
      temp_file_path <- file.path(worker_temp_dir, paste0("chunk_", i, ".rds"))
      saveRDS(chunk_annotated_data, file = temp_file_path)
      
      # Clean up memory
      rm(chunk_location_data, chunk_coords, interpolated_inSpace, interpolated_inTime, chunk_annotated_data); gc()
    }
    
    # 3: Read all temporary files from the temp directory 
    cat(crayon::green("All chunks processed. Combining results...\n"))
    temp_files <- list.files(worker_temp_dir, pattern = "\\.rds$", full.names = TRUE)
    if (length(temp_files) == 0) {
      warning("No temporary chunk files were created for this task.")
      return(NULL)
    }
    # Read the data back and create single dataframe
    final_annotated_data <- rbindlist(lapply(temp_files, readRDS))
    
    # Rename columns of ERA5 data, otherwise they look chaotic
    # use `setnames` from data.table to prevent messing up
    original_gps_cols <- names(location_data)
    weather_cols <- names(final_annotated_data)[!names(final_annotated_data) %in% original_gps_cols]
    
    prefix <- sub("ERA5_(.*)_\\d{4}\\.nc", "\\1", basename(extracted_data$era5_file_name))
    new_weather_names <- paste0(prefix, "_", weather_cols)
    
    setnames(final_annotated_data, old = weather_cols, new = new_weather_names)
    
    cat("\nFinished processing ERA5 raster file:", crayon::blue(basename(extracted_data$era5_file_name)), "\n")
    
    # Return the final data frame
    return(as.data.frame(final_annotated_data))
    
  }, finally = {
    # cleaning data from disc after successfull processing 
    cat("Cleaning up temporary directory:", crayon::yellow(worker_temp_dir), "\n")
    unlink(worker_temp_dir, recursive = TRUE, force = TRUE)
  })
  
  return(final_result)
}

  # -------------------------------------------------------------------------
  # FUNCTION 4: The Main Annotation Function (alternative)
  # -------------------------------------------------------------------------
  ## This function cannot be run in parallel, but in a loop-approach
  ## The function has no temporarily hard-disc-saving approach, so it should run a bit faster than the former one
  ## But it is less error prone and might crash due to memory issues
annotate_era5_data_loop <- function(extracted_data, chunk_size = 5000) { 
  # Safety check 
  if (is.null(extracted_data) || is.null(extracted_data$era5_file_name)) {
    warning("annotate_era5_data received a NULL or invalid input. Skipping.")
    return(NULL)
  }
  
  cat("\nLoading ERA5 raster file:", crayon::blue(basename(extracted_data$era5_file_name)), "\n")
  era5_raster <- terra::rast(extracted_data$era5_file_name)
  coords <- extracted_data$location_data[, c("Long", "Lat")]
  n_datapoints <- nrow(coords)
  
  cat("Extracting data for", crayon::yellow(n_datapoints), "datapoints in chunks of", crayon::yellow(chunk_size), "each\n")
  chunk_starts <- seq(1, n_datapoints, by = chunk_size)
  results_from_chunks <- list()
  
  # Loop through each chunk
  for (i in 1:length(chunk_starts)) {
    start_index <- chunk_starts[i]
    end_index <- min(start_index + chunk_size - 1, n_datapoints)
    
    cat("  Processing chunk", crayon::bold(i), "of", length(chunk_starts), 
        "(datapoints", crayon::yellow(start_index), "to", crayon::yellow(end_index), ")...\n")
    
    # Get the coordinates for the current chunk
    current_coords_chunk <- coords[start_index:end_index, ]
    
    # Perform the extract operation only for this small chunk
    tryCatch({
    chunk_result <- terra::extract(era5_raster, current_coords_chunk, method = "bilinear")
    }, error = function(e) {
      cat(crayon::red("Failed to process chunk ", i , "\n"))
      return(NULL) # Return NULL on error
    })
    
   # cat("  extraction", crayon::bold(i), "of", length(chunk_starts), "worked. Trying to store in list...\n") # can be used for debugging
    
    # Store the result in list
    results_from_chunks[[i]] <- chunk_result
    rm(chunk_result)
    gc() 
  }

  cat("Combining chunk results into dataframe\n")
  interpolated_inSpace <- do.call(rbind, results_from_chunks)
  rm(results_from_chunks)

  interpolated_inSpace <- interpolated_inSpace[, -1] # Drop the ID column
  
  cat("Interpolating in Time\n")
  interpolated_inTime <- .interpolate_in_time(interpolated_inSpace, extracted_data)
  rm(interpolated_inSpace)
  gc() 
  
  if (is.null(interpolated_inTime)) {
    warning(paste("Temporal interpolation failed for file:", basename(extracted_data$era5_file_name)))
    return(NULL)
  }
  
  annotated_data <- cbind(extracted_data$location_data, interpolated_inTime)
  
  # Create new, more descriptive column names
  original_gps_cols <- names(extracted_data$location_data)
  
  # Get the names of the NEW weather columns from the correct data frame
  weather_col <- names(annotated_data)[!names(annotated_data) %in% original_gps_cols]
  
  # Check if we found exactly one new column (as expected from your download strategy)
  if (length(weather_col) == 1) {
    
    # Extract the descriptive batch name from the filename
    new_name <- sub("ERA5_(.*)_\\d{4}\\.nc", "\\1", basename(extracted_data$era5_file_name))
    
    # Use setnames to rename the column IN PLACE on the correct data frame
    setnames(annotated_data, old = weather_col, new = new_name)
    
  } else {
    warning(
      "Expected to find exactly one new weather column, but found ", 
      length(weather_col), ". Renaming skipped for file: ", 
      basename(extracted_data$era5_file_name)
    )
  }

  cat("Finished processing ERA5 raster file:", crayon::blue(basename(extracted_data$era5_file_name)), "\n")
  return(annotated_data)
}