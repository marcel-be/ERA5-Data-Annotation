#### Annotade GPS data with weather data from ERA5 files ####
## Files must be downloaded and within a single directory 
## check: download-script and check for correct file label names
## function script "ERA5_function_libs.r" must be sourced

# Load packages
library(dplyr)
library(data.table)
library(future.apply)
setwd("D:/roniah/")
setwd("/Users/pandadamda/roniah/")

# Source the needed functions:
source(paste0(getwd(),"/scripts/data_prep/ERA5_function_libs.r"))
options(future.globals.maxSize = 2000 * 1024^2) # This is 1000 MiB
plan(multisession, workers = 8) # set up number of workers for parallel processing
# For max parallel-power: workers = availableCores() - 1 (will probably crash due to memory limitations)


# Define paths and load data 
era5_dir <- paste0(getwd(),"/data/ERA5/") 
track_data <- fread("./data/data_storage/data_prep/02_RMdata_end_deployment.csv") %>% as_tibble()
era5_files <- list.files(era5_dir, pattern = "\\.nc$", full.names = TRUE)


###### TEST RUN DATA SET #######
track_data<- filter(track_data, individual_local_identifier=="Agathe")
track_data = track_data[seq(1, nrow(track_data), 100), ]
########################


## annotation workflow
# 1. Match GPS data to weather files
# Directory to store the intermediate successful results
intermediate_dir <- file.path(era5_dir, "annotated_results_parallel")
dir.create(intermediate_dir, showWarnings = FALSE)

# Create a list of jobs (== ERA5 files) that still need to be done.
jobs_to_do <- list()
for (file in era5_files) {
  output_filename <- paste0("annotated_", tools::file_path_sans_ext(basename(file)), ".rds")
  output_filepath <- file.path(intermediate_dir, output_filename)
  
  if (!file.exists(output_filepath)) {
    jobs_to_do <- append(jobs_to_do, file)
  }
}
if (length(jobs_to_do) == 0) {
  cat(crayon::green("All files have already been processed!\n"))
} else {
  cat(crayon::yellow(length(jobs_to_do)), "files remaining to process.\n")}
  

## run annotation in PARALLEL
start<- Sys.time()
annotated_data_list<-  future_lapply(
    jobs_to_do, # Loop only over the files that are not yet done
    function(current_file) {
      
      # Define the output path for this specific job
      output_filename <- paste0("annotated_", tools::file_path_sans_ext(basename(current_file)), ".rds")
      output_filepath <- file.path(intermediate_dir, output_filename)
      
      # Use tryCatch to handle errors for this one file
      tryCatch({
        
        # Step A: Extract
        extracted_data <- extract_locations_and_era5_data(
          track = track_data,
          era5_file = current_file,
          eventCol = "individual_local_identifier",
          coordsCol = c("location_long", "location_lat")
        )
        
        # Step B: Annotate
        if (!is.null(extracted_data)) {
          annotated_result <- annotate_era5_data(
            extracted_data,
            chunk_size = 5000
          )
          
          # Step C: Save the successful result to disk
          if (!is.null(annotated_result)) {
            saveRDS(annotated_result, file = output_filepath)
          }
        }
        
      }, error = function(e) {
        # This code runs if an error occurs.
        # It won't be visible on Windows with multisession, but it prevents a crash.
        # We could write the error to a log file here if needed.
        cat("Error processing", basename(current_file), ":", e$message, "\n")
        return(NULL) # Return NULL to signify failure for this iteration
      })
    },
    future.seed = TRUE
  )
end<- Sys.time()
end - start # test: 5 times faster than looping via "lapply" (next step)


## run annotation in LOOP Approach (takes longer, less error prone and good for debugging)
failed_files_log <- list()
start<- Sys.time()
annotated_data_list <- lapply(
  jobs_to_do, 
  function(current_file) {
    output_filename <- paste0("annotated_", tools::file_path_sans_ext(basename(current_file)), ".rds")
    output_filepath <- file.path(intermediate_dir, output_filename)
    
    extracted_data <- extract_locations_and_era5_data(
      track = track_data,
      era5_file = current_file,
      eventCol = "individual_local_identifier",
      coordsCol = c("location_long", "location_lat")
      #heightCol = "height_above_ellipsoid" # not needed for pure weather-data-annotation
    )
    if (is.null(extracted_data)) {
      return(NULL)
    }
    tryCatch({
    annotated_result <- annotate_era5_data(
      extracted_data,
      chunk_size = 5000 # or whatever value works
    )
    if (!is.null(annotated_result)) {
      saveRDS(annotated_result, file = output_filepath)
    }
    return(annotated_result)
      }, error = function(e) {
      cat(crayon::red("Failed to process", basename(current_file), "\n"))
      failed_files_log <<- append(failed_files_log, current_file) # Log the failure
      return(NULL) # Return NULL on error
    })
      })
end<- Sys.time()
end - start # 18h

## Put all files together into one dataframe (== tracking df + all ERA5 variables)
intermediate_files <- list.files(intermediate_dir, pattern = "\\.rds$", full.names = TRUE)
annotated_data_list <- lapply(intermediate_files, readRDS)
annotated_data_list <- annotated_data_list[!sapply(annotated_data_list, is.null)]
annotated_df_long <- rbindlist(annotated_data_list, fill = TRUE, use.names = TRUE)

str(annotated_df_long)


##############################
#### Change naming in annotate functions!!!!
##############################

## The following step is only needed if there was an issue with the naming of the "geopotential"-variables! (Should be fixed in the actual functions_libs)
annotated_df_long <- annotated_df_long %>%
  mutate(
    # Merge the two geopotential columns into one called "geopotential"
    geopotential = coalesce(`pl_geopot_z_pressure_level=900`, `pl_z_pressure_level=900`),
    .keep = "unused" # This is a great option to drop the original messy columns
  )


annotated_df <- annotated_df_long %>%
  filter(!is.null(event_id) & !is.na(event_id)) %>%
  group_by(event_id, timestamp, Long, Lat) %>%
  summarise(
    across(everything(), ~ first(na.omit(.))),
    # Remove the .groups argument from here
  ) %>%
  ungroup()


# save df
fwrite(annotated_df, file = "./data/data_storage/data_prep/03_RMdata_ERA5.csv")
