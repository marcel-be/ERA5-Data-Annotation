##########################################################################################################
# Download ERA5 data from Climate Data Store (CDS) of Copernicus Project
# Use CDS API to fetch files


#helpful code for Download and Annotation: GitHub: https://github.com/kamransafi/ERA5_move2

setwd("/path/to/your/project/folder/")

library(ecmwfr)
source(".../ERA5_function_libs.r" )

#set a key to the keychain
wf_set_key(key = "...")

# you can retrieve the key using
wf_get_key()

output_dir<- "/.../.../"
dir.create(output_dir, showWarnings = T)


## The following ar EXAMPLES:
years_to_download <- c("2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024")
bounding_box <- c(54.3, -10.2, 36.2, 17.3) #(N, W, S, E)
R
# Define all download jobs
all_jobs <- list(
  list(variables = c("10m_u_component_of_wind"),
       dataset = "reanalysis-era5-single-levels", dataset_type_name = "sl_wind_u_10m"),
  list(variables = c("100m_u_component_of_wind"), 
       dataset = "reanalysis-era5-single-levels", dataset_type_name = "sl_wind_u_100m"),
  list(variables = c("10m_v_component_of_wind"),
       dataset = "reanalysis-era5-single-levels", dataset_type_name = "sl_wind_v_10m"),
  list(variables = c("100m_v_component_of_wind"), 
       dataset = "reanalysis-era5-single-levels", dataset_type_name = "sl_wind_v_100m"),
  list(variables = c("total_precipitation"),
       dataset = "reanalysis-era5-single-levels", dataset_type_name = "sl_surface_prec"),
  list(variables = c("2m_temperature"),
       dataset = "reanalysis-era5-single-levels", dataset_type_name = "sl_surface_temp"),
  list(variables = c("mean_sea_level_pressure"), 
       dataset = "reanalysis-era5-single-levels", dataset_type_name = "sl_surface_pressure"),
  list(variables = c("geopotential"), 
       dataset = "reanalysis-era5-single-levels", dataset_type_name = "sl_surface_geopot"),
  list(variables = c("geopotential"),
       dataset = "reanalysis-era5-pressure-levels", dataset_type_name = "pl_geopot", pressure_levels = c("900")),
  list(variables = c("temperature"),
       dataset = "reanalysis-era5-pressure-levels", dataset_type_name = "pl_temp", pressure_levels = c("900"))
)


# Create the full list of jobs to do by combining years and job types
jobs_to_do <- list()
for (year in years_to_download) {
  for (job_template in all_jobs) {
    new_job <- job_template
    new_job$year <- year # Add the year to the job description
    jobs_to_do <- append(jobs_to_do, list(new_job))
  }
}

max_retries <- 3
retry_count <- 0

# Execute Download; Retry Download in case it crashes. The "Jobs" take controll of what is already downloaded. 

while (length(jobs_to_do) > 0 && retry_count <= max_retries) {
  
  if (retry_count > 0) {
    cat(paste("--- ATTEMPTING RETRY", retry_count, "OF", max_retries, "FOR", length(jobs_to_do), "FAILED JOBS ---\n"))
    # Wait a few minutes before retrying to let server issues potentially resolve
    Sys.sleep(300) # Pause for 5 minutes
  }
  
  failed_jobs_this_round <- list() # To store failures from the current attempt
  
  # Loop through the list of jobs that need to be done
  for (job in jobs_to_do) {
    
    # Call the download function, returns TRUE or FALSE
    success <- download_era5_data(
      year              = job$year,
      variables         = job$variables,
      dataset           = job$dataset,
      dataset_type_name = job$dataset_type_name,
      pressure_levels   = job$pressure_levels,
      output_dir        = output_dir,
      area              = bounding_box
    )
    
    # If the download failed, add the job to the list of failures
    if (!success) {
      failed_jobs_this_round <- append(failed_jobs_this_round, list(job))
    }
    
    # Add a small delay between requests to be nice to the server
    Sys.sleep(10) # Pause for 10 seconds
  }
  
  # After trying all jobs, update the list for the next round
  jobs_to_do <- failed_jobs_this_round
  retry_count <- retry_count + 1
}

# Summary:
if (length(jobs_to_do) == 0) {
  cat("Success! All jobs were completed successfully.\n")
} else {
  cat("The following", length(jobs_to_do), "jobs failed after", max_retries, "retries:\n")
  for (failed_job in jobs_to_do) {
    cat("  - Year:", failed_job$year, "| Batch:", failed_job$dataset_type_name, "\n")
  }
}