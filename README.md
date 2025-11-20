# ERA5 Weather Annotation for GPS Tracking Data

This repository contains R scripts to download ERA5 reanalysis weather data from the Copernicus Climate Data Store (CDS) and annotate GPS tracking data with environmental variables. Dwnload ERA5 variables and precisely link them to your point-coordinate data (lat, lon, time) to bridge your measurements with detailed historical atmospheric conditions (point coordinates).

This version is based on the work by [Kamran Safi](https://github.com/kamransafi) and has been modified to work directly with **plain CSV files / Dataframes** and work with large datasets and larfa ERA5 datafiles. It **does not** require `move2` or `track` objects. It utilizes `data.table`, `terra`, and `future.apply` for fast, memory-efficient, parallel processing of large datasets.

## ⚠️ Current Limitations

*   **Yearly Files Only:** The annotation logic is currently hard-coded to process files named with a specific yearly pattern (e.g., `..._2023.nc`).
*   **No ERA5-Land Support:** Because ERA5-Land data is typically downloaded/provided in **monthly** batches, this script **will not** currently work with ERA5-Land data. It is optimized for ERA5 Single Levels or Pressure Levels downloaded in yearly chunks.

## Features

*   **CSV/Dataframe Native:** Works with standard tracking data formats (CSV).
*   **Parallel Processing:** Uses `future.apply` to process multiple weather files simultaneously.
*   **Memory Efficient:** Implements chunking (processing data in batches) and temporary file storage to prevent RAM overload when working with large rasters.
*   **Resume Capability:** Both the download and annotation scripts check for existing output files and skip them, allowing you to interrupt and resume the workflow without losing progress.
*   **3D + Time Interpolation:** Interpolates weather data bilinearly in space and linearly in time to match exact GPS timestamps.

## Prerequisites

### 1. Copernicus CDS Account
You must register with the [Copernicus Climate Data Store (CDS)](https://cds.climate.copernicus.eu) and obtain your **API Key**.

### 2. R Dependencies
Ensure you have the following packages installed:

```r
install.packages(c("dplyr", "data.table", "future.apply", "lubridate", "terra", "ecmwfr", "crayon"))
```

## File Structure

The workflow consists of three main scripts:

1.  **`function_lib.r`**: Contains all helper functions for downloading, extracting, and interpolating data.
2.  **`request_ERA5.r`**: Handles the API requests to download ERA5 NetCDF (`.nc`) files.
3.  **`annotation_ERA5.r`**: Matches the downloaded weather data to your GPS tracking CSV.

## Usage

### Step 1: Prepare Your Data
Your tracking data should be a CSV file containing, at a minimum, columns for:
*   **ID** (e.g., `individual_local_identifier`)
*   **Timestamp** (e.g., `timestamp`)
*   **Longitude** (e.g., `location_long`)
*   **Latitude** (e.g., `location_lat`)

### Step 2: Setup Functions
Open `function_lib.r`. You generally do not need to edit this, but ensure it is saved in a known location as the other scripts will source it.

### Step 3: Download Weather Data
Open `request_ERA5.r`:
1.  **Set Paths**: Update `setwd()` and the `source()` path to point to your `function_lib.r`.
2.  **API Key**: Input your CDS API key in `wf_set_key()`.
3.  **Output**: Define your `output_dir` where `.nc` files will be saved.
4.  **Configure Jobs**: Edit `years_to_download` and the `all_jobs` list to select the variables you need (e.g., Wind, Temperature, Precipitation).

Run the script. It will download files named in the format `ERA5_[type]_[year].nc`.

### Step 4: Annotate GPS Data
Open `annotation_ERA5.r`:
1.  **Set Paths**: Update paths for `track_data` (your CSV), `era5_dir` (where you downloaded files), and `source(...)`.
2.  **Column Mapping**: Inside the `extract_locations_and_era5_data` function call, ensure column names match your CSV:
    ```r
    eventCol = "individual_local_identifier",  # Your ID column
    coordsCol = c("location_long", "location_lat") # Your Lon/Lat columns
    ```
3.  **Parallel Settings**: Adjust `workers = 8` based on your computer's CPU cores.

Run the script. It will:
1.  Create an `intermediate_dir`.
2.  Process files in parallel, saving `.rds` chunks.
3.  Combine all chunks into a `final.csv`.

## Logic Overview

### Download Logic
The download script loops through years and variable sets. It uses a `while` loop with retries to handle server timeouts or API errors. If a file already exists in the folder, it is skipped.

### Annotation Logic
1.  **Extract**: Loads a specific year's ERA5 file. Filters the GPS data to include only points from that year.
2.  **Chunking**: To save memory, the code splits the GPS points into chunks (default 5000 points).
3.  **Space Interpolation**: Uses `terra::extract` (bilinear) to get weather values for the coordinates.
4.  **Time Interpolation**: Calculates the exact weather value for the specific GPS timestamp by interpolating between the hourly ERA5 data steps.
5.  **Save**: Intermediate results are saved immediately to disk to prevent data loss if the script crashes.

## Credits & Attribution

This repository is a modified version of the work by [Kamran Safi](https://github.com/kamransafi).
The original repository can be found here: **[ERA5_move2](https://github.com/kamransafi/ERA5_move2)**.

**Modifications in this version:**
*   Removed dependencies on `move2` and `sf` objects in favor of `data.table` and standard dataframes.
*   Optimized memory management using manual chunking during the `terra::extract` process.
*   Added `future.apply` implementation for multisession parallel processing.
*   Restructured error handling for batch processing.

## License

Please refer to the original repository for license details regarding the underlying logic. Ensure you comply with the **Copernicus Climate Data Store (CDS)** terms of use when publishing data derived from ERA5.
