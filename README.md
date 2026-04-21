## Fetch SQLite Database data to csv file
- Download .db files from http://millionsongdataset.com/pages/getting-dataset/ and put them under `SQLite_DB` folder
  
  <img width="478" height="317" alt="image" src="https://github.com/user-attachments/assets/3445ad94-5661-4b56-ac4b-5ea52261e6cd" />

- Execute `merge_song_dataset_from_SQLite.py` to get the output .csv file


## Convert .arff to .csv
- Download .arff files from https://www.ifs.tuwien.ac.at/mir/msd/download.html, unzip and put them under `Million_Song_Dataset_Benchmarks` folder
  <img width="1256" height="377" alt="image" src="https://github.com/user-attachments/assets/a0a98120-a4dc-4d07-8ee1-285e05ec9c34" />

- Execute `merge_song_dataset_from_benchmark.py` to get the output .csv file

---

### Data Integration and Preparation

The following scripts are used to transform the raw Million Song Dataset (MSD) files into a structured database and a flattened, cleaned dataset for machine learning models.

#### 1. Database Construction (`rebuild_msd_database.py`)
This script merges fragmented data sources (CSV and ARFF files) into a single SQLite database to enable efficient relational queries and feature joining.

- **Objective**: Create a unified relational database `msd_merged.db`.
- **Workflow**:
    - Imports `Million_Song_Dataset.csv` into the `songs` table (acting as the primary mapping).
    - Imports `All_sample_properties.csv` into the `all_sample_properties` table.
    - Scans the `Million_Song_Dataset_Benchmarks` folder and imports all `.arff` files as individual feature tables (prefixed with `features_`).
    - Automatically creates indices on `track_id` for all tables to ensure fast join operations.
- **Requirement**: Ensure all `.arff` files and source CSVs are in the project root or the specified subfolders.

#### 2. Dataset Flattening and Cleaning (`flatten_msd_axis0.py`)
This script flattens the multi-table database into a single CSV file while performing strict data cleaning.

- **Objective**: Produce a wide-format CSV (`msd_flattened_axis0_full_features.csv`) with no missing values.
- **Key Features**:
    - **Comprehensive Merging**: Combines all available feature tables (e.g., rhythm, timbre, chroma) and song properties into one wide row per `track_id`.
    - **Overcoming SQLite Limits**: Multi-partition logic is implemented to bypass SQLite's 2000-column limitation by creating intermediate buffer tables.
    - **Strict Axis-0 Cleaning**: Only songs that have **complete feature sets** (no `NULL` values in any column) are exported. This ensures the final dataset is ready for high-performance modeling without further imputation.
    - **Performance Optimizations**: Uses `PRAGMA` settings and batch-based row fetching to handle the massive data volume efficiently.
- **Output**: `msd_flattened_axis0_full_features.csv`
- **Result Metrics**: Upon completion, the script reports the total songs processed and the final count of songs meeting the "complete features" criteria.
  
