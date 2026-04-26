## Data Source

### Million Song Dataset: SQLite
- Download .db files from http://millionsongdataset.com/pages/getting-dataset/ and put them under `SQLite_DB` folder
  
  <img width="478" height="317" alt="image" src="https://github.com/user-attachments/assets/3445ad94-5661-4b56-ac4b-5ea52261e6cd" />


### Academic Institution: .arff
- Download .arff files from https://www.ifs.tuwien.ac.at/mir/msd/download.html, unzip and put them under `Million_Song_Dataset_Benchmarks` folder
  <img width="1256" height="377" alt="image" src="https://github.com/user-attachments/assets/a0a98120-a4dc-4d07-8ee1-285e05ec9c34" />

---
## Python Scripts

### Data Integration and Preparation

The following scripts are used to transform the raw Million Song Dataset (MSD) files into a structured database and a flattened, cleaned dataset for machine learning models.

#### 1-1. Data Integration (`MSD_SQLite_Integrator.py`)

- **Objective**: merge csv dataset from 3 SQLite databases:
  - `track_metadata.db`
  - `artist_term.db`
  - `artist_similarity.db`

- **Key Functions**:
  - Merge `songs` with `artist_term` using `artist_id` as the primary key
  - Then merge with `artist_similarity` using `artist_id`

- **Output**: `Million_Song_Dataset.csv` with single table

#### 1-2. Data Integration (`MSD_Arff_Integrator.py`)

- **Objective**: integrating `Million_Song_Dataset.csv` with `Million_Song_Dataset_Benchmarks/*.arff`.
- **Key Functions**:
    - Write `Million_Song_Dataset.csv` into the `songs` table (Primary table).
    - Write `Million_Song_Dataset_Benchmarks/*.arff` into separate feature tables (prefixed with `features_`, one table per arff file).
    - Automatically creates indices on track_id for all tables to facilitate future joins.
- **Output**: A relational database `MSD_with_all_features.db` with many tables.

#### 2. Data Flattening (`flatten.py`)

- **Objective**: Flattens the multi-table database (`MSD_with_all_features.db`) into a single table in .csv format (`flattened_MSD_with_all_features.csv`).
- **Key Features**:
    - Merge all feature tables in `MSD_with_all_features.db` using `track_id` as primary key.
    - Use partition tables to bypass SQLite’s 2000-column limitation. (TODO: can remove in the future)
- **Output**: `flattened_MSD_with_all_features.csv` with single table  

#### 3. Preprocessing (`preprocess.py`)

- **Objective**: 
- **Key Features**:
    - Remove missing values: Only songs that have **complete feature sets** are exported to csv, so that we don't need to do missing value imputation.
- **Output**: 