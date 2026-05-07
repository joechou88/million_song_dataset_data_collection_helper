## Data Source

### Million Song Dataset: h5
- Download .h5 file from http://millionsongdataset.com/pages/getting-dataset/ and put them under `h5_DB` folder
  <img width="473" height="314" alt="image" src="https://github.com/user-attachments/assets/9ffb8078-7b2a-426e-a7df-a38bc0edb975" />

### Million Song Dataset: SQLite
- Download .db files from http://millionsongdataset.com/pages/getting-dataset/ and put them under `SQLite_DB` folder
  <img width="478" height="317" alt="image" src="https://github.com/user-attachments/assets/3445ad94-5661-4b56-ac4b-5ea52261e6cd" />


### Academic Institution: .arff
- Download .arff files from https://www.ifs.tuwien.ac.at/mir/msd/download.html, unzip and put them under `Million_Song_Dataset_Benchmarks` folder
  <img width="1253" height="368" alt="image" src="https://github.com/user-attachments/assets/69baa055-813e-40cf-87b8-d424f6f697c1" />

---
## Python Scripts

#### 1-1. Data Integration (`MSD_h5_Integrator.py`)

- **Objective**: fetch and save 53 features from `msd_summary_file.h5`
- **Key Functions**:
  - Fetch data from 3 primary groups in `msd_summary_file.h5`: analysis, metadata, and musicbrainz
  - Remove following columns in advance:
    - genre: all missing values
    - analyzer_version: all missing values
    - idx_artist_terms: replace with `term` from `artist_term.db` later
    - idx_similar_artists: replace with `similar` from `artist_similarity.db` later
- **Output**: `msd_summary_file.csv`

#### 1-2. Data Integration (`MSD_SQLite_Integrator.py`)

- **Objective**: merge csv dataset from 3 SQLite databases:
  - `track_metadata.db`
  - `artist_term.db`
  - `artist_similarity.db`
- **Key Functions**:
  - Merge `songs` with `artist_term` using `artist_id` as the primary key
  - Then merge with `artist_similarity` using `artist_id`
- **Output**: `Million_Song_Dataset.csv` with single table

We then execute `merge_h5_with_SQLite.py` to merge the 2 csv files above.

#### 1-3. Data Integration (`MSD_Arff_Integrator.py`)

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

- **Objective**: Transform `flattened_MSD_with_all_features.csv` into a clean, high-quality dataset optimized for machine learning.
- **Key Features**:
    - Missing values imputation: Implement MICE to capture complex relationships between variables and fill gaps.
    - Categorical variables encoding: Use One-hot Encoding to transform the `term` column into multiple binary features.
    - Remove outliers: Use Isolation Forest to prune 5% of the data as outliers, preventing extreme values from skewing regression coefficients.
    - Continuous variables scaling: Applies Robust Scaling to center the non-binary numeric features and scales them based on the Interquartile Range (IQR)
    - Mitigate multicollinearity: Use Adaptive Elastic Net to effectively selects the most significant variables
- **Output**: 
    - Intermediate: Multiple .pkl files for each stage of preprocessing.
    - Final: `MSD_with_all_features_preprocessed.csv`
