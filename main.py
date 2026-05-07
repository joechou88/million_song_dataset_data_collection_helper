import os
import pandas as pd
from config import MSDConfig
from MSD_h5_Integrator import MSDH5Integrator
from MSD_SQLite_Integrator import MSDSqliteIntegrator
from MSD_Arff_Integrator import MSDArffIntegrator
from flatten import Flatten
from preprocess import Preprocess
from merge_h5_with_SQLite import MergeH5WithSQLite

def validate_paths(config):
    required_items = {
        "h5_DB": config.h5_dir_path,
        "SQLite_DB": config.sqlite_dir_path,
        "Million_Song_Dataset_Benchmarks": config.arff_dir,
    }

    for name, path in required_items.items():
        if not os.path.exists(path):
            print(f"[FileNotFoundError] Missing '{name}' at: {path}")
            return False
    return True

def main():

    # Data Integration
    config = MSDConfig()
    if not validate_paths(config):
        return

    h5_integrator = MSDH5Integrator(config)
    h5_integrator.integrate()

    sqlite_integrator = MSDSqliteIntegrator(config)
    sqlite_integrator.integrate()

    merger = MergeH5WithSQLite(config)
    merger.merge()
    
    arff_integrator = MSDArffIntegrator(config)
    arff_integrator.integrate()  # Skip this with given MSD_with_all_features.db

    flatten = Flatten(config)
    flatten.flatten()

    # Data Preprocessing
    preprocess = Preprocess(config)
    preprocess_current_path = os.path.splitext(config.preprocessed_pkl_path)[0]
    preprocess_steps = [
        ("Outlier Detection (Isolation Forest)", "_remove_outliers.pkl", preprocess.remove_outliers),
        ("Scaling for continuous variables (Robust Scaler)", "_continuous_scaled.pkl", preprocess.scale_continuous_variables),
        ("Missing Values Imputation (MICE)", "_missing_values_imputation.pkl", preprocess.missing_values_imputation),
        ("One-hot Encoding for categorical variables (term)", "_categorical_encoded.pkl", preprocess.encode_categorical_variables),
        ("Adaptive Elastic Net", "_adaptive_elastic_net.pkl", preprocess.adaptive_elastic_net)    
    ]
    df = None
    start_step_idx = 0
    for i in range(len(preprocess_steps) - 1, -1, -1):
        check_path = preprocess_current_path + preprocess_steps[i][1]
        if os.path.exists(check_path):
            print(f"Found existing checkpoint: {check_path}. Resuming from here...")
            df = pd.read_pickle(check_path)
            start_step_idx = i + 1
            break
    if df is None:
        preprocess_read_path = config.flattened_output_csv_path
        print(f"No preprocessing checkpoints found. Loading data from: {preprocess_read_path}")
        df = pd.read_csv(preprocess_read_path)
    
    for i in range(start_step_idx, len(preprocess_steps)):
        name, pkl, function = preprocess_steps[i]
        current_save_path = preprocess_current_path + pkl
        
        print(f"--- Start {name} ---")
        df = function(df)
        df.to_pickle(current_save_path)
        print(f"--- Complete {name}. Saved as {current_save_path} ---\n")

    final_preprocessed_path = config.preprocessed_output_csv_path
    df.to_csv(final_preprocessed_path, index=False)
    print(f"--- All preprocessing steps completed. Saved as: {final_preprocessed_path} ---")

if __name__ == "__main__":
    main()