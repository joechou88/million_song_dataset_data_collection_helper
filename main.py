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

    preprocess_read_path = config.flattened_output_csv_path
    print(f"Loading data from: {preprocess_read_path}\n")
    df = pd.read_csv(preprocess_read_path)
    
    # Data Preprocessing
    preprocess = Preprocess(config)
    preprocess_current_path = os.path.splitext(config.preprocessed_pkl_path)[0]

    print("--- Start One-hot Encoding for categorical variables (term) ---")
    df = preprocess.encode_categorical_variables(df)
    current_path = preprocess_current_path + "_categorical_encoded.pkl"
    df.to_pickle(current_path)
    print(f"--- Complete One-hot Encoding. Saved as {current_path} ---\n")

    print("--- Start Outlier Detection (Isolation Forest) ---")
    df = preprocess.remove_outliers(df)
    current_path = preprocess_current_path + "_remove_outliers.pkl"
    df.to_pickle(current_path)
    print(f"--- Outliers removed. Saved as {current_path} ---\n")

    print("--- Start Adaptive Elastic Net to mitigate multicollinearity problem ---")
    df = preprocess.adaptive_elastic_net(df)
    current_path = preprocess_current_path + "_adaptive_elastic_net.pkl"
    df.to_pickle(current_path)
    print(f"--- Complete Adaptive Elastic Net. Final file saved as {current_path} ---\n")

    print(f"--- Start imputing missing values for {preprocess_read_path} ---")
    df = preprocess.missing_values_imputation(df)
    current_path = preprocess_current_path + "_missing_values_imputation.pkl"
    df.to_pickle(current_path)
    print(f"--- Complete imputing missing values. Saved as {current_path} ---\n")

    print("--- Start Scaling for continuous variables (Robust Scaler) ---")
    df = preprocess.scale_continuous_variables(df)
    current_path = preprocess_current_path + "_continuous_scaled.pkl"
    df.to_pickle(current_path)
    print(f"--- Complete Feature scaling with Robust Scaler. Saved as {current_path} ---\n")

    final_preprocessed_path = config.preprocessed_output_csv_path
    df.to_csv(final_preprocessed_path, index=False)
    print(f"--- All preprocessing steps completed. Saved as: {final_preprocessed_path} ---")

if __name__ == "__main__":
    main()