import os
import pandas as pd
import shutil
from config import MSDConfig
from MSD_SQLite_Integrator import MSDSqliteIntegrator
from MSD_Arff_Integrator import MSDArffIntegrator
from flatten import Flatten
from preprocess import Preprocess

def validate_paths(config):
    required_items = {
        "Million_Song_Dataset.csv": config.csv_path,
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
    
    sqlite_integrator = MSDSqliteIntegrator(config)
    sqlite_integrator.integrate()
    
    arff_integrator = MSDArffIntegrator(config)
    arff_integrator.integrate()  # Skip this with given MSD_with_all_features.db

    flatten = Flatten(config)
    flatten.flatten()

    current_path = config.flattened_output_csv_path
    print(f"Loading data from: {current_path}")
    df = pd.read_csv(current_path)
    # Data Preprocessing
    preprocess = Preprocess(config)
    df, current_path = preprocess.remove_missing_values(df, current_path)
    df, current_path = preprocess.encode_categorical_variables(df, current_path)
    df, current_path = preprocess.remove_outliers(df, current_path)
    df, current_path = preprocess.scale_continuous_variables(df, current_path)
    df, current_path = preprocess.adaptive_elastic_net(df, current_path)

    final_preprocessed_path = config.preprocessed_output_csv_path
    shutil.copy(current_path, final_preprocessed_path)
    print("--- All preprocessing steps completed. Saved as: {final_preprocessed_path} ---")

if __name__ == "__main__":
    main()