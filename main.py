import os
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

    # Data Preprocessing
    preprocess = Preprocess(config)
    preprocess.remove_missing()

if __name__ == "__main__":
    main()