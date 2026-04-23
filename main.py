import os
from config import MSDConfig
from MSD_Dataset_Integrator import MSDDatasetIntegrator
from flatten_and_remove_missing_values import Flatten

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
    
    integrator = MSDDatasetIntegrator(config)
    integrator.integrate()  # Skip this with given MSD_with_all_features.db

    flatten = Flatten(config)
    flatten.flatten_and_remove_missing_values()

    # Data Preprocessing
    

if __name__ == "__main__":
    main()