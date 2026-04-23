import os

class MSDConfig:
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.abspath(__file__))

        self.db_name = "MSD_with_all_features.db"
        self.csv_name = "Million_Song_Dataset.csv"
        self.arff_dir_name = "Million_Song_Dataset_Benchmarks"
        self.flattened_output_csv_name = "flattened_MSD_with_all_features.csv"

        self.db_path = os.path.join(self.base_dir, self.db_name)
        self.csv_path = os.path.join(self.base_dir, self.csv_name)
        self.arff_dir = os.path.join(self.base_dir, self.arff_dir_name)
        self.flattened_output_csv_path = os.path.join(self.base_dir, self.flattened_output_csv_name)
