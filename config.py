import os

class MSDConfig:
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.abspath(__file__))

        self.db_dir_name = "SQLite_DB"
        self.meta_name = "track_metadata.db"
        self.term_name = "artist_term.db"
        self.sim_name  = "artist_similarity.db"
        self.db_name = "MSD_with_all_features.db"
        self.csv_name = "Million_Song_Dataset.csv"
        self.arff_dir_name = "Million_Song_Dataset_Benchmarks"
        self.flattened_output_csv_name = "flattened_MSD_with_all_features.csv"
        self.preprocessed_output_csv_name = "MSD_with_all_features_preprocessed.csv"

        self.meta_db = os.path.join(self.base_dir, self.db_dir_name, self.meta_name)
        self.term_db = os.path.join(self.base_dir, self.db_dir_name, self.term_name)
        self.sim_db  = os.path.join(self.base_dir, self.db_dir_name, self.sim_name)
        self.db_path = os.path.join(self.base_dir, self.db_name)
        self.csv_path = os.path.join(self.base_dir, self.csv_name)
        self.arff_dir = os.path.join(self.base_dir, self.arff_dir_name)
        self.flattened_output_csv_path = os.path.join(self.base_dir, self.flattened_output_csv_name)
        self.preprocessed_output_csv_path = os.path.join(self.base_dir, self.preprocessed_output_csv_name)
