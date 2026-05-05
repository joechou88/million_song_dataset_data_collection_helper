import os

class MSDConfig:
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.abspath(__file__))

        self.h5_dir_name = "h5_DB"
        self.h5_name = "msd_summary_file.h5"
        self.h5_to_csv_name = "msd_summary_file.csv"
        self.db_dir_name = "SQLite_DB"
        self.meta_name = "track_metadata.db"
        self.term_name = "artist_term.db"
        self.sim_name  = "artist_similarity.db"
        self.db_name = "MSD_with_all_features.db"
        self.csv_name = "Million_Song_Dataset.csv"
        self.merged_csv_name = "msd_summary_file_merged.csv"
        self.arff_dir_name = "Million_Song_Dataset_Benchmarks"
        self.flattened_output_csv_name = "flattened_MSD_with_all_features.csv"
        self.preprocessed_pkl_name = "MSD_with_all_features.pkl"
        self.preprocessed_output_csv_name = "MSD_with_all_features_preprocessed.csv"

        self.h5_dir_path    = os.path.join(self.base_dir, self.h5_dir_name)
        self.sqlite_dir_path = os.path.join(self.base_dir, self.db_dir_name)
        self.h5_db_path = os.path.join(self.base_dir, self.h5_dir_name, self.h5_name)
        self.h5_to_csv_path = os.path.join(self.base_dir, self.h5_to_csv_name)
        self.meta_db_path = os.path.join(self.base_dir, self.db_dir_name, self.meta_name)
        self.term_db_path = os.path.join(self.base_dir, self.db_dir_name, self.term_name)
        self.sim_db_path  = os.path.join(self.base_dir, self.db_dir_name, self.sim_name)
        self.db_path = os.path.join(self.base_dir, self.db_name)
        self.csv_path = os.path.join(self.base_dir, self.csv_name)
        self.merged_csv_path = os.path.join(self.base_dir, self.merged_csv_name)
        self.arff_dir = os.path.join(self.base_dir, self.arff_dir_name)
        self.flattened_output_csv_path = os.path.join(self.base_dir, self.flattened_output_csv_name)
        self.preprocessed_pkl_path = os.path.join(self.base_dir, self.preprocessed_pkl_name)
        self.preprocessed_output_csv_path = os.path.join(self.base_dir, self.preprocessed_output_csv_name)
