import sqlite3
import pandas as pd
import os

class MSDSqliteIntegrator:
    def __init__(self, config):
        self.config = config

    def integrate(self):
        if os.path.exists(self.config.csv_name):
            print(f"File '{self.config.csv_name}' already exists. Skipping SQLite integration.")
            print("Please remove the file if you want to rerun MSD_SQLite_Integrator.py\n")
            return
        
        print("--- Start integrating Million_Song_Dataset.csv with SQLite_DB/*.db ---")
        
        conn_meta = sqlite3.connect(self.config.meta_db)
        conn_term = sqlite3.connect(self.config.term_db)
        conn_sim  = sqlite3.connect(self.config.sim_db)

        df_meta = pd.read_sql_query("SELECT * FROM songs", conn_meta)
        df_term = pd.read_sql_query("SELECT * FROM artist_term", conn_term)
        df_sim  = pd.read_sql_query("SELECT * FROM similarity", conn_sim)

        df_term = df_term.groupby('artist_id')['term'].apply(lambda x: ', '.join(x.astype(str))).reset_index()
        df_sim = df_sim.groupby('target')['similar'].apply(lambda x: ', '.join(x.astype(str))).reset_index()

        df_meta_term = pd.merge(df_meta, df_term, on='artist_id', how='left')
        df_meta_term_sim = pd.merge(df_meta_term, df_sim, left_on='artist_id', right_on='target', how='left')

        df_meta_term_sim = df_meta_term_sim.drop(columns=['target'])    # artist_similarity.db 的 target 其實就是 artist_id，不用重複列出
        df_meta_term_sim.to_csv(self.config.csv_name, index=False, encoding='utf-8-sig')

        print("--- Complete integrating Million_Song_Dataset.csv with SQLite_DB/*.db ---")
        print(f"Number of rows output: {len(df_meta_term_sim)}\n")

        conn_meta.close()
        conn_term.close()
        conn_sim.close()
