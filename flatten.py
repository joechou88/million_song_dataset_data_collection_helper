import csv
import sqlite3
import os

class Flatten:
    def __init__(self, config):
        self.config = config

    def flatten(self):
        if os.path.exists(self.config.flattened_output_csv_path):
            print(f"File '{self.config.flattened_output_csv_name}' already exists. Skipping flattening.")
            print(f"Please remove the file manually if you want to rerun flatten.py\n")
            return

        self.conn = sqlite3.connect(self.config.db_path)
        self.cursor = self.conn.cursor()

        print(f"--- Start flattening and remove observations with missing values ---")

        self.cursor.execute("PRAGMA journal_mode = OFF")
        self.cursor.execute("PRAGMA synchronous = OFF")
        self.cursor.execute("PRAGMA temp_store = FILE")
        self.cursor.execute("PRAGMA cache_size = -2000000")

        # Delete tmp table generated from last time
        merged_table_base_name = "merged_partition"
        self.cursor.execute(f"SELECT name FROM sqlite_master WHERE name LIKE '{merged_table_base_name}%'")
        outdated_merged_table = [r[0] for r in self.cursor.fetchall()]
        for table in outdated_merged_table:
            self.cursor.execute(f"DROP TABLE IF EXISTS {table}")
        self.conn.commit()

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'songs'")
        all_tables = [t[0] for t in self.cursor.fetchall()]

        if not all_tables:
            print("[DBNotFound]: MSD_with_all_features.db is not ready. Please exectue MSD_Dataset_Integrator.py first.")
            return
        print(f"  Number of tables in MSD_with_all_features.db ready to be flattened: {len(all_tables)}")

        # Since SQLite has a default column limit of 2000, which is defined at compile time and cannot be easily modified at runtime, we partition into multiple tables to bypass the limit
        merged_table_id = 1
        merged_table = f"{merged_table_base_name}{merged_table_id}"

        print(f"  Processing SQLite table {merged_table}...")

        self.cursor.execute("PRAGMA table_info(songs)")
        cols = [c[1] for c in self.cursor.fetchall() if c[1] != "track_id"]
        exclude_missing_values_in_songs = " AND ".join([f'"{col}" IS NOT NULL' for col in cols])
        self.cursor.execute(f"CREATE TABLE {merged_table} AS SELECT * FROM songs WHERE track_id IS NOT NULL AND {exclude_missing_values_in_songs}")
        self.cursor.execute(f"CREATE UNIQUE INDEX idx_{merged_table}_tid ON {merged_table}(track_id)")
        self.conn.commit()

        for table in all_tables:
            print(f"  Merging features from table {table} ...")
            self.cursor.execute(f"PRAGMA table_info({table})")
            cols = [c[1] for c in self.cursor.fetchall() if c[1].lower() != 'track_id']
            select_cols = ", ".join([f't."{c}"' for c in cols])
            
            success = False
            while not success:
                temp_table = f"{merged_table}_tmp"
                self.cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
                sql = f"CREATE TABLE {temp_table} AS SELECT m.*, {select_cols} FROM {merged_table} m LEFT JOIN (SELECT * FROM \"{table}\" GROUP BY track_id) t ON m.track_id = t.track_id"
                try:
                    self.cursor.execute(sql)
                    self.cursor.execute(f"DROP TABLE {merged_table}")
                    self.cursor.execute(f"ALTER TABLE {temp_table} RENAME TO {merged_table}")
                    self.cursor.execute(f"CREATE UNIQUE INDEX idx_{merged_table}_tid ON {merged_table}(track_id)")
                    self.conn.commit()
                    success = True
                except sqlite3.OperationalError as e:
                    if "too many columns" in str(e).lower():
                        merged_table_id += 1
                        merged_table = f"{merged_table_base_name}{merged_table_id}"
                        print(f"    Column limit 2000 reached. Start a new partition table {merged_table}")
                        self.cursor.execute(f"CREATE TABLE {merged_table} AS SELECT track_id FROM songs")
                        self.cursor.execute(f"CREATE UNIQUE INDEX idx_{merged_table}_tid ON {merged_table}(track_id)")
                        self.conn.commit()
                    else: raise e

        print(f"\n--- Start exporting to csv: {self.config.flattened_output_csv_name} ---")
        active_parts = [f"{merged_table_base_name}{i}" for i in range(1, merged_table_id + 1)]

        all_headers = []
        for ptable in active_parts:
            self.cursor.execute(f"PRAGMA table_info({ptable})")
            cols = [c[1] for c in self.cursor.fetchall()]
            ordered = ['track_id'] + [c for c in cols if c != 'track_id']
            if not all_headers: all_headers = ordered
            else: all_headers += [c for c in ordered if c != 'track_id']

        with open(self.config.flattened_output_csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(all_headers)
            
            self.cursor.execute(f"SELECT track_id FROM songs WHERE track_id IS NOT NULL AND {exclude_missing_values_in_songs}")
            track_ids = [r[0] for r in self.cursor.fetchall()]
            total_observations_in_SQLite = len(track_ids)
            total_observations_in_CSV = 0
            
            batch_size = 1000
            for start_idx in range(0, total_observations_in_SQLite, batch_size):
                batch_ids = track_ids[start_idx : start_idx + batch_size]
                id_placeholder = ",".join(["?"] * len(batch_ids))
                batch_rows = {tid: [] for tid in batch_ids}
                for idx_p, ptable in enumerate(active_parts):
                    self.cursor.execute(f"PRAGMA table_info({ptable})")
                    ptable_cols = [c[1] for c in self.cursor.fetchall()]
                    non_tid = ", ".join([f'"{c}"' for c in ptable_cols if c != 'track_id'])
                    col_select = f'track_id, {non_tid}' if non_tid else 'track_id'
                    self.cursor.execute(f"SELECT {col_select} FROM {ptable} WHERE track_id IN ({id_placeholder})", batch_ids)
                    res_dict = {r[0]: r[1:] for r in self.cursor.fetchall()}
                    for tid in batch_ids:
                        data = list(res_dict.get(tid, []))
                        if idx_p == 0: batch_rows[tid] = [tid] + data
                        else: batch_rows[tid] += data
                
                for tid in batch_ids:
                    row = batch_rows[tid]
                    writer.writerow(row)
                    total_observations_in_CSV += 1
                
                if (start_idx // batch_size) % 10 == 0:
                    print(f"    Read Process: {min(start_idx + batch_size, total_observations_in_SQLite)} / {total_observations_in_SQLite} ... ({total_observations_in_CSV} of observations have been written in CSV)", end='\r')

        print(f"\n\n--- Complete flattening and written SQLite into CSV ---")
        print(f"Number of observations originally in SQLite: {total_observations_in_SQLite}")
        print(f"Number of observations written into CSV: {total_observations_in_CSV}\n")
        self.conn.close()
