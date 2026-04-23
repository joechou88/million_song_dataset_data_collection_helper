import csv
import sqlite3

class Flatten:
    def __init__(self, config):
        self.config = config
        self.conn = sqlite3.connect(self.config.db_path)
        self.cursor = self.conn.cursor()

    def flatten_and_remove_missing_values(self):       
        print(f"--- Start flattening and remove observations with missing values ---")

        self.cursor.execute("PRAGMA journal_mode = OFF")
        self.cursor.execute("PRAGMA synchronous = OFF")
        self.cursor.execute("PRAGMA temp_store = FILE")
        self.cursor.execute("PRAGMA cache_size = -2000000")

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'features_%'")
        all_tables = [t[0] for t in self.cursor.fetchall()]
        if not all_tables:
            print("[DBNotFound]: MSD_with_all_features.db is not ready. Please exectue MSD_Dataset_Integrator.py first.")
            return
        print(f"  Number of tables in MSD_with_all_features.db ready to be flattened: {len(all_tables)}")

        # Since SQLite has a default column limit of 2000, which is defined at compile time and cannot be easily modified at runtime, we partition into multiple tables to bypass the limit
        merged_table_base_name = "merged_partition"
        merged_table_id = 1
        merged_table = f"{merged_table_base_name}{merged_table_id}"
        
        # Delete tmp table generated from last time
        self.cursor.execute(f"SELECT name FROM sqlite_master WHERE name LIKE '{merged_table_base_name}%'")
        outdated_merged_table = [r[0] for r in self.cursor.fetchall()]
        for table in outdated_merged_table: self.cursor.execute(f"DROP TABLE {table}")
        self.conn.commit()

        print(f"  Processing SQLite table {merged_table}...")
        self.cursor.execute(f"CREATE TABLE {merged_table} AS SELECT * FROM songs")
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
            if not all_headers: all_headers = cols
            else: all_headers += [c for c in cols if c != 'track_id']

        with open(self.config.flattened_output_csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(all_headers)
            
            self.cursor.execute("SELECT track_id FROM songs")
            track_ids = [r[0] for r in self.cursor.fetchall()]
            total_observations = len(track_ids)
            number_of_non_missing_observations = 0
            
            batch_size = 1000
            for start_idx in range(0, total_observations, batch_size):
                batch_ids = track_ids[start_idx : start_idx + batch_size]
                id_placeholder = ",".join(["?"] * len(batch_ids))
                batch_rows = {tid: [] for tid in batch_ids}
                for idx_p, ptable in enumerate(active_parts):
                    self.cursor.execute(f"SELECT * FROM {ptable} WHERE track_id IN ({id_placeholder})", batch_ids)
                    res_dict = {r[0]: r[1:] for r in self.cursor.fetchall()}
                    for tid in batch_ids:
                        data = list(res_dict.get(tid, []))
                        if idx_p == 0: batch_rows[tid] = [tid] + data
                        else: batch_rows[tid] += data
                
                for tid in batch_ids:
                    row = batch_rows[tid]
                    if None not in row:   # Remove observations with missing values  
                        writer.writerow(row)
                        number_of_non_missing_observations += 1
                
                if (start_idx // batch_size) % 10 == 0:
                    print(f"    Read Process: {min(start_idx + batch_size, total_observations)} / {total_observations} ... (目前保留歌曲: {number_of_non_missing_observations} 筆)", end='\r')

        print(f"\n\n--- Complete flattening and remove observations with missing values ---")
        print(f"Number of observations before flattening: {total_observations}")
        print(f"Number of observations after flattening: {number_of_non_missing_observations}")
        self.conn.close()
