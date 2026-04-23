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

        # 2. 初始表設定 (使用不同的命名空間以免覆蓋之前的 axis=1 版本)
        target_base = "msd_flat_axis0_part"
        current_part = 1
        target_table = f"{target_base}{current_part}"
        
        # 刪除舊的同名暫存表
        self.cursor.execute(f"SELECT name FROM sqlite_master WHERE name LIKE '{target_base}%'")
        old_parts = [r[0] for r in self.cursor.fetchall()]
        for op in old_parts: self.cursor.execute(f"DROP TABLE {op}")
        self.conn.commit()

        print(f"  正在初始化第一個分區 {target_table}...")
        self.cursor.execute(f"CREATE TABLE {target_table} AS SELECT * FROM songs")
        self.cursor.execute(f"CREATE UNIQUE INDEX idx_{target_table}_tid ON {target_table}(track_id)")
        self.conn.commit()

        # 3. 逐一合併 (過程中不刪除欄位)
        for table in all_tables:
            print(f"  正在合併所有特徵: {table} ...")
            self.cursor.execute(f"PRAGMA table_info({table})")
            cols = [c[1] for c in self.cursor.fetchall() if c[1].lower() != 'track_id']
            
            prefix = table.replace("features_", "f_").replace("all_sample_properties", "prop")
            select_cols = ", ".join([f't."{c}" as "{prefix}_{c}"' for c in cols])
            
            success = False
            while not success:
                temp_table = f"{target_table}_new"
                self.cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
                sql = f"CREATE TABLE {temp_table} AS SELECT m.*, {select_cols} FROM {target_table} m LEFT JOIN (SELECT * FROM \"{table}\" GROUP BY track_id) t ON m.track_id = t.track_id"
                try:
                    self.cursor.execute(sql)
                    self.cursor.execute(f"DROP TABLE {target_table}")
                    self.cursor.execute(f"ALTER TABLE {temp_table} RENAME TO {target_table}")
                    self.cursor.execute(f"CREATE UNIQUE INDEX idx_{target_table}_tid ON {target_table}(track_id)")
                    self.conn.commit()
                    success = True
                except sqlite3.OperationalError as e:
                    if "too many columns" in str(e).lower():
                        current_part += 1
                        target_table = f"{target_base}{current_part}"
                        print(f"    [!] 欄位過多，開啟新分區 {target_table}")
                        self.cursor.execute(f"CREATE TABLE {target_table} AS SELECT track_id FROM songs")
                        self.cursor.execute(f"CREATE UNIQUE INDEX idx_{target_table}_tid ON {target_table}(track_id)")
                        self.conn.commit()
                    else: raise e

        # 4. 整合匯出為大表 CSV (並在此時執行 axis=0 dropna)
        print(f"\n--- 正在匯出整合大表 (執行 dropna axis=0): {self.config.flattened_output_csv_name} ---")
        active_parts = [f"{target_base}{i}" for i in range(1, current_part + 1)]

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
            total = len(track_ids)
            saved_count = 0
            
            batch_size = 1000
            for start_idx in range(0, total, batch_size):
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
                        saved_count += 1
                
                if (start_idx // batch_size) % 10 == 0:
                    print(f"    Read Process: {min(start_idx + batch_size, total)} / {total} ... (目前保留歌曲: {saved_count} 筆)", end='\r')

        print(f"\n\n--- Complete flattening and remove observations with missing values ---")
        print(f"Number of observations before flattening: {total}")
        print(f"Number of observations after flattening: {saved_count}")
        self.conn.close()
