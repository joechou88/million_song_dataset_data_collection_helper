import os
import csv
import sqlite3
import glob

class MSDDatasetIntegrator:
    def __init__(self):
        self.current_dir = os.path.dirname(os.path.abspath(__file__))
        
        self.db_name = "msd_merged.db"
        self.csv_name = "Million_Song_Dataset.csv"
        self.property_name = "All_sample_properties.csv"
        self.arff_name = "Million_Song_Dataset_Benchmarks"
        self.side_db_name = "SQLite_DB"

        self.db_path = os.path.join(self.current_dir, self.db_name)
        self.csv_path = os.path.join(self.current_dir, self.csv_name)
        self.property_path = os.path.join(self.current_dir, self.property_name)
        self.arff_dir = os.path.join(self.current_dir, self.arff_name)
        self.side_db_dir = os.path.join(self.current_dir, self.side_db_name)

        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def get_arff_attributes(filepath):
        attributes = []
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line = line.strip()
                if not line: continue
                if line.lower().startswith('@attribute'):
                    parts = line.split()
                    name = parts[1].strip('"').strip("'")
                    attributes.append(name)
                if line.lower().startswith('@data'): break
        return attributes

    def import_csv_to_db(self, file_path, table_name, cursor):
        print(f"  Write csv file '{os.path.basename(file_path)}' into table '{table_name}'")
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            if f.read(1) != '\ufeff': f.seek(0)
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            cols = ", ".join([f'"{h}" TEXT' for h in headers])
            cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
            cursor.execute(f'CREATE TABLE {table_name} ({cols})')
            placeholders = ", ".join(["?"] * len(headers))
            insert_query = f'INSERT INTO {table_name} VALUES ({placeholders})'
            batch = []
            for i, row in enumerate(reader):
                batch.append(tuple(row[h] for h in headers))
                if len(batch) >= 10000:
                    cursor.executemany(insert_query, batch)
                    batch = []
                    print(f"    已插入 {i+1} 筆...", end='\r')
            if batch: cursor.executemany(insert_query, batch)
        if 'track_id' in headers:
            cursor.execute(f'CREATE INDEX "idx_{table_name}_track_id" ON {table_name} (track_id)')

    def rebuild(self):

        print(f"--- Start integrating Million Song Dataset (with Mapping) ---")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='songs'")
        if not cursor.fetchone():
            self.import_csv_to_db(self.csv_path, "songs", cursor)
        else:
            print("Table 'songs' is ready. Skip importing.")
        conn.commit()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='all_sample_properties'")
        if not cursor.fetchone():
            self.import_csv_to_db(self.property_path, "all_sample_properties", cursor)
        else:
            print("Table 'all_sample_properties' is ready. Skip importing.")
        conn.commit()

        # 3. 導入 ARFF
        arff_files = sorted(glob.glob(os.path.join(self.arff_dir, "*.arff")))
        for arff_path in arff_files:
            table_name = "features_" + os.path.splitext(os.path.basename(arff_path))[0].replace("-", "_").lower()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            if cursor.fetchone(): continue

            print(f"  正在處理特徵檔: {os.path.basename(arff_path)}...")
            attrs = self.get_arff_attributes(arff_path)
            safe_attrs = []
            found_id = False
            for a in attrs:
                if a.lower() in ['track_id', 'instancename', 'msd_trackid']:
                    safe_attrs.append('"track_id" TEXT')
                    found_id = True
                else: safe_attrs.append(f'"{a}" REAL')
            if not found_id: continue

            cursor.execute(f'CREATE TABLE {table_name} ({", ".join(safe_attrs)})')
            insert_query = f'INSERT INTO {table_name} VALUES ({", ".join(["?"] * len(attrs))})'
            with open(arff_path, 'r', encoding='utf-8', errors='ignore') as f:
                while True:
                    line = f.readline()
                    if not line or line.lower().startswith('@data'): break
                batch = []
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('%'): continue
                    if line.endswith(','): line = line[:-1]
                    row = [x.strip().strip("'").strip('"') for x in line.split(',')]
                    if len(row) >= len(attrs): batch.append(tuple(row[:len(attrs)]))
                    if len(batch) >= 10000:
                        cursor.executemany(insert_query, batch)
                        batch = []
                if batch: cursor.executemany(insert_query, batch)
            cursor.execute(f'CREATE INDEX "idx_{table_name}_track_id" ON {table_name} (track_id)')
            conn.commit()

        # 4. 側邊資料庫：直接帶著 track_id 遷移 (Mapping 重頭戲)
        print("--- 正在將藝人層級資料對齊 track_id 後存入 ---")
        side_mapping = {
            "artist_similarity.db": [("similarity", "artist_similarity", "target")],
            "artist_term.db": [("artist_mbtag", "artist_mbtag", "artist_id"), ("artist_term", "artist_term", "artist_id")]
        }

        for db_file, tables in side_mapping.items():
            src_path = os.path.join(self.side_db_dir, db_file)
            if not os.path.exists(src_path): continue
            
            cursor.execute(f"ATTACH DATABASE '{src_path}' AS source_db")
            for src_table, target_table, join_col in tables:
                cursor.execute(f"SELECT name FROM source_db.sqlite_master WHERE type='table' AND name='{src_table}'")
                if not cursor.fetchone(): continue
                
                print(f"  正在轉存 {target_table} (加入 mapping)...")
                cursor.execute(f"DROP TABLE IF EXISTS {target_table}")
                
                # 這裡執行 Mapping：將 source 表與當前 DB 的 songs 表 Join，取出 track_id
                sql = f"""
                CREATE TABLE {target_table} AS 
                SELECT s.track_id, t.* 
                FROM source_db.{src_table} t
                JOIN songs s ON t.{join_col} = s.artist_id
                """
                cursor.execute(sql)
                cursor.execute(f"CREATE INDEX idx_{target_table}_track_id ON {target_table} (track_id)")
            cursor.execute("DETACH DATABASE source_db")

        conn.commit()
        conn.close()
        print("--- 任務完成！資料表現在全部都具備 track_id 欄位 ---")

    if __name__ == "__main__":
        rebuild()
