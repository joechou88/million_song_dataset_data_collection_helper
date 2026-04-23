import os
import csv
import sqlite3
import glob

class MSDDatasetIntegrator:
    def __init__(self, config):
        self.config = config
        self.conn = sqlite3.connect(self.config.db_path)
        self.cursor = self.conn.cursor()

    def get_arff_attributes(self, filepath):
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

    def import_csv_to_db(self, file_path, table_name):
        print(f"  Write csv file '{os.path.basename(file_path)}' into table '{table_name}'")
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            if f.read(1) != '\ufeff': f.seek(0)
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            cols = ", ".join([f'"{h}" TEXT' for h in headers])
            self.cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
            self.cursor.execute(f'CREATE TABLE {table_name} ({cols})')
            placeholders = ", ".join(["?"] * len(headers))
            insert_query = f'INSERT INTO {table_name} VALUES ({placeholders})'
            batch = []
            for i, row in enumerate(reader):
                batch.append(tuple(row[h] for h in headers))
                if len(batch) >= 10000:
                    self.cursor.executemany(insert_query, batch)
                    batch = []
                    print(f"    Inserted {i+1} rows...", end='\r')
            if batch: self.cursor.executemany(insert_query, batch)
        if 'track_id' in headers:
            self.cursor.execute(f'CREATE INDEX "idx_{table_name}_track_id" ON {table_name} (track_id)')

    def integrate(self):
        print(f"--- Start integrating Million Song Dataset ---")
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='songs'")
        if not self.cursor.fetchone():
            self.import_csv_to_db(self.config.csv_path, "songs")
        else:
            print("Table 'songs' is ready. Skip importing.")
        self.conn.commit()

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='all_sample_properties'")
        if not self.cursor.fetchone():
            self.import_csv_to_db(self.config.property_path, "all_sample_properties")
        else:
            print("Table 'all_sample_properties' is ready. Skip importing.")
        self.conn.commit()

        arff_files = sorted(glob.glob(os.path.join(self.config.arff_dir, "*.arff")))
        for arff_path in arff_files:
            table_name = "features_" + os.path.splitext(os.path.basename(arff_path))[0].replace("-", "_").lower()
            self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            if self.cursor.fetchone(): continue

            print(f"  Processing arff file: {os.path.basename(arff_path)}...")
            attrs = self.get_arff_attributes(arff_path)
            safe_attrs = []
            found_id = False
            for a in attrs:
                if a.lower() in ['track_id', 'instancename', 'msd_trackid']:
                    safe_attrs.append('"track_id" TEXT')
                    found_id = True
                else: safe_attrs.append(f'"{a}" REAL')
            if not found_id: continue

            self.cursor.execute(f'CREATE TABLE {table_name} ({", ".join(safe_attrs)})')
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
                        self.cursor.executemany(insert_query, batch)
                        batch = []
                if batch: self.cursor.executemany(insert_query, batch)
            self.cursor.execute(f'CREATE INDEX "idx_{table_name}_track_id" ON {table_name} (track_id)')
            self.conn.commit()

        self.conn.commit()
        self.conn.close()
        print("--- Complete integrating Million_Song_Dataset.csv with Million_Song_Dataset_Benchmarks/*.arff ---")
