import os
import sqlite3

class MSDQueryTool:
    """高階資料查詢工具，自動處理 track_id 與 artist_id 的複雜關聯。"""
    def __init__(self, db_path):
        self.db_path = db_path
        self._table_cols = {} # table -> [cols]
        self._col_to_table = {} # col -> table (primary)
        self._refresh_metadata()

    def _refresh_metadata(self):
        if not os.path.exists(self.db_path): return
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')")
        tables = [t[0] for t in cursor.fetchall()]
        
        for t in tables:
            cursor.execute(f"PRAGMA table_info({t})")
            cols = [c[1] for c in cursor.fetchall()]
            self._table_cols[t] = cols
            
            for c in cols:
                # 排除過於通用的 ID 欄位，由工具邏輯處理關連
                if c in ['track_id', 'artist_id', 'target', 'instancename', 'msd_trackid']:
                    continue
                
                # 優先權邏輯：
                # 1. 如果是特殊欄位 (similar, mbtag)，優先對應到專屬表
                # 2. 如果是songs或是未記錄的欄位
                if c == 'similar':
                    self._col_to_table[c] = 'artist_similarity'
                elif c == 'mbtag':
                    self._col_to_table[c] = 'artist_mbtag'
                elif c == 'term':
                    self._col_to_table[c] = 'artist_term'
                elif c not in self._col_to_table or t == 'songs':
                    self._col_to_table[c] = t
        conn.close()

    def list_tables(self):
        return sorted(list(self._table_cols.keys()))

    def list_columns(self, table=None):
        if table: return self._table_cols.get(table, [])
        return sorted(list(self._col_to_table.keys()))

    def get(self, columns, limit=None):
        import pandas as pd
        if isinstance(columns, str): columns = [columns]
        
        needed_tables = set()
        selected_cols_sql = []
        
        # 決定需要的表與正確的欄位路徑
        for col in columns:
            if col == 'track_id':
                needed_tables.add('songs')
                selected_cols_sql.append('songs.track_id')
                continue
            if col == 'artist_id':
                needed_tables.add('songs')
                selected_cols_sql.append('songs.artist_id')
                continue
            if col == 'target':
                needed_tables.add('artist_similarity')
                selected_cols_sql.append('artist_similarity.target')
                continue
            if col == 'similar':
                needed_tables.add('artist_similarity')
                selected_cols_sql.append('artist_similarity.similar')
                continue

            table = self._col_to_table.get(col)
            if not table:
                print(f"警告：找不到欄位 '{col}'")
                continue
            needed_tables.add(table)
            selected_cols_sql.append(f'{table}."{col}"')

        if not needed_tables: return pd.DataFrame()

        # 核心 Join 邏輯：從 songs 出發
        base_table = 'songs'
        other_tables = needed_tables - {base_table}
        
        sql = f"SELECT {', '.join(selected_cols_sql)} FROM {base_table}"
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for t in other_tables:
            # 取得表格資訊以決定 Join Key
            cursor.execute(f"PRAGMA table_info({t})")
            t_cols = [c[1] for c in cursor.fetchall()]
            
            if 'track_id' in t_cols:
                # 一般特徵表用 track_id
                sql += f" LEFT JOIN {t} ON {base_table}.track_id = {t}.track_id"
            elif 'artist_id' in t_cols:
                # 標籤表用 artist_id
                sql += f" LEFT JOIN {t} ON {base_table}.artist_id = {t}.artist_id"
            elif 'target' in t_cols:
                # 相似度表用 target (對應 artist_id)
                sql += f" LEFT JOIN {t} ON {base_table}.artist_id = {t}.target"
            else:
                # 萬一什麼都沒有，試圖檢查 source_db 是否有 artist_id 
                print(f"警告：無法確定表格 {t} 的 Join 方式")
                
        if limit: sql += f" LIMIT {limit}"
        
        df = pd.read_sql(sql, conn)
        conn.close()
        return df

if __name__ == "__main__":
    pass
