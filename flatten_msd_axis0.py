import sqlite3
import os
import csv
import time

def flatten_msd_axis0():
    workspace = "/Volumes/Crucial X10/MDS期末專案"
    db_path = os.path.join(workspace, "msd_merged.db")
    csv_output = os.path.join(workspace, "msd_flattened_axis0_full_features.csv")
    
    print(f"--- 開始處理資料平坦化 (橫向清理版 - 保留所有特徵，刪除不完整歌曲) ---")
    
    if not os.path.exists(db_path):
        print(f"錯誤：找不到資料庫 {db_path}，請先執行 rebuild_msd_database.py")
        return
        
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 優化效能設定
    cursor.execute("PRAGMA journal_mode = OFF")
    cursor.execute("PRAGMA synchronous = OFF")
    cursor.execute("PRAGMA temp_store = FILE")
    cursor.execute("PRAGMA cache_size = -2000000")
    
    # 1. 確定來源表
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE 'features_%' OR name = 'all_sample_properties')")
    all_tables = [t[0] for t in cursor.fetchall()]
    if not all_tables:
        print("錯誤：資料庫中沒有特徵表，請先重新執行 rebuild_msd_database.py 以恢復原始資料。")
        return
    print(f"  待處理表格總數: {len(all_tables)}")

    # 2. 初始表設定 (使用不同的命名空間以免覆蓋之前的 axis=1 版本)
    target_base = "msd_flat_axis0_part"
    current_part = 1
    target_table = f"{target_base}{current_part}"
    
    # 刪除舊的同名暫存表
    cursor.execute(f"SELECT name FROM sqlite_master WHERE name LIKE '{target_base}%'")
    old_parts = [r[0] for r in cursor.fetchall()]
    for op in old_parts: cursor.execute(f"DROP TABLE {op}")
    conn.commit()

    print(f"  正在初始化第一個分區 {target_table}...")
    cursor.execute(f"CREATE TABLE {target_table} AS SELECT * FROM songs")
    cursor.execute(f"CREATE UNIQUE INDEX idx_{target_table}_tid ON {target_table}(track_id)")
    conn.commit()

    # 3. 逐一合併 (過程中不刪除欄位)
    for table in all_tables:
        print(f"  正在合併所有特徵: {table} ...")
        cursor.execute(f"PRAGMA table_info({table})")
        cols = [c[1] for c in cursor.fetchall() if c[1].lower() != 'track_id']
        
        prefix = table.replace("features_", "f_").replace("all_sample_properties", "prop")
        select_cols = ", ".join([f't."{c}" as "{prefix}_{c}"' for c in cols])
        
        success = False
        while not success:
            temp_table = f"{target_table}_new"
            cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
            sql = f"CREATE TABLE {temp_table} AS SELECT m.*, {select_cols} FROM {target_table} m LEFT JOIN (SELECT * FROM \"{table}\" GROUP BY track_id) t ON m.track_id = t.track_id"
            try:
                cursor.execute(sql)
                cursor.execute(f"DROP TABLE {target_table}")
                cursor.execute(f"ALTER TABLE {temp_table} RENAME TO {target_table}")
                cursor.execute(f"CREATE UNIQUE INDEX idx_{target_table}_tid ON {target_table}(track_id)")
                conn.commit()
                success = True
            except sqlite3.OperationalError as e:
                if "too many columns" in str(e).lower():
                    current_part += 1
                    target_table = f"{target_base}{current_part}"
                    print(f"    [!] 欄位過多，開啟新分區 {target_table}")
                    cursor.execute(f"CREATE TABLE {target_table} AS SELECT track_id FROM songs")
                    cursor.execute(f"CREATE UNIQUE INDEX idx_{target_table}_tid ON {target_table}(track_id)")
                    conn.commit()
                else: raise e

    # 4. 整合匯出為大表 CSV (並在此時執行 axis=0 dropna)
    print(f"\n--- 正在匯出整合大表 (執行 dropna axis=0): {os.path.basename(csv_output)} ---")
    active_parts = [f"{target_base}{i}" for i in range(1, current_part + 1)]
    
    # 取得欄位清單
    all_headers = []
    for ptable in active_parts:
        cursor.execute(f"PRAGMA table_info({ptable})")
        cols = [c[1] for c in cursor.fetchall()]
        if not all_headers: all_headers = cols
        else: all_headers += [c for c in cols if c != 'track_id']

    with open(csv_output, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(all_headers)
        
        cursor.execute("SELECT track_id FROM songs")
        track_ids = [r[0] for r in cursor.fetchall()]
        total = len(track_ids)
        saved_count = 0
        
        batch_size = 1000
        for start_idx in range(0, total, batch_size):
            batch_ids = track_ids[start_idx : start_idx + batch_size]
            id_placeholder = ",".join(["?"] * len(batch_ids))
            
            # 從各個分區抓取
            batch_rows = {tid: [] for tid in batch_ids}
            for idx_p, ptable in enumerate(active_parts):
                cursor.execute(f"SELECT * FROM {ptable} WHERE track_id IN ({id_placeholder})", batch_ids)
                res_dict = {r[0]: r[1:] for r in cursor.fetchall()}
                for tid in batch_ids:
                    data = list(res_dict.get(tid, []))
                    if idx_p == 0: batch_rows[tid] = [tid] + data
                    else: batch_rows[tid] += data
            
            # axis=0 檢查: 只要有任何一格是 None (空)，就不寫入
            for tid in batch_ids:
                row = batch_rows[tid]
                if None not in row:
                    writer.writerow(row)
                    saved_count += 1
            
            if (start_idx // batch_size) % 10 == 0:
                print(f"    讀取進度: {min(start_idx + batch_size, total)} / {total} ... (目前保留歌曲: {saved_count} 筆)", end='\r')

    print(f"\n\n--- 任務完成！ ---")
    print(f"原本歌曲總數: {total}")
    print(f"過濾 (axis=0) 後剩餘歌曲: {saved_count}")
    print(f"結果檔案: {csv_output}")
    conn.close()

if __name__ == "__main__":
    flatten_msd_axis0()
