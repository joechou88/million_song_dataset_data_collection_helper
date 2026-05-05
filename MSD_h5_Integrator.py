import h5py
import pandas as pd

file_path = 'msd_summary_file.h5'
output_csv = 'msd_summary_file.csv'
chunk_size = 100000

with h5py.File(file_path, 'r') as h5:
    total_songs = h5['metadata']['songs'].shape[0]
    print(f"開始處理... 總計 {total_songs} 筆資料")

    for start in range(0, total_songs, chunk_size):
        end = min(start + chunk_size, total_songs)

        df_a = pd.DataFrame(h5['analysis']['songs'][start:end])
        df_m = pd.DataFrame(h5['metadata']['songs'][start:end])
        df_mb = pd.DataFrame(h5['musicbrainz']['songs'][start:end])

        df_chunk = pd.concat([df_a, df_m, df_mb], axis=1)
        
        str_cols = df_chunk.select_dtypes([object]).columns
        for col in str_cols:
            df_chunk[col] = df_chunk[col].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)

        if start == 0:
            df_chunk.to_csv(output_csv, index=False, mode='w', encoding='utf-8')
        else:
            df_chunk.to_csv(output_csv, index=False, mode='a', header=False, encoding='utf-8')
            
        print(f"進度: {end} / {total_songs} 已完成")

print(f"🎉 轉換完成！檔案儲存為: {output_csv}")