import sqlite3
import pandas as pd

conn_meta = sqlite3.connect('SQLite_DB/track_metadata.db')
conn_term = sqlite3.connect('SQLite_DB/artist_term.db')
conn_sim  = sqlite3.connect('SQLite_DB/artist_similarity.db')

df_meta = pd.read_sql_query("SELECT * FROM songs", conn_meta)
df_term = pd.read_sql_query("SELECT * FROM artist_term", conn_term)
df_sim  = pd.read_sql_query("SELECT * FROM similarity", conn_sim)

df_term = df_term.groupby('artist_id')['term'].apply(lambda x: ', '.join(x.astype(str))).reset_index()
df_sim = df_sim.groupby('target')['similar'].apply(lambda x: ', '.join(x.astype(str))).reset_index()

df_meta_term = pd.merge(df_meta, df_term, on='artist_id', how='left')
df_meta_term_sim = pd.merge(df_meta_term, df_sim, left_on='artist_id', right_on='target', how='left')

df_meta_term_sim = df_meta_term_sim.drop(columns=['target'])    # artist_similarity.db 的 target 其實就是 artist_id，不用重複列出
output_filename = "Million_Song_Dataset.csv"
df_meta_term_sim.to_csv(output_filename, index=False, encoding='utf-8-sig')

print(f"--- SQLite_DB 串連成功 ---")
print(f"最終資料列數 (Rows): {len(df_meta_term_sim)}")

conn_meta.close()
conn_term.close()
conn_sim.close()