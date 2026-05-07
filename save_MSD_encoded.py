import os
import pandas as pd
from config import MSDConfig

config = MSDConfig()

pkl_path = os.path.join(config.base_dir, "MSD_with_all_features_categorical_encoded.pkl")
out_path = os.path.join(config.base_dir, "MSD_with_all_features_categorical_encoded.csv")

CHUNK_SIZE = 50000

print(f"Loading : {os.path.basename(pkl_path)} ...")
df = pd.read_pickle(pkl_path)
total = len(df)
print(f"Shape   : {df.shape}")
print(f"Writing : {os.path.basename(out_path)} in chunks of {CHUNK_SIZE:,} ...")

for i, start in enumerate(range(0, total, CHUNK_SIZE)):
    chunk = df.iloc[start : start + CHUNK_SIZE]
    chunk.to_csv(out_path, index=False, mode='w' if i == 0 else 'a', header=(i == 0))
    print(f"  Written {min(start + CHUNK_SIZE, total):,} / {total:,} rows ...", end='\r')

print(f"\nSaved   : {os.path.basename(out_path)}")
print(f"Total rows written: {total:,}")
