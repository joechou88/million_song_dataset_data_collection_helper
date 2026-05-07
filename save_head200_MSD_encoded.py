import os
import pandas as pd
from config import MSDConfig

config = MSDConfig()

pkl_path = os.path.join(config.base_dir, "MSD_with_all_features_categorical_encoded.pkl")
out_path = os.path.join(config.base_dir, "head200_MSD_with_all_features_categorical_encoded.csv")

print(f"Loading : {os.path.basename(pkl_path)} ...")
df = pd.read_pickle(pkl_path)

head = df.head(200)
head.to_csv(out_path, index=False)

print(f"Saved   : {os.path.basename(out_path)}")
print(f"Shape   : {head.shape}")
