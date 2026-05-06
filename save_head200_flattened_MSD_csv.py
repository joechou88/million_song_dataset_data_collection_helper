import pandas as pd
from config import MSDConfig

config = MSDConfig()

print(f"Reading from : {config.flattened_output_csv_name}")
df = pd.read_csv(config.flattened_output_csv_path, nrows=200)

df.to_csv(config.head_flattened_output_csv_path, index=False)

print(f"Saved        : {config.head_flattened_output_csv_name}")
print(f"Shape        : {df.shape}")
