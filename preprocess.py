import pandas as pd
import os

class Preprocess:
    def __init__(self, config):
        self.config = config

    def remove_missing(self):
        input_path = self.config.flattened_output_csv_path

        base, ext = os.path.splitext(input_path)
        output_path = f"{base}_remove_missing_values{ext}"

        print(f"--- Start removing missing values for {input_path} ---")

        df = pd.read_csv(input_path)
        df_clean = df.dropna()
        df_clean.to_csv(output_path, index=False)

        print(
            f"--- Rows before: {len(df)}, after: {len(df_clean)} ---\n"
            f"Complete removing missing values. Saved as {output_path}\n"
        )