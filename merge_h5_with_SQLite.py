import time
import pandas as pd
from config import MSDConfig


class MergeH5WithSQLite:
    """Merges shs_perf, shs_work, term, similar from Million_Song_Dataset.csv
    into msd_summary_file.csv and writes the result to merged_csv_path."""

    COLS_TO_MERGE = ["track_id", "shs_perf", "shs_work", "term", "similar"]

    def __init__(self, config: MSDConfig):
        self.config = config

    def merge(self):
        msd     = self._load_msd_columns()
        summary = self._load_summary()
        merged  = self._join(summary, msd)
        self._save(merged)
        self._print_summary(summary, merged)

    def _load_msd_columns(self) -> pd.DataFrame:
        path = self.config.csv_path
        print(f"[1/4] Loading selected columns from '{path}' ...")
        t0 = time.time()
        df = pd.read_csv(
            path,
            usecols=self.COLS_TO_MERGE,
            encoding="utf-8",
            low_memory=False,
        )
        print(f"      Loaded {len(df):,} rows in {time.time() - t0:.1f}s")
        return df

    def _load_summary(self) -> pd.DataFrame:
        path = self.config.h5_to_csv_path
        print(f"[2/4] Loading '{path}' ...")
        t0 = time.time()
        df = pd.read_csv(
            path,
            encoding="latin1",
            low_memory=False,
        )
        print(f"      Loaded {len(df):,} rows in {time.time() - t0:.1f}s")
        return df

    def _join(self, summary: pd.DataFrame, msd: pd.DataFrame) -> pd.DataFrame:
        print("[3/4] Merging columns (left join on track_id) ...")
        t0 = time.time()
        drop_cols = [c for c in ["shs_perf", "shs_work", "term", "similar"]
                     if c in summary.columns]
        if drop_cols:
            print(f"      Dropping pre-existing columns in summary: {drop_cols}")
            summary = summary.drop(columns=drop_cols)

        merged = summary.merge(msd, on="track_id", how="left")
        print(f"      Merged shape: {merged.shape} — done in {time.time() - t0:.1f}s")
        return merged

    def _save(self, merged: pd.DataFrame):
        path = self.config.merged_csv_path
        print(f"[4/4] Saving output to '{path}' ...")
        t0 = time.time()
        merged.to_csv(path, index=False, encoding="utf-8")
        print(f"      Saved in {time.time() - t0:.1f}s")

    def _print_summary(self, summary: pd.DataFrame, merged: pd.DataFrame):
        matched = merged["shs_perf"].notna().sum()
        print("\n── Merge complete ──────────────────────────────────────────────────────")
        print(f"  Input  : {self.config.h5_to_csv_name}  "
              f"({len(summary):,} rows, {len(summary.columns)} cols)")
        print(f"  Output : {self.config.merged_csv_name}  "
              f"({len(merged):,} rows, {len(merged.columns)} cols)")
        print(f"  New columns added : shs_perf, shs_work, term, similar")
        print(f"  Rows with matched data : {matched:,} / {len(merged):,}")
