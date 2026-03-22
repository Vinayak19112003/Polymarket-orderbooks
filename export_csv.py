"""Export parquet data to CSV files."""

import sys
import pyarrow.dataset as ds
import pandas as pd

data_dir = "/home/ubuntu/Polymarket-orderbooks/data"
out_dir = "/home/ubuntu/Polymarket-orderbooks"

# Snapshots
print("Exporting snapshots...")
snap_df = ds.dataset(f"{data_dir}/snapshots/", format="parquet").to_table().to_pandas()

# Flatten bid/ask lists into columns
for i in range(20):
    snap_df[f"bid_p{i+1}"] = snap_df["bid_prices"].apply(lambda x, i=i: x[i] if i < len(x) else None)
    snap_df[f"bid_s{i+1}"] = snap_df["bid_sizes"].apply(lambda x, i=i: x[i] if i < len(x) else None)
    snap_df[f"ask_p{i+1}"] = snap_df["ask_prices"].apply(lambda x, i=i: x[i] if i < len(x) else None)
    snap_df[f"ask_s{i+1}"] = snap_df["ask_sizes"].apply(lambda x, i=i: x[i] if i < len(x) else None)
snap_df.drop(columns=["bid_prices", "bid_sizes", "ask_prices", "ask_sizes", "date"], errors="ignore", inplace=True)
snap_df["timestamp"] = pd.to_datetime(snap_df["ts_ms"], unit="ms")
snap_df.sort_values(["window_slug", "outcome", "ts_ms"], inplace=True)
snap_df.to_csv(f"{out_dir}/snapshots.csv", index=False)
print(f"  → snapshots.csv ({len(snap_df)} rows)")

# Ticks
print("Exporting ticks...")
tick_df = ds.dataset(f"{data_dir}/ticks/", format="parquet").to_table().to_pandas()
tick_df["timestamp"] = pd.to_datetime(tick_df["ts_ms"], unit="ms")
tick_df.sort_values(["window_slug", "outcome", "ts_ms"], inplace=True)
tick_df.to_csv(f"{out_dir}/ticks.csv", index=False)
print(f"  → ticks.csv ({len(tick_df)} rows)")

print("Done.")
