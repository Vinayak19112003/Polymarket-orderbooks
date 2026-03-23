"""Export parquet data to daily CSV files, split if >90MB."""

import os
import pyarrow.dataset as ds
import pandas as pd

data_dir = "/home/ubuntu/Polymarket-orderbooks/data"
out_dir = "/home/ubuntu/Polymarket-orderbooks/csv_data"
os.makedirs(f"{out_dir}/snapshots", exist_ok=True)
os.makedirs(f"{out_dir}/ticks", exist_ok=True)

MAX_MB = 90


def export_split(df, prefix, out_path):
    """Export df, split into parts if CSV > MAX_MB."""
    tmp = f"{out_path}/{prefix}_tmp.csv"
    df.to_csv(tmp, index=False)
    size_mb = os.path.getsize(tmp) / 1024 / 1024

    if size_mb <= MAX_MB:
        final = f"{out_path}/{prefix}.csv"
        os.rename(tmp, final)
        print(f"  → {prefix}.csv ({len(df)} rows, {size_mb:.1f} MB)")
    else:
        os.remove(tmp)
        n_parts = int(size_mb // MAX_MB) + 1
        chunk_size = len(df) // n_parts + 1
        for i in range(n_parts):
            part = df.iloc[i * chunk_size:(i + 1) * chunk_size]
            if len(part) == 0:
                continue
            fname = f"{prefix}_part{i+1}.csv"
            part.to_csv(f"{out_path}/{fname}", index=False)
            part_mb = os.path.getsize(f"{out_path}/{fname}") / 1024 / 1024
            print(f"  → {fname} ({len(part)} rows, {part_mb:.1f} MB)")


# Snapshots
print("Exporting snapshots...")
snap_df = ds.dataset(f"{data_dir}/snapshots/", format="parquet").to_table().to_pandas()

for i in range(20):
    snap_df[f"bid_p{i+1}"] = snap_df["bid_prices"].apply(lambda x, i=i: x[i] if i < len(x) else None)
    snap_df[f"bid_s{i+1}"] = snap_df["bid_sizes"].apply(lambda x, i=i: x[i] if i < len(x) else None)
    snap_df[f"ask_p{i+1}"] = snap_df["ask_prices"].apply(lambda x, i=i: x[i] if i < len(x) else None)
    snap_df[f"ask_s{i+1}"] = snap_df["ask_sizes"].apply(lambda x, i=i: x[i] if i < len(x) else None)
snap_df.drop(columns=["bid_prices", "bid_sizes", "ask_prices", "ask_sizes", "date"], errors="ignore", inplace=True)
snap_df["timestamp"] = pd.to_datetime(snap_df["ts_ms"], unit="ms")
snap_df["date"] = snap_df["timestamp"].dt.strftime("%Y-%m-%d")
snap_df.sort_values(["window_slug", "outcome", "ts_ms"], inplace=True)

for date, group in snap_df.groupby("date"):
    export_split(group.drop(columns=["date"]), f"snapshots_{date}", f"{out_dir}/snapshots")

# Ticks
print("Exporting ticks...")
tick_df = ds.dataset(f"{data_dir}/ticks/", format="parquet").to_table().to_pandas()
tick_df["timestamp"] = pd.to_datetime(tick_df["ts_ms"], unit="ms")
tick_df["date"] = tick_df["timestamp"].dt.strftime("%Y-%m-%d")
tick_df.sort_values(["window_slug", "outcome", "ts_ms"], inplace=True)

for date, group in tick_df.groupby("date"):
    export_split(group.drop(columns=["date"]), f"ticks_{date}", f"{out_dir}/ticks")

print("Done.")
