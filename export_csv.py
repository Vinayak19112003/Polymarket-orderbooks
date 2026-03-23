"""Copy daily orderbook CSVs to csv_data/ for GitHub push."""

import os
import shutil
import glob

src_dir = "/home/ubuntu/Polymarket-orderbooks/data/orderbook"
out_dir = "/home/ubuntu/Polymarket-orderbooks/csv_data"
os.makedirs(out_dir, exist_ok=True)

files = sorted(glob.glob(f"{src_dir}/orderbook_*.csv"))
if not files:
    print("No orderbook CSV files found.")
else:
    for f in files:
        name = os.path.basename(f)
        dest = os.path.join(out_dir, name)
        shutil.copy2(f, dest)
        size_mb = os.path.getsize(dest) / 1024 / 1024
        with open(dest) as fh:
            rows = sum(1 for _ in fh) - 1
        print(f"  → {name} ({rows} rows, {size_mb:.1f} MB)")

print("Done.")
