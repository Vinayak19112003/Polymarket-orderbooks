"""Backfill outcome column in orderbook CSVs based on final prices."""

import glob
import pandas as pd

csv_dir = "/home/ubuntu/Polymarket-orderbooks/data/orderbook"
files = sorted(glob.glob(f"{csv_dir}/orderbook_*.csv"))

for f in files:
    df = pd.read_csv(f, dtype={"outcome": str})
    df["outcome"] = df["outcome"].fillna("")
    changed = 0

    for slug in df["window_slug"].unique():
        mask = df["window_slug"] == slug
        window_rows = df[mask]

        # Skip if already filled
        if window_rows["outcome"].notna().all() and (window_rows["outcome"] != "").all():
            continue

        # Get last row's yes_bid to determine outcome
        last = window_rows.iloc[-1]
        yes_bid = last.get("yes_bid")
        end_ts = int(last["window_end_ts"])

        # Only fill if window has ended
        import time
        if end_ts > int(time.time()):
            continue

        if pd.notna(yes_bid):
            if yes_bid >= 0.9:
                outcome = "Up"
            elif yes_bid <= 0.1:
                outcome = "Down"
            else:
                continue  # unclear, skip
            df.loc[mask, "outcome"] = outcome
            changed += window_rows.shape[0]

    if changed > 0:
        df.to_csv(f, index=False)
        print(f"  {f}: filled {changed} rows")
    else:
        print(f"  {f}: no changes needed")
