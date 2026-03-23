"""Backfill outcome column in orderbook CSVs based on final prices."""

import glob
import time
import pandas as pd

csv_dir = "/home/ubuntu/Polymarket-orderbooks/data/orderbook"
files = sorted(glob.glob(f"{csv_dir}/orderbook_*.csv"))
now = int(time.time())

for f in files:
    df = pd.read_csv(f, dtype={"outcome": str})
    df["outcome"] = df["outcome"].fillna("")
    changed = 0

    for slug in df["window_slug"].unique():
        mask = df["window_slug"] == slug
        window_rows = df[mask]

        # Skip if already filled
        if (window_rows["outcome"] != "").all():
            continue

        # Only fill if window has ended
        end_ts = int(window_rows.iloc[-1]["window_end_ts"])
        if end_ts > now:
            continue

        # Check last row prices
        last = window_rows.iloc[-1]
        yes_bid = last.get("yes_bid")
        no_bid = last.get("no_bid")

        outcome = None
        if pd.notna(yes_bid) and yes_bid >= 0.8:
            outcome = "Up"
        elif pd.notna(no_bid) and no_bid >= 0.8:
            outcome = "Down"
        elif pd.isna(yes_bid) and pd.notna(no_bid) and no_bid > 0.5:
            outcome = "Down"
        elif pd.isna(no_bid) and pd.notna(yes_bid) and yes_bid > 0.5:
            outcome = "Up"

        if outcome:
            df.loc[mask, "outcome"] = outcome
            changed += window_rows.shape[0]
            print(f"  {slug} → {outcome}")

    if changed > 0:
        df.to_csv(f, index=False)
        print(f"  Saved {f}: filled {changed} rows")
    else:
        print(f"  {f}: no changes needed")
