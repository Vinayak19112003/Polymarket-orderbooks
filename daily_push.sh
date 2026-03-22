#!/bin/bash
cd /home/ubuntu/Polymarket-orderbooks

# Export all parquet data into clean CSVs
/home/ubuntu/Polymarket-orderbooks/.venv/bin/python export_csv.py

# Stage only CSVs and scripts
git add snapshots.csv ticks.csv export_csv.py export_backtest.py polymarket_collector/ collector.py requirements.txt .gitignore
git commit -m "Daily data update $(date -u +%Y-%m-%d)"
git push origin HEAD:main
