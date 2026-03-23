#!/bin/bash
cd /home/ubuntu/Polymarket-orderbooks

# Export all parquet data into daily CSVs
/home/ubuntu/Polymarket-orderbooks/.venv/bin/python export_csv.py

# Stage CSVs and scripts
git add csv_data/ polymarket_collector/ export_csv.py export_backtest.py collector.py requirements.txt .gitignore daily_push.sh
git commit -m "Daily data update $(date -u +%Y-%m-%d)"
git push origin HEAD:main
