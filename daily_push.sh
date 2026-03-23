#!/bin/bash
cd /home/ubuntu/Polymarket-orderbooks

# Fill outcomes for settled windows
/home/ubuntu/Polymarket-orderbooks/.venv/bin/python fill_outcomes.py

# Copy today's orderbook CSV
/home/ubuntu/Polymarket-orderbooks/.venv/bin/python export_csv.py

# Stage and push
git add csv_data/ polymarket_collector/ export_csv.py fill_outcomes.py daily_push.sh .gitignore
git diff --cached --quiet && exit 0  # nothing to commit
git commit -m "Data update $(date -u +'%Y-%m-%d %H:%M') UTC"
git push origin HEAD:main
