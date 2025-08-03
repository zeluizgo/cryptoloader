#!/bin/bash

# Make sure we exit if any command fails
set -e

echo "[entrypoint] Starting periodic ETL execution (every 15 minutes)..."

# Infinite loop to run every 15 minutes
while true; do
    echo "[entrypoint] Running ETL job at $(date)..."
    python3 /app/LoadData.py

    echo "[entrypoint] Sleeping for 15 minutes..."
    sleep 14400  # 240 minutes (4h) in seconds
done
