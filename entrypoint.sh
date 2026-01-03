#!/bin/bash

# Make sure we exit if any command fails
#set -e

echo "[entrypoint] Starting periodic ETL execution (every 15 minutes)..."

# -------------------------
# 4. Preload known_hosts
# -------------------------
# SAFE:
# - stores ONLY public host keys
# - avoids interactive SSH prompts
# - no secrets baked into image
#ssh-keyscan \
#    spark-master \
#    spark-worker-1 \
#    spark-worker-2 \
#    spark-worker-3 \
#    >> /home/appuser/.ssh/known_hosts


chmod 644 /home/appuser/.ssh/known_hosts && \
    chown appuser:appuser /home/appuser/.ssh/known_hosts

# Infinite loop to run every 15 minutes
while true; do
    echo "[entrypoint] Running ETL job at $(date)..."
    python3 /app/LoadData.py

    EXIT_CODE=$?

    if [ $EXIT_CODE -ne 0 ]; then
        echo "[entrypoint] WARNING: LoadData.py exited with code $EXIT_CODE"
    else
        echo "[entrypoint] ETL job completed successfully"
    fi

    echo "[entrypoint] Sleeping for 15 minutes..."
    sleep 14400  # 240 minutes (4h) in seconds
done
