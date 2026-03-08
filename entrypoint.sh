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


#chmod 644 /home/appuser/.ssh/known_hosts && \
#    chown appuser:appuser /home/appuser/.ssh/known_hosts

# Infinite loop to run every 15 minutes


if [[ "$HOSTNAME" == "etl-firstattempt" ]]; then
    echo "[entrypoint] Running ETL job at $(date)...  on node ($HOSTNAME)"
    #echo "User is: $USER"
    #echo "User is: $(whoami)"
    python3 /app/LoadData.py

    EXIT_CODE=$?

    if [ $EXIT_CODE -ne 0 ]; then
        echo "[entrypoint] WARNING: LoadData.py exited with code $EXIT_CODE"
    else
        echo "[entrypoint] ETL job completed successfully"
    fi

    echo "[entrypoint] Finishing ETL Job at $(date). "
fi



if [[ "$HOSTNAME" == "spark-firstattempt" ]]; then
    echo "[entrypoint] Starting Spark Job node ($HOSTNAME)"
    python3 "/app/SparkJob.py" "--symbol" "0" "--exchange" "binance"

    EXIT_CODE=$?

    if [ $EXIT_CODE -ne 0 ]; then
        echo "[entrypoint] WARNING: SparkJob.py exited with code $EXIT_CODE"
    else
        echo "[entrypoint] Spark job completed successfully"
    fi
    echo "[entrypoint] Finishing Spark Job at $(date). "
fi

