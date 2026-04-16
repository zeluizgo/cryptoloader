#!/bin/bash

# Make sure we exit if any command fails
#set -e

# -------------------------
# Read Docker secrets
# -------------------------
if [ -f /run/secrets/rabbitmq-user ]; then
    export RABBITMQ_USER=$(cat /run/secrets/rabbitmq-user)
fi
if [ -f /run/secrets/rabbitmq-password ]; then
    export RABBITMQ_PASSWORD=$(cat /run/secrets/rabbitmq-password)
fi

echo "[entrypoint] Services will start at 01:00 AM..."

# -------------------------
# Wait until 01:00 AM
# -------------------------
wait_until_1am() {
    now=$(date +%s)
    target=$(date -d "today 01:00" +%s)
    if [ "$now" -ge "$target" ]; then
        target=$(date -d "tomorrow 01:00" +%s)
    fi
    sleep_seconds=$((target - now))
    echo "[entrypoint] Sleeping ${sleep_seconds}s until 01:00 AM ($(date -d @$target))..."
    sleep "$sleep_seconds"
}

wait_until_1am

# -------------------------
# 4. Preload known_hosts
# -------------------------
# SAFE:
# - stores ONLY public host keys
# - avoids interactive SSH prompts
# - no secrets baked into image
mkdir -p /tmp/ssh
ssh-keyscan -H \
    spark-master \
    spark-worker-1 \
    spark-worker-2 \
    spark-worker-3 \
    > /tmp/ssh/known_hosts 2>/dev/null
export CLUSTER_KNOWN_HOSTS=/tmp/ssh/known_hosts

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

