# Use a slim Python base image
FROM python:3.11-slim

# -------------------------
# 1. System dependencies
# -------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    openssh-client \
    rsync \
    zip \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# -------------------------
# 2. Create non-root user
# -------------------------
RUN useradd -m -s /bin/bash appuser

# -------------------------
# 3. Prepare SSH directory
# -------------------------
RUN mkdir -p /home/appuser/.ssh && \
    chmod 700 /home/appuser/.ssh && \
    chown -R appuser:appuser /home/appuser/.ssh

# -------------------------
# 4. Preload known_hosts
# -------------------------
# SAFE:
# - stores ONLY public host keys
# - avoids interactive SSH prompts
# - no secrets baked into image
RUN ssh-keyscan \
    spark-master \
    spark-worker-1 \
    spark-worker-2 \
    spark-worker-3 \
    >> /home/appuser/.ssh/known_hosts

RUN chmod 644 /home/appuser/.ssh/known_hosts && \
    chown appuser:appuser /home/appuser/.ssh/known_hosts

# Create and set work directory
WORKDIR /app

# Install Python dependencies (if needed)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# ---- copy source code
COPY dao/ /app/dao/
COPY SparkJob.py /app/

# Build dao.zip inside image
RUN zip -r /app/dao.zip /app/dao

RUN chown -R appuser:appuser /app

# -------------------------
# 7. Environment
# -------------------------
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PYTHONPATH=/app
ENV SPARK_PYTHON=python3
ENV SPARK_DRIVER_PYTHON=python3

# Ship dao.zip to YARN executors automatically
ENV PYSPARK_SUBMIT_ARGS="--py-files /app/dao.zip pyspark-shell"

# Copy application files
COPY LoadData.py .
COPY entrypoint.sh .

# Make the script executable
RUN chmod +x entrypoint.sh

# -------------------------
# 8. Drop privileges
# -------------------------
USER appuser

# Set entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
