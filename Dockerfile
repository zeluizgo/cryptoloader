# Use a slim Python base image
FROM python:3.11-slim

# ---- system deps (zip is needed at build time)
RUN apt-get update \
 && apt-get install -y --no-install-recommends zip rsync \
 && rm -rf /var/lib/apt/lists/*

# Create and set work directory
WORKDIR /app

# Install Python dependencies (if needed)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# ---- copy source code
COPY dao/ /app/dao/
COPY SparkJob.py /app/job.py

# ---- create dao.zip inside the image
RUN zip -r /app/dao.zip /app/dao

# ---- ensure Spark sees the zip
ENV PYTHONPATH=/app
ENV SPARK_SUBMIT_OPTS="--py-files /app/dao.zip"


# Copy application files
COPY LoadData.py .
COPY entrypoint.sh .

# Make the script executable
RUN chmod +x entrypoint.sh


# Set entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
