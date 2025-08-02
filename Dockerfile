# Use a slim Python base image
FROM python:3.11-slim

# Create and set work directory
WORKDIR /app

# Install Python dependencies (if needed)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY LoadData.py .
COPY entrypoint.sh .

# Make the script executable
RUN chmod +x entrypoint.sh

# Create /work directory for writing
RUN mkdir -p /work

# Set entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
