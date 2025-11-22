FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY replication_manager.py .

# Create data directory
RUN mkdir -p /data/replication

# Expose port
EXPOSE 9003

# Run the service
CMD ["python", "replication_manager.py"]