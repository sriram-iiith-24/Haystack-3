FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY directory_service.py .

# Create data directory
RUN mkdir -p /data/directory

# Expose port
EXPOSE 9000

# Run the service
CMD ["python", "directory_service.py"]