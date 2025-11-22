FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY store_service.py .

# Create data directory
RUN mkdir -p /data/volumes

# Expose port
EXPOSE 8000

# Run the service
CMD ["python", "store_service.py"]