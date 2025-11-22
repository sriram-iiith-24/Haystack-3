FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY cache_service.py .

# Expose port
EXPOSE 8100

# Run the service
CMD ["python", "cache_service.py"]