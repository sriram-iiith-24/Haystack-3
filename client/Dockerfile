FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY client_service.py .
COPY check_status.py .

CMD ["sleep", "infinity"]