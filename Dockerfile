FROM python:3.13-slim

# Install system dependencies for psycopg2
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . .

# Start command
CMD ["sh", "-c", "cd sentimentops-backend && uvicorn app:app --host 0.0.0.0 --port $PORT"]
