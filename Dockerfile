FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Don't copy the code - we'll mount it instead
# COPY . .

RUN mkdir -p /data

ENV DATABASE_PATH=/data/marketing_data.db

# Use python -u for unbuffered output (better logging)
CMD ["python", "-u", "main.py"]