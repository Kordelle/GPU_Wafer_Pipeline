# Start from official Python image

FROM python:3.11-slim

# Install system dependencies for Snappy compression
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory inside container
WORKDIR /app

# Copy dependency file first (Docker layer caching optimization)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY generate_data_enhanced.py .
COPY explore_distributions.py .
COPY upload_to_minio.py .
COPY kafka_producer.py .

# Create output directory
RUN mkdir /output


CMD ["python", "generate_data_enhanced.py", "--mode", "batch", "--records", "1000"]

