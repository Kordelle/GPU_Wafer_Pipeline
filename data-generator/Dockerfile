FROM python:3.11-slim

# Install system dependencies for Snappy compression
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libsnappy-dev \
    bash \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory inside container
WORKDIR /app

# Copy dependency file first (Docker layer caching optimization)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy only production application code
COPY generate_data_enhanced.py .
COPY kafka_producer.py .
COPY kafka_consumer.py .

# Create output directory (not used in streaming mode, but keeps compatibility)
RUN mkdir -p /output

# Start both producer and consumer
CMD ["sh", "-c", "python kafka_producer.py & python kafka_consumer.py"]