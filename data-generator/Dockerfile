# Start from official Python image

FROM python:3.11-slim

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

# Create output directory
RUN mkdir /output


CMD ["python", "generate_data_enhanced.py", "--mode", "batch", "--records", "1000"]

