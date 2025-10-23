# Start from official Python image

FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Copy dependency file first (Docker layer caching optimization)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY generate_data.py .

# Create output directory
RUN mkdir /output

# Run the script when container starts
CMD ["python", "generate_data.py"]