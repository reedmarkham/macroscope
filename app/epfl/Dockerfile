FROM python:3.10-slim

# Install system dependencies (optional for debugging TIFFs)
RUN apt-get update && apt-get install -y \
    build-essential \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Create working directory
WORKDIR /app

# Copy dependency list and install packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy script into container
COPY . .

# Default command (can be overridden)
CMD ["python", "main.py"]
