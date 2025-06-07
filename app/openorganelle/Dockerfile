FROM python:3.10-slim

# Optional system deps for scientific packages
RUN apt-get update && apt-get install -y \
    build-essential \
    libz-dev \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all Python files and modules
COPY *.py .
COPY __init__.py .
COPY helpers.py .
COPY processing.py .

CMD ["python", "main.py"]