# syntax=docker/dockerfile:1

FROM python:3.10-slim

WORKDIR /app

# Install git + pip dependencies
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Clone pyDM3reader and install it from setup.py
RUN git clone --branch v1.5 https://github.com/piraynal/pyDM3reader.git /tmp/pyDM3reader \
    && pip install /tmp/pyDM3reader \
    && rm -rf /tmp/pyDM3reader

COPY requirements.txt .

# Install your app dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all Python files and modules
COPY *.py .

CMD ["python", "main.py"]
