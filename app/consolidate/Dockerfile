# Use minimal Python base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy only the script (no requirements.txt needed)
COPY main.py .

# Set the default entrypoint
ENTRYPOINT ["python", "main.py"]
