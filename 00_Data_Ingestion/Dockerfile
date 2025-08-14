# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set environment variables to prevent buffering
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /app

# Install system dependencies if needed (e.g., for specific libraries)
# RUN apt-get update && apt-get install -y --no-install-recommends some-package && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code into the container
COPY ./src /app/src


# Default command can be overridden in docker-compose.yml
# CMD ["python", "src/some_default_script.py"]