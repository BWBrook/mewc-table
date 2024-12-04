# Use a lightweight Python base image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the dependencies file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY src/ ./src
COPY ./src/params.yaml /app/params.yaml

# Set default environment variables
ENV PARAMS_FILE=/app/params.yaml
ENV RUN_SCRIPTS="1"
ENV WORKFLOW_MODE="manual"

# Default command to run the workflow manager
ENTRYPOINT ["python", "/app/src/common.py"]
