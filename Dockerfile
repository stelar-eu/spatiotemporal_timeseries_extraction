# Use an official Python runtime as a parent image
FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    curl \
    jq \
    ffmpeg libsm6 libxext6 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
COPY main.py ./
COPY run.sh ./
COPY resources/ ./resources/
COPY src/ ./src/

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

# ENTRYPOINT ["python", "-u", "main.py", "resources/input.json", "resources/output.json"]

# Make run.sh executable
RUN chmod +x run.sh
ENTRYPOINT [ "./run.sh" ]