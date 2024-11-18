# Use an official Python runtime as a parent image
FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    curl \
    jq \
    ffmpeg libsm6 libxext6 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
COPY image2ts_pipeline.py ./
COPY run.sh ./

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

RUN chmod +x run.sh
ENTRYPOINT ["./run.sh"]
#ENTRYPOINT ["ls", "-al"]