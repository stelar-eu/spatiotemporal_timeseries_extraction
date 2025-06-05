#!/bin/bash

docker pull alexdarancio7/stelar_image2ts:latest

# Define paths relative to the script's location or a known base directory
# Assuming 'resources' is in the same directory as the script
HOST_RESOURCES_PATH="$(pwd)/resources"
CONTAINER_RESOURCES_PATH="/app/resources" # Or any path expected inside the container

# Ensure the host resources directory exists
mkdir -p "$HOST_RESOURCES_PATH"
# Ensure the expected input file exists (optional, for testing)
# touch "$HOST_RESOURCES_PATH/input_tmp.json"

# Define paths *inside* the container
input_path="${CONTAINER_RESOURCES_PATH}/input_tmp.json"
output_path="${CONTAINER_RESOURCES_PATH}/output.json"

docker run -it \
--network="host" \
-v "${HOST_RESOURCES_PATH}:${CONTAINER_RESOURCES_PATH}" \
alexdarancio7/stelar_image2ts \
"$input_path" \
"$output_path"