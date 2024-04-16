#!/bin/bash

docker pull alexdarancio7/stelar_image2ts:latest

MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_ENDPOINT_URL=http://localhost:9000

input_path="s3://stelar-spatiotemporal/LAI_small"
output_path="s3://stelar-spatiotemporal"
fields_path="s3://stelar-spatiotemporal/fields_2020_07_27.gpkg"

docker run -it \
--network="host" \
alexdarancio7/stelar_image2ts \
--input_path $input_path \
--output_path $output_path \
--field_path $fields_path \
--MINIO_ACCESS_KEY $MINIO_ACCESS_KEY \
--MINIO_SECRET_KEY $MINIO_SECRET_KEY  \
--MINIO_ENDPOINT_URL $MINIO_ENDPOINT_URL
