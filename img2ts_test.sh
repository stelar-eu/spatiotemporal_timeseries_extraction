#!/bin/bash

docker pull alexdarancio7/stelar_image2ts:latest

MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_ENDPOINT_URL=localhost:9000

input_path="s3://$MINIO_ENDPOINT_URL/stelar-spatiotemporal/LAI"
out_dir="s3://$MINIO_ENDPOINT_URL/stelar-spatiotemporal"
fields_path="s3://$MINIO_ENDPOINT_URL/stelar-spatiotemporal/fields.gpkg"

docker run -it \
--network="host" \
alexdarancio7/stelar_image2ts \
--input_path $input_path \
--output_path $output_path \
--field_path $field_path \
--skippx \
--MINIO_ACCESS_KEY $MINIO_ACCESS_KEY \
--MINIO_SECRET_KEY $MINIO_SECRET_KEY 