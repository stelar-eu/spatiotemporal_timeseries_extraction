#!/bin/bash

docker pull alexdarancio7/stelar_image2ts:latest

MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_ENDPOINT_URL=localhost:9000

ras_paths="s3://$MINIO_ENDPOINT_URL/stelar-spatiotemporal/LAI/30TYQ_LAI_2020.RAS"
rhd_paths="s3://$MINIO_ENDPOINT_URL/stelar-spatiotemporal/LAI/30TYQ_LAI_2020.RHD"
out_dir="s3://$MINIO_ENDPOINT_URL/stelar-spatiotemporal/LAI"
fields_path="s3://$MINIO_ENDPOINT_URL/stelar-spatiotemporal/fields.gpkg"

docker run -it \
--network="host" \
alexdarancio7/stelar_image2ts \
--input_path s3://localhost:9000/path/to/input_dir \
--output_path s3://localhost:9000/path/to/output_dir \
--field_path s3://localhost:9000/path/to/fields.shp \
--skippx \
--MINIO_ACCESS_KEY $MINIO_ACCESS_KEY \
--MINIO_SECRET_KEY $MINIO_SECRET_KEY 