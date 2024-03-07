#!/bin/bash

. .venv/bin/activate

MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_ENDPOINT_URL=http://localhost:9000

input_path="s3://stelar-spatiotemporal/LAI_small"
out_dir="s3://stelar-spatiotemporal"

python3 image2ts_pipeline.py --input_path $input_path --output_path $out_dir --MINIO_ACCESS_KEY $MINIO_ACCESS_KEY --MINIO_SECRET_KEY $MINIO_SECRET_KEY --MINIO_ENDPOINT_URL $MINIO_ENDPOINT_URL
