#!/bin/bash

docker pull alexdarancio7/stelar_image2ts:latest

input_path="s3://klms/Pilot_B/UCB2/33TUN/DATA_FUSION"
output_path="s3://klms/Pilot_B/UCB2/33TUN/TIMESERIES_EXTRACTION"
extension='TIF'
field_path="s3://klms/Pilot_B/UCB2/33TUN/FIELD_SEGMENTATION/OUTPUT/fields_2023_07_07.gpkg"
MINIO_ACCESS_KEY="NT7UkSCviiEkKBbwjCQi"
MINIO_SECRET_KEY="G2jJ8Ut0VycvOktMERClbxRd3zECVZy8HXFcdnK2"
MINIO_ENDPOINT_URL=https://stelar-klms.eu:9000

docker run -it \
--network="host" \
alexdarancio7/stelar_image2ts \
--input_path $input_path \
-x $extension \
--output_path $output_path \
--field_path $field_path \
--skip_pixel \
--MINIO_ACCESS_KEY $MINIO_ACCESS_KEY \
--MINIO_SECRET_KEY $MINIO_SECRET_KEY  \
--MINIO_ENDPOINT_URL $MINIO_ENDPOINT_URL
