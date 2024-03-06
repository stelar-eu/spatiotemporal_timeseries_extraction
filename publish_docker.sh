#!/bin/bash

echo "Please enter the password for the docker hub account alexdarancio7: "
read -s DOCKER_PASSWORD

docker login -u alexdarancio7 -p $DOCKER_PASSWORD

docker build -t alexdarancio7/stelar_image2ts:latest .

docker push alexdarancio7/stelar_image2ts:latest