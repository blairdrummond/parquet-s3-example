#!/bin/bash

set -e

docker run -d -p 9000:9000 minio/minio server /data

echo "Starting minio on http://0.0.0.0:9000 ..."
echo "	user:pass => minioadmin:minioadmin"
echo

echo "Waiting a minute for minio to get ready"
sleep 60
echo "Adding host localminio and bucket localminio/cars"
mc config host add --insecure localminio 'http://0.0.0.0:9000' minioadmin minioadmin
mc mb localminio/cars


wget http://www.businessandeconomics.mq.edu.au/__data/assets/file/0011/232310/car.csv
echo
echo "Downloaded car.csv dataset (4mb) locally"
