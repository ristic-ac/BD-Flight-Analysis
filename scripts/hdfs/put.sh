#!/bin/bash

echo "Loading data into HDFS"

echo "Creating data directory in HDFS"

docker exec -it namenode bash -c "hadoop fs -mkdir /data"

echo "Copying data into HDFS"

files=(
    "/data/itineraries_sample.csv"
    "/data/airports.csv"
    "/data/airport_coords.csv"
)

for file in "${files[@]}"; do
    echo "Copying $file into HDFS"
    docker exec -it namenode bash -c "hadoop fs -put $file /data/"
done

echo "Data copied into HDFS"

echo "Listing data in HDFS"

docker exec -it namenode bash -c "hadoop fs -ls /data"

echo "Data loaded into HDFS"

echo "Press any key to continue"

read -n 1 -s