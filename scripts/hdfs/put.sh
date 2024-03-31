#!/bin/bash

echo "Loading data into HDFS"

echo "Creating data directory in HDFS"

docker exec -it namenode bash -c "hadoop fs -mkdir /data"

echo "Copying data into HDFS"

docker exec -it namenode bash -c "hadoop fs -put /data/itineraries_sample.csv /data/"

echo "Listing data in HDFS"

docker exec -it namenode bash -c "hadoop fs -put /data/airports.csv /data/"

echo "Listing data in HDFS"

docker exec -it namenode bash -c "hadoop fs -ls /data"

echo "Data loaded into HDFS"

echo "Press any key to continue"

read -n 1 -s