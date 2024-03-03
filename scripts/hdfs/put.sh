#!/bin/bash

echo "Loading data into HDFS"

docker exec -it namenode bash -c "hadoop fs -mkdir /data"
docker exec -it namenode bash -c "hadoop fs -put /data/itineraries_sample.csv /data/"
docker exec -it namenode bash -c "hadoop fs -put /data/airports.csv /data/"
docker exec -it namenode bash -c "hadoop fs -ls /data"

echo "Data loaded into HDFS"