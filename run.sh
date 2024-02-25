#!/bin/bash

echo "Running put.sh"
./scripts/hdfs/put.sh

# This doesnt actually need to be run, as the topics are created when the kafka container is started via environment variable TOPIC 
# echo "Running create_topics.sh"
# ./scripts/kafka/create_topics.sh

echo "Running batch.sh"
./scripts/spark/batch/batch.sh

echo "Running streaming.sh"
./scripts/spark/streaming/streaming.sh