#!/bin/bash

# Manually set up MongoDB & Metabase

echo "Running put.sh"
./scripts/hdfs/put.sh

echo "Running create_topics.sh"
./scripts/kafka/create_topics.sh

echo "Running transform.sh"
./scripts/spark/batch/transform.sh
