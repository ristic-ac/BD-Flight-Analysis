#!/bin/bash

echo "Running batch.sh"
./scripts/spark/batch/batch.sh

echo "Running streaming.sh"
./scripts/spark/streaming/streaming.sh