#!/bin/bash

echo "Executing Transform"
sleep 1
docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 /spark/batch/transform.py"
echo "Transform executed"