#!/bin/bash

echo "Executing Spark batch job"

# https://www.mongodb.com/docs/spark-connector/v3.0/
docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 /spark/batch/transform.py"
# docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 /spark/batch/query1.py"
docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 /spark/batch/query2.py"

echo "Spark batch job executed"
