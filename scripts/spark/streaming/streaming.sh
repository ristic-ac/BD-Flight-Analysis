#!/bin/bash

echo "Executing Spark streaming job"

docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /spark/streaming/query1.py "
# ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /spark/streaming/query1.py 

echo "Spark streaming job executed"