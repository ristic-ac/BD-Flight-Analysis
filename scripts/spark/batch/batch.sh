#!/bin/bash

echo "Executing Spark batch job"

# https://www.mongodb.com/docs/spark-connector/v3.0/
<<<<<<< HEAD
docker cp /home/branislav/Downloads/mongo-spark-connector_2.12-3.0.2.jar spark-master:/spark/jars/
docker exec -it spark-master bash
./spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 /spark/batch/query1.py 
=======
docker exec -it spark-master bash -c "./spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 /spark/batch/query1.py "

echo "Spark batch job executed"
>>>>>>> c631e14 (sync)
