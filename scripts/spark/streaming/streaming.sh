#!/bin/bash

echo "Executing Spark streaming job"

docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /spark/streaming/query1.py"

echo "Finished query 1"

docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /spark/streaming/query2.py"

echo "Finished query 2"

docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /spark/streaming/query3.py"

echo "Finished query 3"

docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /spark/streaming/query4.py"

echo "Finished query 4"

docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /spark/streaming/query5.py"

echo "Finished query 5"

echo "Spark streaming job executed"