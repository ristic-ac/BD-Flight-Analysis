#!/bin/bash

echo "Executing Spark streaming job"

for ((i=1; i<=5; i++)); do
    echo "Executing Query $i"
    sleep 1
    docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /spark/streaming/query${i}.py"
    echo "Finished Query $i"
done

echo "Spark streaming job executed"