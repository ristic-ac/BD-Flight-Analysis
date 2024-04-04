#!/bin/bash

echo "Executing Spark batch job"
sleep 1

for ((i=1; i<=10; i++)); do
    echo "Executing Query $i"
    sleep 1
    docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 /spark/batch/query$i.py"
    echo "Finished Query $i"
    echo "Press any key to continue"
    read -n 1 -s
done

echo "Spark batch job executed"