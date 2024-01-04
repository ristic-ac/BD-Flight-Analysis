docker exec -it namenode bash
hadoop fs -mkdir /data
hadoop fs -put /data/itineraries_sample.csv /data


docker exec -it spark-master bash

/usr/bin# ./kafka-topics --create --topic flights_germany --partitions 1 --replication-factor 1 --zookeeper zookeeper:2181
/usr/bin# ./kafka-topics --list --zookeeper zookeeper:2181

./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /spark/streaming/query1.py 

# Create mongodb db and collection in mongo
docker exec -it mongo bash
mongo
use flights
db.createCollection("flights_germany")
# Get all documents from collection
db.flights_germany.find().pretty()

