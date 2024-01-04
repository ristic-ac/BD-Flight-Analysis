# Kafka broker
docker exec -it kafka1 bash
./usr/bin/kafka-topics --create --topic flights_germany --partitions 1 --replication-factor 1 --zookeeper zookeeper:2181
./usr/bin/kafka-topics --list --zookeeper zookeeper:2181