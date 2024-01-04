docker exec -it namenode bash
hadoop fs -mkdir /data
hadoop fs -put /data/itineraries_sample.csv /data/
