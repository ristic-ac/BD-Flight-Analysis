#!/usr/bin/python3
import os
import time
from kafka import KafkaProducer
import kafka.errors
import pandas as pd

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = os.environ["TOPIC"]

# Load the CSV file into pandas dataframe
df = pd.read_csv("flights_germany.csv")
df.head(5)
while True:
    try:
        # Connect to Kafka
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
        for index, row in df.iterrows():
            print("Sending message to Kafka")
            # Extract the key and value from the row
            key = row["key"]
            # Value is row without key
            row = row.drop("key")
            # Cast stop to int
            row["stops"] = int(row["stops"])
            # Cast price to float
            row["price"] = float(row["price"])
            # Cast departure_date_distance to int
            row["departure_date_distance"] = int(row["departure_date_distance"])
            # Send the message
            producer.send(TOPIC, key=str(key).encode(), value=row.to_json().encode())
            print(row.to_json())
            time.sleep(1)
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)
