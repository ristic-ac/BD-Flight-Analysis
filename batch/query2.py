from datetime import datetime
import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql import functions as F
from quietlogs import quiet_logs

MONGO_DATABASE = "flights"
MONGO_COLLECTION = "query2"
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

INPUT_URI = "mongodb://mongodb:27017/" + MONGO_DATABASE + "." + MONGO_COLLECTION
OUTPUT_URI = INPUT_URI

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL - Query 2") \
    .master('local')\
    .config("spark.mongodb.input.uri", INPUT_URI) \
    .config("spark.mongodb.output.uri", OUTPUT_URI) \
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
    .getOrCreate()

quiet_logs(spark)

df = spark.read.json(HDFS_NAMENODE + "/data/itineraries_sample_array.json")

# For each airport, determine the flight on the 17th of April for which there were the average available seats to BOS 

QUERY2 = df.filter((df.flightDate == "2022-04-17") & (df.destinationAirport == "BOS")) \
           .groupBy("startingAirport") \
           .agg(F.round(F.avg("seatsRemaining"), 2).alias("avgSeatsRemaining"))

# Print on console
QUERY2.show()

QUERY2 \
    .write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", OUTPUT_URI) \
    .option("database", MONGO_DATABASE) \
    .option("collection", MONGO_COLLECTION) \
    .save()