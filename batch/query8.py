from datetime import datetime
import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, concat_ws, sort_array, array, array_sort
from pyspark.sql.types import *
from pyspark.sql import functions as F
from quietlogs import quiet_logs

MONGO_DATABASE = "flights"
MONGO_COLLECTION = "query8"
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

INPUT_URI = "mongodb://mongodb:27017/" + MONGO_DATABASE + "." + MONGO_COLLECTION
OUTPUT_URI = INPUT_URI

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL - Query 8") \
    .master('local')\
    .config("spark.mongodb.input.uri", INPUT_URI) \
    .config("spark.mongodb.output.uri", OUTPUT_URI) \
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
    .getOrCreate()

quiet_logs(spark)

df = spark.read.json(HDFS_NAMENODE + "/data/itineraries_sample_array.json")

# Determine the minimum travel duration

minTravelTime = df.agg(F.min("travelDurationMinutes")).collect()[0][0]

print(minTravelTime)

# Find those who have the minimum travel duration or 5 minutes more than the minimum travel duration
QUERY8 = df.filter(df.travelDurationMinutes < minTravelTime + 5) \
           .select("startingAirport", "destinationAirport") \
           .distinct() \
           .withColumn("flight", array_sort(array(col("startingAirport"), col("destinationAirport")))) \
           .drop("startingAirport", "destinationAirport") \
           .distinct() \

# # Print on console
QUERY8.show()

QUERY8 \
    .write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", OUTPUT_URI) \
    .option("database", MONGO_DATABASE) \
    .option("collection", MONGO_COLLECTION) \
    .save()