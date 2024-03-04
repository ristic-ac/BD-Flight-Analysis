from datetime import datetime
import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql import functions as F

MONGO_DATABASE = "flights"
MONGO_COLLECTION = "query2"
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

INPUT_URI = "mongodb://mongodb:27017/" + MONGO_DATABASE + "." + MONGO_COLLECTION
OUTPUT_URI = INPUT_URI

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL - Query 1") \
    .master('local')\
    .config("spark.mongodb.input.uri", INPUT_URI) \
    .config("spark.mongodb.output.uri", OUTPUT_URI) \
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
    .getOrCreate()

df = spark.read.json(HDFS_NAMENODE + "/data/itineraries_sample_array.json")

df.show()

target_date = datetime.strptime("2019-11-28", "%Y-%m-%d").date()

# For each airport, determine the flight on the 4th of July for which there were the most available seats to BOS 
QUERY2 = df.filter(df.flightDate == target_date) \
    # .filter(df["destinationAirport"] == "BOS") \
    # .groupBy("startingAirport") \
    # .max("seatsRemaining")

# Print on console
QUERY2.show()
