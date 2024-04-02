from datetime import datetime
import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from quietlogs import quiet_logs

MONGO_DATABASE = "flights"
MONGO_COLLECTION = "query4"
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

INPUT_URI = "mongodb://mongodb:27017/" + MONGO_DATABASE + "." + MONGO_COLLECTION
OUTPUT_URI = INPUT_URI

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL - Query 4") \
    .master('local')\
    .config("spark.mongodb.input.uri", INPUT_URI) \
    .config("spark.mongodb.output.uri", OUTPUT_URI) \
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
    .getOrCreate()

quiet_logs(spark)

df = spark.read.json(HDFS_NAMENODE + "/data/itineraries_sample_array.json")

df.printSchema()

# Find top 10% of itineraries with the highest total fare
windowSpec = Window.orderBy(col("totalFare").desc())

QUERY4 = df \
    .select(
        col("startingAirport"),
        col("destinationAirport"),
        col("totalFare"),
        F.cume_dist().over(windowSpec).alias("rank")
    ) \
    .filter(col("rank") <= 0.1)

# # Print on console
QUERY4.show()

QUERY4 \
    .write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", OUTPUT_URI) \
    .option("database", MONGO_DATABASE) \
    .option("collection", MONGO_COLLECTION) \
    .save()