from datetime import datetime
import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, concat_ws, sort_array, array, array_sort
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from quietlogs import quiet_logs

MONGO_DATABASE = "flights"
MONGO_COLLECTION = "query11"
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

INPUT_URI = "mongodb://mongodb:27017/" + MONGO_DATABASE + "." + MONGO_COLLECTION
OUTPUT_URI = INPUT_URI

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL - Query 11") \
    .master('local')\
    .config("spark.mongodb.input.uri", INPUT_URI) \
    .config("spark.mongodb.output.uri", OUTPUT_URI) \
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
    .getOrCreate()

quiet_logs(spark)

df = spark.read.json(HDFS_NAMENODE + "/data/itineraries_sample_array.json")

# Determine the ratio of isBasicEconomy by startingAirport
window1 = Window.partitionBy("startingAirport", "isBasicEconomy")
window2 = Window.partitionBy("startingAirport")

QUERY11 = df \
    .withColumn("ratio", F.round(F.count("isBasicEconomy").over(window1) / F.count("*").over(window2), 4)) \
    .select("startingAirport", "isBasicEconomy", "ratio") \
    .distinct() \
    .sort("startingAirport", "isBasicEconomy")
        
    
# # Print on console
QUERY11.show()

QUERY11 \
    .write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", OUTPUT_URI) \
    .option("database", MONGO_DATABASE) \
    .option("collection", MONGO_COLLECTION) \
    .save()