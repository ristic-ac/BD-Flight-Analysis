from datetime import datetime
import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, concat_ws, sort_array, array, array_sort
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from quietlogs import quiet_logs

MONGO_DATABASE = "flights"
MONGO_COLLECTION = "query10"
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

# df.show()

df.printSchema()

# Determine the average overpayed amount and the total overpayed amount for each legId when compared to the moving average of the totalFare over the last 4 searches

windowSpec = Window.partitionBy("legId").orderBy("searchDate").rowsBetween(-3, 0)

QUERY10 = df \
    .select(
        col("totalFare"),
        F.round(F.avg("totalFare").over(windowSpec), 2).alias("movingAvg")
    ) \
    .filter(col("movingAvg").isNotNull()) \
    .withColumn("overpayed", F.round(col("totalFare") - col("movingAvg"), 2)) \
    .select(
        col("overpayed")
    ) \
    .filter(col("overpayed") > 0) \
    .agg(
        F.round(F.avg("overpayed"),2).alias("avgOverpayed"),
        F.round(F.sum("overpayed"),2).alias("sumOverpayed")
    )    
            
        
    
# # Print on console
QUERY10.show()

QUERY10 \
    .write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", OUTPUT_URI) \
    .option("database", MONGO_DATABASE) \
    .option("collection", MONGO_COLLECTION) \
    .save()