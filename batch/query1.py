import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from quietlogs import quiet_logs

MONGO_DATABASE = "flights"
MONGO_COLLECTION = "query1"
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

quiet_logs(spark)

df = spark.read.json(HDFS_NAMENODE + "/data/itineraries_sample_array.json")

airport_coordinates = spark.read.csv(HDFS_NAMENODE + "/data/airport_coords.csv", header=True)

airport_coordinates.printSchema()

airport_coordinates.show()

# Find price of a most expensive flight from each startingAirport

windowSpec = Window.partitionBy("startingAirport")

QUERY1 = df \
    .withColumn("maxTotalFare", F.max("totalFare").over(windowSpec)) \
    .where(col("totalFare") == col("maxTotalFare")) \
    .select("startingAirport", "totalFare") \
    .distinct() \
    .join(airport_coordinates, df.startingAirport == airport_coordinates.code, "left") \
    .select("startingAirport", "totalFare", "lat", "long") \
    .withColumn("lat", col("lat").cast(FloatType())) \
    .withColumn("long", col("long").cast(FloatType()))

QUERY1.show()

QUERY1 \
    .write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", OUTPUT_URI) \
    .option("database", MONGO_DATABASE) \
    .option("collection", MONGO_COLLECTION) \
    .save()