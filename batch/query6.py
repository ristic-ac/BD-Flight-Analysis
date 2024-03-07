#  |-- baseFare: double (nullable = true)
#  |-- destinationAirport: string (nullable = true)
#  |-- elapsedDays: long (nullable = true)
#  |-- fareBasisCode: string (nullable = true)
#  |-- flightDate: string (nullable = true)
#  |-- isBasicEconomy: boolean (nullable = true)
#  |-- isNonStop: boolean (nullable = true)
#  |-- isRefundable: boolean (nullable = true)
#  |-- legId: string (nullable = true)
#  |-- searchDate: string (nullable = true)
#  |-- seatsRemaining: long (nullable = true)
#  |-- segmentsAirlineCode: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- segmentsAirlineName: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- segmentsArrivalAirportCode: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- segmentsArrivalTimeEpochSeconds: array (nullable = true)
#  |    |-- element: long (containsNull = true)
#  |-- segmentsArrivalTimeRaw: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- segmentsCabinCode: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- segmentsDepartureAirportCode: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- segmentsDepartureTimeEpochSeconds: array (nullable = true)
#  |    |-- element: long (containsNull = true)
#  |-- segmentsDepartureTimeRaw: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- segmentsDistance: array (nullable = true)
#  |    |-- element: long (containsNull = true)
#  |-- segmentsDurationInSeconds: array (nullable = true)
#  |    |-- element: long (containsNull = true)
#  |-- segmentsEquipmentDescription: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- startingAirport: string (nullable = true)
#  |-- totalFare: double (nullable = true)
#  |-- travelDuration: string (nullable = true)

from datetime import datetime
import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql import functions as F

MONGO_DATABASE = "flights"
MONGO_COLLECTION = "query6"
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

df = spark.read.json(HDFS_NAMENODE + "/data/itineraries_sample_array.json")

# df.show()

df.printSchema()

# Determine the airport with the most number of landing flights 
QUERY6 = df.select(F.explode("segmentsArrivalAirportCode").alias("arrivalAirport")) \
           .groupBy("arrivalAirport").count() \
           .orderBy(col("count").desc()) \
           .limit(1)

# # Print on console
QUERY6.show()

QUERY6 \
    .write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", OUTPUT_URI) \
    .option("database", MONGO_DATABASE) \
    .option("collection", MONGO_COLLECTION) \
    .save()