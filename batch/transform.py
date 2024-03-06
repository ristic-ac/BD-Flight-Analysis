#!/usr/bin/python

import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql import functions as F

# Logs
def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL - Query 1") \
    .master('local')\
    .getOrCreate()

quiet_logs(spark)

# Define the schema for the flight information
flight_schema = StructType([
    StructField("legId", StringType(), True),
    StructField("searchDate", DateType(), True),
    StructField("flightDate", DateType(), True),
    StructField("startingAirport", StringType(), True),
    StructField("destinationAirport", StringType(), True),
    StructField("fareBasisCode", StringType(), True),
    StructField("travelDuration", StringType(), True),
    StructField("elapsedDays", IntegerType(), True),
    StructField("isBasicEconomy", BooleanType(), True),
    StructField("isRefundable", BooleanType(), True),
    StructField("isNonStop", BooleanType(), True),
    StructField("baseFare", FloatType(), True),
    StructField("totalFare", FloatType(), True),
    StructField("seatsRemaining", IntegerType(), True),
    StructField("totalTravelDistance", IntegerType(), True),
    StructField("segmentsDepartureTimeEpochSeconds", StringType(), True),
    StructField("segmentsDepartureTimeRaw", StringType(), True),
    StructField("segmentsArrivalTimeEpochSeconds", StringType(), True),
    StructField("segmentsArrivalTimeRaw", StringType(), True),
    StructField("segmentsArrivalAirportCode", StringType(), True),
    StructField("segmentsDepartureAirportCode", StringType(), True),
    StructField("segmentsAirlineName", StringType(), True),
    StructField("segmentsAirlineCode", StringType(), True),
    StructField("segmentsEquipmentDescription", StringType(), True),
    StructField("segmentsDurationInSeconds", StringType(), True),
    StructField("segmentsDistance", StringType(), True),
    StructField("segmentsCabinCode", StringType(), True)
])

# Define HDFS namenode
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# Read the CSV file
df = spark.read.csv(HDFS_NAMENODE + "/data/itineraries_sample.csv", header=True, schema=flight_schema)

# Print the schema
# df.printSchema()

# Schema = |legId|searchDate|flightDate|startingAirport|destinationAirport|fareBasisCode|travelDuration|elapsedDays|isBasicEconomy|isRefundable|isNonStop|baseFare|totalFare|seatsRemaining|totalTravelDistance|segmentsDepartureTimeEpochSeconds|segmentsDepartureTimeRaw|segmentsArrivalTimeEpochSeconds|segmentsArrivalTimeRaw|segmentsArrivalAirportCode|segmentsDepartureAirportCode| segmentsAirlineName|segmentsAirlineCode|segmentsEquipmentDescription|segmentsDurationInSeconds|segmentsDistance|segmentsCabinCode|

# Show first 20 rows
# df.show(20)

# From StringType() to ArrayType(IntegerType()), delimited by "||"
def string_to_array_int(column):
    column_name = column._jc.toString()
    return F.split(column, "\|\|").cast("array<int>").alias(column_name)
    

# Applying the transformation to the column segmentsDepartureTimeEpochSeconds
df_with_array = df.withColumn("segmentsDepartureTimeEpochSeconds", string_to_array_int(df["segmentsDepartureTimeEpochSeconds"]))

# Applying the transformation to the column segmentsArrivalTimeEpochSeconds
df_with_array = df_with_array.withColumn("segmentsArrivalTimeEpochSeconds", string_to_array_int(df["segmentsArrivalTimeEpochSeconds"]))

# Applying the transformation to the column segmentsDurationInSeconds
df_with_array = df_with_array.withColumn("segmentsDurationInSeconds", string_to_array_int(df["segmentsDurationInSeconds"]))

# Applying the transformation to the column segmentsDistance
df_with_array = df_with_array.withColumn("segmentsDistance", string_to_array_int(df["segmentsDistance"]))

# Print converted columns in df_with_array
# df_with_array.select("segmentsDepartureTimeEpochSeconds", "segmentsArrivalTimeEpochSeconds", "segmentsDurationInSeconds", "segmentsDistance").show(20, False)


# From StringType() to ArrayType(StringType()), delimited by "||"
def string_to_array_string(column):
    column_name = column._jc.toString()
    return F.split(column, "\|\|").cast("array<string>").alias(column_name)

# Applying the transformation to the column segmentsArrivalAirportCode
df_with_array = df_with_array.withColumn("segmentsArrivalAirportCode", string_to_array_string(df["segmentsArrivalAirportCode"]))

# Applying the transformation to the column segmentsDepartureAirportCode
df_with_array = df_with_array.withColumn("segmentsDepartureAirportCode", string_to_array_string(df["segmentsDepartureAirportCode"]))

# Applying the transformation to the column segmentsAirlineName
df_with_array = df_with_array.withColumn("segmentsAirlineName", string_to_array_string(df["segmentsAirlineName"]))

# Applying the transformation to the column segmentsAirlineCode
df_with_array = df_with_array.withColumn("segmentsAirlineCode", string_to_array_string(df["segmentsAirlineCode"]))

# Applying the transformation to the column segmentsEquipmentDescription
df_with_array = df_with_array.withColumn("segmentsEquipmentDescription", string_to_array_string(df["segmentsEquipmentDescription"]))

# Applying the transformation to the column segmentsCabinCode
df_with_array = df_with_array.withColumn("segmentsCabinCode", string_to_array_string(df["segmentsCabinCode"]))

# Print converted columns in df_with_array
# df_with_array.select("segmentsArrivalAirportCode", "segmentsDepartureAirportCode", "segmentsAirlineName", "segmentsAirlineCode", "segmentsEquipmentDescription", "segmentsCabinCode").show(20, False)

# From StringType() to ArrayType(DateType()), delimited by "||"
def string_to_array_timestamp(column):
    column_name = column._jc.toString()
    return F.split(column, "\|\|").cast("array<timestamp>").alias(column_name)

# Applying the transformation to the column segmentsDepartureTimeRaw
df_with_array = df_with_array.withColumn("segmentsDepartureTimeRaw", string_to_array_timestamp(df["segmentsDepartureTimeRaw"]))

# Applying the transformation to the column segmentsArrivalTimeRaw
df_with_array = df_with_array.withColumn("segmentsArrivalTimeRaw", string_to_array_timestamp(df["segmentsArrivalTimeRaw"]))

# Print converted columns in df_with_array
# df_with_array.select("segmentsDepartureTimeRaw", "segmentsArrivalTimeRaw").show(20, False)

# Calculate the total duration in minutes
df_with_array = df_with_array.withColumn("travelDurationMinutes", \
                                         F.regexp_extract(df["travelDuration"], "PT(\d+)H", 1).cast(IntegerType()) * 60 \
                                         + F.regexp_extract(df["travelDuration"], "PT\d+H(\d+)M", 1).cast(IntegerType()))

# Drop the column travelDuration
df_with_array = df_with_array.drop("travelDuration")

df_with_array.write.json(HDFS_NAMENODE + "/data/itineraries_sample_array.json")