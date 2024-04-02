from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from quietlogs import quiet_logs
import os

spark = SparkSession \
    .builder \
    .appName("SSS - Query 3") \
    .master('local')\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
    .getOrCreate()

quiet_logs(spark)

schema = StructType([
  StructField("departure_code", StringType(), True),
  StructField("arrival_code", StringType(), True),
  StructField("scrape_date", DateType(), True),
  StructField("departure_date_distance", StringType(), True),
  StructField("airline", StringType(), True),
  StructField("stops", IntegerType(), True),
  StructField("price", FloatType(), True),
  StructField("departure", TimestampType(), True),
  StructField("arrival", TimestampType(), True)
])

# Read data from Kafka topic "flights_germany" 

df_flights_raw = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092") \
  .option("subscribe", "flights_germany") \
  .load() \

df_flights_raw.printSchema()

# Take timestamp and parsed value from the value column
df_flights = df_flights_raw \
  .withColumn("parsed_value", from_json(col("value").cast("string"), schema)) \
  .withColumn("timestamp_received", col("timestamp")) \
  .select("parsed_value.*", "timestamp_received") \

# Define HDFS namenode
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# Read the CSV file
df_airports = spark.read.csv(HDFS_NAMENODE + "/data/airports.csv", header=True)

# Which airline had the most direct flights, windowed every 30 seconds and sliding every 10 seconds (Sliding window)
df_flights = df_flights \
  .withWatermark("timestamp_received", "1 seconds") \
  .filter(col("stops") == 0) \
  .groupBy(window("timestamp_received", "30 seconds", "10 seconds"), "airline") \
  .agg(count("stops").alias("direct_flights")) \
  
df_flights_console = df_flights \
  .writeStream \
  .outputMode("append") \
  .format("console") \
  .option("truncate", "false") \
  .start()

df_flights_hdfs = df_flights.writeStream \
  .outputMode("append") \
  .format("json") \
  .option("path", HDFS_NAMENODE + "/data/query3") \
  .option("checkpointLocation", HDFS_NAMENODE + "/tmp/query3_checkpoint") \
  .start()

spark.streams.awaitAnyTermination()
