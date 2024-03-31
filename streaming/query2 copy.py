from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import os


def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("SSS - Query 2") \
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

df_flights = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092") \
  .option("subscribe", "flights_germany") \
  .load() \
  .withColumn("parsed_value", from_json(col("value").cast("string"), schema)) \
  .select("parsed_value.*") \

  # .withColumn("value", F.col("value").cast("string")) \
  # .select("value") \

# Define HDFS namenode
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# Read the CSV file
df_airports = spark.read.csv(HDFS_NAMENODE + "/data/airports.csv", header=True)

# Print df_airports
df_airports.show()

df_flights.printSchema()

# Find shortest flight based on difference between departure and arrival
df_flights = df_flights \
  .withColumn("flight_duration", col("arrival").cast("long") - col("departure").cast("long")) \
  .groupBy([c for c in df_flights.columns if c != "flight_duration"]) \
  .agg(min("flight_duration").alias("min_flight_duration")) \
  .orderBy(col("min_flight_duration").asc()) \
  .limit(5)
  
  
df_flights \
  .writeStream \
  .trigger(processingTime="2 seconds") \
  .outputMode("complete") \
  .format("console") \
  .option("truncate", "false") \
  .start()

spark.streams.awaitAnyTermination()
