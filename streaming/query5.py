from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os


def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("SSS - Query 4") \
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
  # .withColumn("parsed_value", from_json(col("value").cast("string"), schema)) \
  # .select("parsed_value.*") \

df_flights_raw.printSchema()
#  |-- key: binary (nullable = true)
#  |-- value: binary (nullable = true)
#  |-- topic: string (nullable = true)
#  |-- partition: integer (nullable = true)
#  |-- offset: long (nullable = true)
#  |-- timestamp: timestamp (nullable = true)
#  |-- timestampType: integer (nullable = true)

# Take timestamp and parsed value from the value column
df_flights = df_flights_raw \
  .withColumn("parsed_value", from_json(col("value").cast("string"), schema)) \
  .withColumn("timestamp_received", col("timestamp")) \
  .select("parsed_value.*", "timestamp_received") \

# Define HDFS namenode
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# Read the CSV file
df_airports = spark.read.csv(HDFS_NAMENODE + "/data/airports.csv", header=True)

# Calculate the average price of flight for each airline, windowed every 1 minute with a 30 seconds delay.
df_flights = df_flights \
  .groupBy(window(df_flights.timestamp_received, "1 minute", "30 seconds"), "airline") \
  .agg(F.round(avg("price"), 2).alias("avg_price")) \
  .orderBy(col("window").desc(), col("avg_price").desc()) \
  .limit(3)


df_flights \
  .writeStream \
  .trigger(processingTime="2 seconds") \
  .outputMode("complete") \
  .format("console") \
  .option("truncate", "false") \
  .start()

spark.streams.awaitAnyTermination()
