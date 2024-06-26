from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from quietlogs import quiet_logs
import os

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

# Define HDFS namenode
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# Read the CSV file
df_airports = spark.read.csv(HDFS_NAMENODE + "/data/airports.csv", header=True)

# Print df_airports
df_airports.show()

df_flights.printSchema()

  
df_flights_console = df_flights \
  .writeStream \
  .outputMode("append") \
  .format("console") \
  .option("truncate", "false") \
  .start()

# Save to HDFS
df_flights_hdfs = df_flights.writeStream \
  .outputMode("append") \
  .format("json") \
  .option("path", HDFS_NAMENODE + "/data/query2") \
  .option("checkpointLocation", HDFS_NAMENODE + "/tmp/query2_checkpoint") \
  .start() 

spark.streams.awaitAnyTermination()
