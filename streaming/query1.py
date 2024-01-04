from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F


def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("SSS - Query 1") \
    .getOrCreate()

quiet_logs(spark)

# Set schema
# Schema:
# departure_city: String
# arrival_city: String
# scrape_date: Date
# departure_date: Date
# Departure_date_distance: Time interval (String?, Unix time?)
# departure_time: Time
# arrival_time: Time
# airline: String
# stops: Int
# price: Float

schema = StructType([
  StructField("departure_city", StringType(), True),
  StructField("arrival_city", StringType(), True),
  StructField("scrape_date", DateType(), True),
  StructField("departure_date", DateType(), True),
  StructField("departure_date_distance", StringType(), True),
  StructField("departure_time", TimestampType(), True),
  StructField("arrival_time", TimestampType(), True),
  StructField("airline", StringType(), True),
  StructField("stops", IntegerType(), True),
  StructField("price", FloatType(), True)
])

# Read data from Kafka topic "flights_germany" 

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:9092") \
  .option("subscribe", "flights_germany") \
  .load() \
  .withColumn("parsed_value", F.from_json(F.col("value").cast("string"), schema))

df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Value read from Kafka topic are pandas dataframe rows, and keys are ints, so we need to convert them to Spark
# dataframe columns
# df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# # Print schema
# df.printSchema()

# # Cast value column to schema and key column to integer
# df = df.select(from_json(col("value").cast("string"), schema).alias("data"), col("key").cast("int"))

# # Print schema
# df.printSchema()

# # Explode data column
# df = df.selectExpr("data.departure_city", "data.arrival_city", "data.scrape_date", "data.departure_date", "data.departure_date_distance", "data.departure_time", "data.arrival_time", "data.airline", "data.stops", "data.price", "key")

# # Print schema
# df.printSchema()

# # Get departure city and arrival city pairs from the dataframe
# df = df.select("departure_city", "arrival_city").distinct()

# # Print dataframe
# df.printSchema()

# # Write to MongoDB
# query = df \
#   .writeStream \
#   .outputMode("append") \
#   .format("mongo") \
#   .option("uri", "mongodb://mongodb:27017/") \
#   .option("database", "flights") \
#   .option("collection", "flights_germany") \
#   .start()
spark.streams.awaitAnyTermination()
