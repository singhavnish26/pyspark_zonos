from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession \
  .builder \
  .appName("KafkaStreamToDataFrame") \
  .getOrCreate()

# Set the Spark log level to ERROR
sc = spark.sparkContext
sc.setLogLevel('ERROR')

# Define the Kafka topic name and host
topic_name = "ext_device-telemetry_10121"
kafka_host = "zonos.engrid.in:9092"

# Read the data from the Kafka topic into a DataFrame
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_host) \
  .option("subscribe", topic_name) \
  .option("startingOffsets", "earliest") \
  .load()

# Define the schema for the data
schema = StructType([
  StructField("device", StringType(), True),
  StructField("lastReceiveTime", TimestampType(), True)
])

# Parse the value column into a DataFrame using the defined schema
parsed_df = df.selectExpr("CAST(value AS STRING)") \
  .select(from_json(col("value"), schema).alias("data")) \
  .select("data.*")

# Print the data as a table
query = parsed_df \
  .writeStream \
  .format("console") \
  .option("truncate", "false") \
  .start()

query.awaitTermination()
