from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, struct

# Create a Spark session
spark = SparkSession.builder.appName("DevMeasurementToDF").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Read data from the Kafka topic as a stream
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "zonos.engrid.in:9092") \
  .option("subscribe", "ext_device-measurement_10121") \
  .option("startingOffsets", "earliest") \
  .load()

# Define the schema for the data
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType, DoubleType

schema = StructType([
  StructField("device", StringType()),
  StructField("measureTime", TimestampType()),
  StructField("measureTime", TimestampType()),
  StructField("receiveTime", TimestampType()),
  StructField("persistTime", TimestampType()),
  StructField("readingReason", StringType()),
  StructField("dataPoints", ArrayType(
    StructType([
      StructField("register", StringType()),
      StructField("value", DoubleType()),
      StructField("unit", StringType()),
      StructField("measureTime", TimestampType())
    ])
  )),
  StructField("status", ArrayType(StringType())),
  StructField("profile", StringType())
])

# Flatten the nested JSON data
parsed_df = df.select(
  df.value.cast("string").alias("json_data"),
  df.timestamp.alias("event_timestamp")
)

flattened_df = parsed_df.select(
  "event_timestamp",
  struct(parsed_df.json_data).alias("data")
).select(
  "event_timestamp",
  "data.device",
  "data.measureTime",
  "data.receiveTime",
  "data.persistTime",
  "data.readingReason",
  explode("data.dataPoints").alias("dataPoints"),
  "data.status",
  "data.profile"
)

# Print the flattened dataframe as a table
query = flattened_df.writeStream.format("console").option("truncate", "false").start()
query.awaitTermination()
