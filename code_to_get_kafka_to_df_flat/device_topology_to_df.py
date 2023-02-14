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
topic_name = "ext_device-topology_10121"
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
json_schema = StructType([
    StructField("device", StringType(), True),
    StructField("receiveTime", TimestampType(), True),
    StructField("persistTime", TimestampType(), True),
    StructField("children", StructType([
        StructField("childDevice", StringType(), True),
        StructField("addTime", TimestampType(), True),
        StructField("dataSource", StringType(), True)
    ]), True)
])

# Parse the value column into a DataFrame using the defined schema
parsed_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), json_schema).alias("data"))

# Flatten the children column into the dataframe
flattened_df = parsed_df.select("data.*", "data.children.*")

# Print the data as a table
query = flattened_df \
  .writeStream \
  .format("console") \
  .option("truncate", "false") \
  .start()

query.awaitTermination()
