from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession \
  .builder \
  .appName("KafkaStreamToCassandra") \
  .config("spark.cassandra.connection.host", "13.232.25.194") \
  .config("spark.cassandra.auth.username", "cassandra") \
  .config("spark.cassandra.auth.password", "cassandra") \
  .getOrCreate()

# Set the Spark log level to ERROR
sc = spark.sparkContext
sc.setLogLevel('ERROR')

# Define the Kafka topic name and host
topic_name = "ext_device-event_10121"
kafka_host = "minor.zonos.engrid.in:9092"

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
  StructField("event", StringType(), True),
  StructField("firstOccurrenceTime", TimestampType(), True),
  StructField("lastOccurrenceTime", TimestampType(), True),
  StructField("occurrenceCount", IntegerType(), True),
  StructField("receiveTime", TimestampType(), True),
  StructField("persistTime", TimestampType(), True),
  StructField("state", StringType(), True),
  StructField("context", StringType(), True)
])

# Parse the value column into a DataFrame using the defined schema
parsed_df = df.selectExpr("CAST(value AS STRING)") \
  .select(from_json(col("value"), schema).alias("data")) \
  .select("data.*") \
  .withColumnRenamed("firstOccurrenceTime", "firstoccurrencetime") \
  .withColumnRenamed("lastOccurrenceTime", "lastoccurrencetime") \
  .withColumnRenamed("occurrenceCount", "occurrencecount") \
  .withColumnRenamed("receiveTime", "receivetime") \
  .withColumnRenamed("persistTime", "persisttime") \
  .drop("context")


# Print the flattened dataframe as a table
query = parsed_df.writeStream.outputMode("append").format("console").option("truncate", False).start()

# Wait for the query to finish
query.awaitTermination()

"""
# Write the parsed DataFrame to Cassandra using foreachBatch
def write_to_cassandra(batch_df, batch_id):
    #batch_df.printSchema()
    try:
        batch_df.write \
          .format("org.apache.spark.sql.cassandra") \
          .options(table="device_event", keyspace="poc") \
          .mode("append") \
          .option("spark.cassandra.output.ignoreNulls", "true") \
          .save()
    except Exception as e:
        print(f"Error writing to Cassandra: {str(e)}")

parsed_df.writeStream \
  .foreachBatch(write_to_cassandra) \
  .outputMode("append") \
  .option("checkpointLocation", "/var/log/spark/checkpoints") \
  .start() \
  .awaitTermination()
"""