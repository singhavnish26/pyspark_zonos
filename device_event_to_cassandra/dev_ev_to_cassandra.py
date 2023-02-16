from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession \
  .builder \
  .appName("KafkaStreamToDataFrame") \
  .config("spark.cassandra.connection.host", "13.232.25.194") \
  .config("spark.cassandra.auth.username", "cassandra") \
  .config("spark.cassandra.auth.password", "cassandra") \
  .getOrCreate()

# Set the Spark log level to ERROR
sc = spark.sparkContext
sc.setLogLevel('ERROR')

# Define the Kafka topic name and host
topic_name = "ext_device-event_10121"
kafka_host = "zonos.engrid.in:9092"

# Read the data from the Kafka topic into a DataFrame
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_host) \
  .option("subscribe", topic_name) \
  .option("startingOffsets", "earliest") \
  .load()

# Print the schema of the DataFrame
#df.printSchema()

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
  .select("data.*")

# Write the parsed DataFrame to Cassandra using foreachBatch
def write_to_cassandra(batch_df, batch_id):
    if batch_df is not None and not batch_df.rdd.isEmpty():
        batch_df.printSchema()
        try:
            batch_df.write \
              .format("org.apache.spark.sql.cassandra") \
              .options(table="dev_event", keyspace="poc") \
              .mode("append") \
              .option("confirm.truncate", "true") \
              .option("spark.cassandra.output.ignoreNulls", "true") \
              .option("spark.cassandra.output.batch.grouping.buffer.size", "5000") \
              .option("spark.cassandra.output.concurrent.writes", "100") \
              .option("spark.cassandra.output.batch.grouping.key", "device") \
              .save()
        except Exception as e:
            print(f"Error writing to Cassandra: {str(e)}")


query = parsed_df \
  .writeStream \
  .foreachBatch(write_to_cassandra) \
  .start()

query.awaitTermination()
