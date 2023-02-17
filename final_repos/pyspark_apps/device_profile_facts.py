from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession \
    .builder \
    .appName("ext_device-profile-facts_10121") \
    .config("spark.cassandra.connection.host", "13.232.25.194") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

# Set the Spark log level to ERROR
sc = spark.sparkContext
sc.setLogLevel('ERROR')

# Define the Kafka topic name and host
topic_name = "ext_device-profile-facts_10121"
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
  StructField("profile", StringType(), True),
  StructField("gatheredAt", TimestampType(), True),
  StructField("actual", DoubleType(), True),
  StructField("expected", DoubleType(), True),
  StructField("completeness", StringType(), True),
  StructField("from", TimestampType(), True),
  StructField("to", TimestampType(), True)
])

# Parse the value column into a DataFrame using the defined schema
kafka_df = df.selectExpr("CAST(value AS STRING)") \
  .select(from_json(col("value"), schema).alias("data")) \
  .select("data.*")

kafka_df = kafka_df \
  .withColumnRenamed("gatheredAt", "gatheredat") \
  .withColumnRenamed("from", "from_time") \
  .withColumnRenamed("to", "to_time")

def write_to_cassandra(batch_df, batch_id):
    try:
        batch_df.write \
          .format("org.apache.spark.sql.cassandra") \
          .options(table="dev_profile_facts", keyspace="reporting") \
          .mode("append") \
          .option("spark.cassandra.output.ignoreNulls", "true") \
          .save()
    except Exception as e:
        print(f"Error writing to Cassandra: {str(e)}")

kafka_df.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("append") \
    .option("checkpointLocation", "/var/log/spark/checkpoints") \
    .start() \
    .awaitTermination()
