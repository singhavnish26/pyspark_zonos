from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession \
  .builder \
  .appName("DeviceParameter") \
  .config("spark.cassandra.connection.host", "13.232.25.194") \
  .config("spark.cassandra.auth.username", "cassandra") \
  .config("spark.cassandra.auth.password", "cassandra") \
  .getOrCreate()

# Set the Spark log level to ERROR
sc = spark.sparkContext
sc.setLogLevel('ERROR')

# Define the Kafka topic name and host
topic_name = "ext_device-parameter_10121"
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
  StructField("parameter", StringType(), True),
  StructField("changeTime", TimestampType(), True),
  StructField("receiveTime", TimestampType(), True),
  StructField("persistTime", TimestampType(), True),
  StructField("newValue", StringType(), True),
  StructField("oldValue", StringType(), True)
])

# Parse the value column into a DataFrame using the defined schema
parsed_df = df.selectExpr("CAST(value AS STRING)") \
  .select(from_json(col("value"), schema).alias("data")) \
  .select("data.*")

#parsed_df.printSchema()
kafka_df = parsed_df \
    .withColumnRenamed("changeTime", "changetime") \
    .withColumnRenamed("receiveTime", "receivetime") \
    .withColumnRenamed("persistTime", "persisttime") \
    .withColumnRenamed("newValue", "newvalue") \
    .withColumnRenamed("oldValue", "oldvalue")
#kafka_df.printSchema()
# Print the data as a table
"""query = kafka_df \
  .writeStream \
  .format("console") \
  .option("truncate", "false") \
  .start() \
  .awaitTermination()"""

# Write the parsed DataFrame to Cassandra using foreachBatch
def write_to_cassandra(batch_df, batch_id):
    try:
        batch_df.write \
          .format("org.apache.spark.sql.cassandra") \
          .options(table="dev_parameter", keyspace="poc") \
          .mode("append") \
          .option("spark.cassandra.output.ignoreNulls", "true") \
          .save()
    except Exception as e:
        print(f"Error writing to Cassandra: {str(e)}")

kafka_df.writeStream \
  .foreachBatch(write_to_cassandra) \
  .outputMode("append") \
  .start() \
  .awaitTermination()