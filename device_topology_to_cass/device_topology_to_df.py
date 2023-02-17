from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession \
    .builder \
    .appName("DeviceTopology") \
    .config("spark.cassandra.connection.host", "13.232.25.194") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

# Set the Spark log level to ERROR
sc = spark.sparkContext
sc.setLogLevel('ERROR')

# Define the Kafka topic name and host
topic_name = "ext_device-topology_10121"
kafka_host = "zonos.engrid.in:9092"

# Define the schema for the data
schema = StructType([
    StructField("device", StringType(), True),
    StructField("receiveTime", TimestampType(), True),
    StructField("persistTime", TimestampType(), True),
    StructField("children", ArrayType(StructType([
        StructField("childDevice", StringType(), True),
        StructField("addTime", TimestampType(), True),
        StructField("dataSource", StringType(), True)
    ])), True)
])

# Read the data from the Kafka topic into a DataFrame
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_host) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the value column into a DataFrame using the defined schema
kafka_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Explode the children array to convert to rows
kafka_df = kafka_df.select("device", "receiveTime", "persistTime", explode("children").alias("child"))

# Create new columns for each nested field
kafka_df = kafka_df \
    .withColumn("childDevice", col("child.childDevice")) \
    .withColumn("addTime", col("child.addTime")) \
    .withColumn("dataSource", col("child.dataSource")) \
    .drop("child")

kafka_df.printSchema()
kafka_df = kafka_df \
    .withColumnRenamed("receiveTime", "receivetime") \
    .withColumnRenamed("persistTime", "persisttime") \
    .withColumnRenamed("childDevice", "childdevice") \
    .withColumnRenamed("addTime", "addtime") \
    .withColumnRenamed("dataSource", "datasource")
kafka_df.printSchema()

"""# Print the parsed DataFrame
query = kafka_df \
  .writeStream \
  .format("console") \
  .option("truncate", "false") \
  .start()

query.awaitTermination() """

# Write the flattened DataFrame to Cassandra using foreachBatch
def write_to_cassandra(batch_df, batch_id):
    try:
        batch_df.write \
          .format("org.apache.spark.sql.cassandra") \
          .options(table="device_topology", keyspace="poc_av") \
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
