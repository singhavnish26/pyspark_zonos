from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import from_json, col, when
from pyspark.sql import SparkSession


# Create Spark session
spark = SparkSession \
  .builder \
  .appName("ext_device_10121") \
  .config("spark.cassandra.connection.host", "13.232.25.194") \
  .config("spark.cassandra.auth.username", "cassandra") \
  .config("spark.cassandra.auth.password", "cassandra") \
  .getOrCreate()

# Set the Spark log level to ERROR
sc = spark.sparkContext
sc.setLogLevel('ERROR')



# Read the data from the kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "zonos.engrid.in:9092") \
    .option("subscribe", "ext_device_10121") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the json data
parsed_df = df.select(
    from_json(col("value").cast("string"), json_schema).alias("data")
).select("data.*")

# Flatten the current field
flattened_df = parsed_df.select(
    col("persistTime").alias("persisttime"),
    col("current.device").alias("device"),
    col("current.type").alias("type"),
    col("current.group").alias("group"),
    col("current.inventoryState").alias("inventorystate"),
    col("current.managementState").alias("managementstate"),
    col("current.communicationId").alias("communicationid"),
    col("current.manufacturer").alias("manufacturer"),
    col("current.description").alias("description"),
    col("current.model").alias("model"),
    col("current.location.geo.latitude").alias("latitude"),
    col("current.location.geo.longitude").alias("longitude"),
    col("current.location.address.city").alias("city"),
    col("current.location.address.postalCode").alias("postalcode"),
    col("current.location.address.street").alias("street"),
    col("current.location.address.houseNumber").alias("housenumber"),
    col("current.location.address.floor").alias("floor"),
    col("current.location.address.company").alias("company"),
    col("current.location.address.country").alias("country"),
    col("current.location.address.reference").alias("reference"),
    col("current.location.address.timeZone").alias("timezone"),
    col("current.location.address.region").alias("region"),
    col("current.location.address.district").alias("district"),
    col("current.location.logicalInstallationPoint").alias("logicalinstallationpoint")
)

# Write the parsed DataFrame to Cassandra using foreachBatch
def write_to_cassandra(batch_df, batch_id):
    batch_df = batch_df.withColumn("manufacturer", when(col("manufacturer").isNull(), "unknown").otherwise(col("manufacturer")))
    try:
        batch_df.write \
          .format("org.apache.spark.sql.cassandra") \
          .options(table="device", keyspace="reporting") \
          .mode("append") \
          .option("spark.cassandra.output.ignoreNulls", "true") \
          .save()
    except Exception as e:
        print(f"Error writing to Cassandra: {str(e)}")


flattened_df.writeStream \
  .foreachBatch(write_to_cassandra) \
  .outputMode("append") \
  .option("checkpointLocation", "/var/log/spark/checkpoints") \
  .start() \
  .awaitTermination()