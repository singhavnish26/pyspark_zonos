from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import from_json, col, when
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
# Define the schema for the json data
json_schema = StructType([
    StructField("persistTime", TimestampType(), nullable=True),
    StructField("current", StructType([
        StructField("device", StringType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("group", StringType(), nullable=True),
        StructField("inventoryState", StringType(), nullable=True),
        StructField("managementState", StringType(), nullable=True),
        StructField("communicationId", StringType(), nullable=True),
        StructField("manufacturer", StringType(), nullable=True),
        StructField("description", StringType(), nullable=True),
        StructField("model", StringType(), nullable=True),
        StructField("location", StructType([
            StructField("geo", StructType([
                StructField("latitude", DoubleType(), nullable=True),
                StructField("longitude", DoubleType(), nullable=True)
            ]), nullable=True),
            StructField("address", StructType([
                StructField("city", StringType(), nullable=True),
                StructField("postalCode", StringType(), nullable=True),
                StructField("street", StringType(), nullable=True),
                StructField("houseNumber", StringType(), nullable=True),
                StructField("floor", StringType(), nullable=True),
                StructField("company", StringType(), nullable=True),
                StructField("country", StringType(), nullable=True),
                StructField("reference", StringType(), nullable=True),
                StructField("timeZone", StringType(), nullable=True),
                StructField("region", StringType(), nullable=True),
                StructField("district", StringType(), nullable=True)
            ]), nullable=True),
            StructField("logicalInstallationPoint", StringType(), nullable=True)
        ]), nullable=True),
        StructField("tags", StringType(), nullable=True)
    ]), nullable=True)
])


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
flattened_df.printSchema()
# Print the dataframe as table
"""query = flattened_df \
    .writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
"""
# Write the parsed DataFrame to Cassandra using foreachBatch
def write_to_cassandra(batch_df, batch_id):
    batch_df = batch_df.withColumn("manufacturer", when(col("manufacturer").isNull(), "unknown").otherwise(col("manufacturer")))
    try:
        batch_df.write \
          .format("org.apache.spark.sql.cassandra") \
          .options(table="device_data", keyspace="poc") \
          .mode("append") \
          .option("spark.cassandra.output.ignoreNulls", "true") \
          .save()
    except Exception as e:
        print(f"Error writing to Cassandra: {str(e)}")


flattened_df.writeStream \
  .foreachBatch(write_to_cassandra) \
  .outputMode("append") \
  .start() \
  .awaitTermination()