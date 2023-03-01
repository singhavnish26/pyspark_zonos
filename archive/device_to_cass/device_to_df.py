from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import SparkSession

# create SparkSession
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

# Define the Kafka topic to consume from
topic = "ext_device_10121"
kafka_host= "minor.zonos.engrid.in:9092"
# Define the Cassandra Keyspace and Table to insert into
keyspace="reporting"
table="device"
# Define the schema of the Kafka message
schema = StructType([
    StructField("persistTime", TimestampType()),
    StructField("current", StructType([
        StructField("device", StringType()),
        StructField("type", StringType()),
        StructField("group", StringType()),
        StructField("inventoryState", StringType()),
        StructField("managementState", StringType()),
        StructField("communicationId", StringType()),
        StructField("manufacturer", StringType()),
        StructField("description", StringType()),
        StructField("model", StringType()),
        StructField("location", StructType([
            StructField("geo", StructType([
                StructField("latitude", StringType()),
                StructField("longitude", StringType())
            ])),
            StructField("address", StructType([
                StructField("city", StringType()),
                StructField("postalCode", StringType()),
                StructField("street", StringType()),
                StructField("houseNumber", StringType()),
                StructField("floor", StringType()),
                StructField("company", StringType()),
                StructField("country", StringType()),
                StructField("reference", StringType()),
                StructField("timeZone", StringType()),
                StructField("region", StringType()),
                StructField("district", StringType())
            ])),
            StructField("logicalInstallationPoint", StringType())
        ])),
        StructField("tags", StringType())
    ]))
])

# Read messages from Kafka topic and deserialize them using the schema
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_host).option("startingOffsets", "earliest").option("subscribe", topic).load().selectExpr("CAST(value AS STRING)")

# Parse the JSON payload of the Kafka message
df = df.select(from_json(col("value"), schema).alias("parsed_value"))

# Flatten the dataframe by exploding the nested structs
flattened_df = df.selectExpr("parsed_value.persistTime",
                             "parsed_value.current.device",
                             "parsed_value.current.type",
                             "parsed_value.current.group",
                             "parsed_value.current.inventoryState",
                             "parsed_value.current.managementState",
                             "parsed_value.current.communicationId",
                             "parsed_value.current.manufacturer",
                             "parsed_value.current.description",
                             "parsed_value.current.model",
                             "parsed_value.current.location.geo.latitude",
                             "parsed_value.current.location.geo.longitude",
                             "parsed_value.current.location.address.city",
                             "parsed_value.current.location.address.postalCode",
                             "parsed_value.current.location.address.street",
                             "parsed_value.current.location.address.houseNumber",
                             "parsed_value.current.location.address.floor",
                             "parsed_value.current.location.address.company",
                             "parsed_value.current.location.address.country",
                             "parsed_value.current.location.address.reference",
                             "parsed_value.current.location.address.timeZone",
                             "parsed_value.current.location.address.region",
                             "parsed_value.current.location.address.district",
                             "parsed_value.current.location.logicalInstallationPoint")

#Convert all the column names to lower case instead of camelCase
flattened_df = flattened_df.select([col(c).alias(c.lower()) for c in flattened_df.columns])
flattened_df.printSchema()
def write_to_cassandra(batch_df, batch_id):
    try:
        batch_df.write \
          .format("org.apache.spark.sql.cassandra") \
          .options(table="device", keyspace="reporting") \
          .mode("append") \
          .option("spark.cassandra.output.ignoreNulls", "true") \
          .save()
    except Exception as e:
        print(f"Error writing to Cassandra: {str(e)}")

# Write the parsed DataFrame to Cassandra using foreachBatch
flattened_df.writeStream \
  .foreachBatch(write_to_cassandra) \
  .outputMode("append") \
  .start() \
  .awaitTermination()
  
"""flattened_df.printSchema()
# Print the flattened dataframe as a table
query = flattened_df.writeStream.outputMode("append").format("console").option("truncate", False).start()

# Wait for the query to finish
query.awaitTermination()"""
