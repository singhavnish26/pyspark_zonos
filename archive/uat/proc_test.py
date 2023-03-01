from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import SparkSession

# create SparkSession
spark = SparkSession \
.builder \
.appName("ext_device-process_10121") \
.config("spark.cassandra.connection.host", "13.232.25.194") \
.config("spark.cassandra.auth.username", "cassandra") \
.config("spark.cassandra.auth.password", "cassandra") \
.getOrCreate()

# Set the Spark log level to ERROR
sc = spark.sparkContext
sc.setLogLevel('ERROR')

# Define the Kafka topic to consume from
topic = "ext_device-process_10121"
kafka_host = "minor.zonos.engrid.in:9092"

# Define the schema of the Kafka message
schema = StructType([
    StructField("id", StringType()),
    StructField("type", StringType()),
    StructField("device", StringType()),
    StructField("persistTime", TimestampType()),
    StructField("initTime", TimestampType()),
    StructField("startTime", TimestampType()),
    StructField("previous", StructType([
        StructField("status", StringType())
    ])),
    StructField("current", StructType([
        StructField("status", StringType()),
        StructField("error", StringType())
    ])),
    StructField("completionTime", TimestampType()),
    StructField("executionType", StringType()),
    StructField("parameters", StructType([
        StructField("profileRead", StructType([
            StructField("profileId", StringType()),
            StructField("readingReasonCode", StringType())
        ]))
    ]))
])

# Read messages from Kafka topic and deserialize them using the schema
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_host) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

# Parse the JSON payload of the Kafka message
df = df.select(from_json(col("value"), schema).alias("parsed_value"))

# Flatten the dataframe by exploding the nested structs
flattened_df = df.selectExpr("parsed_value.id",
                            "parsed_value.type",
                            "parsed_value.device",
                            "parsed_value.persistTime",
                            "parsed_value.initTime",
                            "parsed_value.startTime",
                            "parsed_value.previous.status as prev_status",
                            "parsed_value.current.status as curr_status",
                            "parsed_value.current.error",
                            "parsed_value.completionTime",
                            "parsed_value.executionType",
                            "parsed_value.parameters.profileRead.profileId",
                            "parsed_value.parameters.profileRead.readingReasonCode")

# Convert all the column names to lower case instead of camelCase
flattened_df = flattened_df.select([col(c).alias(c.lower()) for c in flattened_df.columns])
flattened_df.printSchema()


def write_to_cassandra(batch_df, batch_id):
    try:
        batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="dev_process", keyspace="reporting") \
        .mode("append") \
        .option("spark.cassandra.output.ignoreNulls", "true") \
        .save()
    except Exception as e:
        print(f"Error writing to Cassandra: {str(e)}")


# Write the parsed DataFrame to Cassandra using foreachBatch
flattened_df.writeStream \
.foreachBatch(write_to_cassandra) \
.outputMode("append") \
.option("checkpointLocation", "/var/log/spark/checkpoints") \
.start() \
.awaitTermination()