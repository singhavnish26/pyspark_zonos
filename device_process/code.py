from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, FloatType, TimestampType

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

# Define the schema for the Kafka message
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("device", StringType(), True),
    StructField("persistTime", StringType(), True),
    StructField("initTime", StringType(), True),
    StructField("startTime", StringType(), True),
    StructField("previous", StructType([
        StructField("status", StringType(), True)
    ]), True),
    StructField("current", StructType([
        StructField("status", StringType(), True),
        StructField("error", StringType(), True)
    ]), True),
    StructField("completionTime", StringType(), True),
    StructField("executionType", StringType(), True),
    StructField("parameters", StructType([
        StructField("profileRead", StructType([
            StructField("profileId", StringType(), True),
            StructField("readingReasonCode", StringType(), True)
        ]), True)
    ]), True)
])

# Read data from Kafka into a DataFrame
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "zonos.engrid.in:9092") \
    .option("subscribe", "ext_device-process_10121") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .selectExpr("data.id", "data.type", "data.device", 
                "to_timestamp(data.persistTime, 'yyyy-MM-dd''T''HH:mm:ss.SSSZ') as persistTime", 
                "to_timestamp(data.initTime, 'yyyy-MM-dd''T''HH:mm:ss.SSSZ') as initTime", 
                "to_timestamp(data.startTime, 'yyyy-MM-dd''T''HH:mm:ss.SSSZ') as startTime",
                "data.previous.status as previous_status", 
                "data.current.status as current_status", 
                "data.current.error as current_error",
                "to_timestamp(data.completionTime, 'yyyy-MM-dd''T''HH:mm:ss.SSSZ') as completionTime", 
                "data.executionType", 
                "data.parameters.profileRead.profileId as profile_id",
                "data.parameters.profileRead.readingReasonCode as reading_reason_code")
kafka_df.printSchema()
# Rename the columns in the DataFrame
kafka_df = kafka_df \
    .withColumnRenamed("current_status", "status") \
    .withColumnRenamed("current_error", "error") \
    .withColumnRenamed("persistTime", "persisttime") \
    .withColumnRenamed("initTime", "inittime") \
    .withColumnRenamed("startTime", "starttime") \
    .withColumnRenamed("completionTime", "completiontime") \
    .withColumnRenamed("completionTime", "completiontime") \
    .withColumnRenamed("executionType", "executiontype")

# Print the schema of the DataFrame
kafka_df.printSchema()


query = kafka_df \
  .writeStream \
  .format("console") \
  .option("truncate", "false") \
  .start() \
  .awaitTermination()