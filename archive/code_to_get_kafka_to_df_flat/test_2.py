from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType, TimestampType, IntegerType, MapType

spark = SparkSession.builder.appName("multi_df").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('ERROR')

# Define the Kafka topic name and host
topic2 = "ext_device-parameter_10121"
topic1 = "ext_device-process_10121"
kafka_host = "zonos.engrid.in:9092"

# Read the data from the Kafka topic 1 into a DataFrame
kafka_df1 = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_host) \
  .option("subscribe", topic1) \
  .option("startingOffsets", "earliest") \
  .load()

kafka_df2 = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_host) \
  .option("subscribe", topic2) \
  .option("startingOffsets", "earliest") \
  .load()
  
  # Define the schema for the data

schema1 = StructType([
  StructField("id", IntegerType(), True),
  StructField("type", StringType(), True),
  StructField("device", StringType(), True),
  StructField("persistTime", TimestampType(), True),
  StructField("initTime", TimestampType(), True),
  StructField("startTime", TimestampType(), True),
  StructField("previous", 
              StructType([
                StructField("status", StringType(), True)
              ]),
              True),
  StructField("current",
              StructType([
                StructField("status", StringType(), True),
                StructField("error", StringType(), True)
              ]),
              True),
  StructField("completionTime", TimestampType(), True),
  StructField("executionType", StringType(), True),
  StructField("parameters", 
              MapType(StringType(),
                      StructType([
                        StructField("profileId", StringType(), True),
                        StructField("readingReasonCode", StringType(), True)
                      ]),
                      True),
              True)
])

schema2 = StructType([
  StructField("device", StringType(), True),
  StructField("parameter", StringType(), True),
  StructField("changeTime", TimestampType(), True),
  StructField("receiveTime", TimestampType(), True),
  StructField("persistTime", TimestampType(), True),
  StructField("newValue", StringType(), True),
  StructField("oldValue", StringType(), True)
])

# Parse the value column into a DataFrame using the defined schema
kafka_df1 = kafka_df1.selectExpr("CAST(value AS STRING)") \
  .select(from_json(col("value"), schema1).alias("data")) \
  .select("data.*")
  
kafka_df2 = kafka_df2.selectExpr("CAST(value AS STRING)") \
  .select(from_json(col("value"), schema2).alias("data")) \
  .select("data.*")
  
console1 = kafka_df1.writeStream.outputMode("append").format("console").start()
console2 = kafka_df2.writeStream.outputMode("append").format("console").start()