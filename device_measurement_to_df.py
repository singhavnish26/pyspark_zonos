from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType, TimestampType

spark = SparkSession.builder.appName("KafkaDataFrame").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('ERROR')

kafka_topic = "ext_device-measurement_10121"

# Define the schema for the JSON data
json_schema = StructType([
    StructField("device", StringType(), True),
    StructField("measureTime", TimestampType(), True),
    StructField("receiveTime", TimestampType(), True),
    StructField("persistTime", TimestampType(), True),
    StructField("readingReason", StringType(), True),
    StructField("dataPoints", ArrayType(
        StructType([
            StructField("register", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("measureTime", TimestampType(), True)
        ])
    ), True),
    StructField("status", ArrayType(StringType()), True),
    StructField("profile", StringType(), True)
])

# Read the Kafka topic into a DataFrame
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "zonos.engrid.in:9092") \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json_data")
df = df.filter(col("value").isNotNull())
# Parse the JSON data
parsed_df = df.select(from_json(col("json_data"), json_schema).alias("data"))
flattened_df = parsed_df.selectExpr(
    "data.device",
    "data.measureTime",
    "data.receiveTime",
    "data.persistTime",
    "data.readingReason",
    "explode(data.dataPoints) as dataPoints"
)
flattened_df = flattened_df.selectExpr(
    "device",
    "measureTime",
    "receiveTime",
    "persistTime",
    "readingReason",
    "dataPoints.register",
    "dataPoints.value",
    "dataPoints.unit",
    "dataPoints.measureTime as dataPointMeasureTime"
)

# Print the parsed DataFrame
query = flattened_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
