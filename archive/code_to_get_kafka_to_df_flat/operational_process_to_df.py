from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .config("spark.dynamicAllocation.enabled", "true") \
    .appName("Flatten_JSON") \
    .getOrCreate()
    
sc = spark.sparkContext
sc.setLogLevel('ERROR')

# Define the schema for the json data
json_schema = StructType([
    StructField("id", IntegerType()),
    StructField("type", StringType()),
    StructField("persistTime", TimestampType()),
    StructField("current", StructType([
        StructField("status", StringType())
    ]))
])

# Read the data from the kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "zonos.engrid.in:9092") \
    .option("subscribe", "ext_operational-process_10121") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the json data
parsed_df = df.select(
    from_json(col("value").cast("string"), json_schema).alias("data")
).select("data.*")

# Flatten the current field
flattened_df = parsed_df.select(
    "id",
    "type",
    "persistTime",
    col("current.status").alias("status")
)

# Show the data in a table
query = flattened_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
