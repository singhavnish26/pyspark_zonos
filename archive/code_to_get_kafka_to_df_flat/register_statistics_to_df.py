from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Flatten_JSON") \
    .config("spark.dynamicAllocation.enabled", "true") \

    .getOrCreate()
    
sc = spark.sparkContext
sc.setLogLevel('ERROR')

# Define the schema for the json data
json_schema = StructType([
    StructField("device", StringType()),
    StructField("register", StringType()),
    StructField("date", TimestampType()),
    StructField("meterReads", StructType([
        StructField("expected", IntegerType()),
        StructField("received", IntegerType()),
        StructField("edited", IntegerType()),
        StructField("invalidated", IntegerType()),
        StructField("estimated", IntegerType())
    ])),
    StructField("deliveryDelay", StructType([
        StructField("minimum", IntegerType()),
        StructField("maximum", IntegerType()),
        StructField("average", DoubleType())
    ]))
])

# Read the data from the kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "zonos.engrid.in:9092") \
    .option("subscribe", "ext_register-statistic_10121") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the json data
parsed_df = df.select(
    from_json(col("value").cast("string"), json_schema).alias("data")
).select("data.*")

# Show the dataframe as a table
#parsed_df.writeStream.outputMode("append").format("console").start().awaitTermination()

# Print the data as a table
query = parsed_df \
  .writeStream \
  .format("console") \
  .option("truncate", "false") \
  .start()

query.awaitTermination()