from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, FloatType, DateType

# Create Spark session
spark = SparkSession \
    .builder \
    .appName("KafkaStreamToConsole") \
    .getOrCreate()

# Set the Spark log level to ERROR
sc = spark.sparkContext
sc.setLogLevel('ERROR')

# define the schema for the Kafka message
schema = StructType([
    StructField("device", StringType(), True),
    StructField("register", StringType(), True),
    StructField("date", StringType(), True),
    StructField("meterReads", StructType([
        StructField("expected", IntegerType(), True),
        StructField("received", IntegerType(), True),
        StructField("edited", IntegerType(), True),
        StructField("invalidated", IntegerType(), True),
        StructField("estimated", IntegerType(), True)
    ]), True),
    StructField("deliveryDelay", StructType([
        StructField("minimum", IntegerType(), True),
        StructField("maximum", IntegerType(), True),
        StructField("average", FloatType(), True)
    ]), True)
])

# read data from Kafka into a DataFrame
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "zonos.engrid.in:9092") \
    .option("subscribe", "ext_register-statistic_10121") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .selectExpr("data.device", "data.register", "data.date", 
                "data.meterReads.*", "data.deliveryDelay.*")

kafka_df.printSchema()

kafka_df = kafka_df \
    .withColumnRenamed("expected", "mr_expected") \
    .withColumnRenamed("received", "mr_received") \
    .withColumnRenamed("edited", "mr_edited") \
    .withColumnRenamed("invalidated", "mr_invalidated") \
    .withColumnRenamed("estimated", "mr_estimated") \
    .withColumnRenamed("minimum", "dd_minimum") \
    .withColumnRenamed("maximum", "dd_maximum") \
    .withColumnRenamed("average", "dd_average")
# print the schema of the modified DataFrame
kafka_df.printSchema()

query = kafka_df \
  .writeStream \
  .format("console") \
  .option("truncate", "false") \
  .start()

query.awaitTermination()