from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType

# set up the SparkSession
spark = SparkSession.builder.appName("KafkaConsumer").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('ERROR')

# define the schema for the Kafka message
schema = StructType([
    StructField("device", StringType(), True),
    StructField("measureTime", StringType(), True),
    StructField("receiveTime", StringType(), True),
    StructField("persistTime", StringType(), True),
    StructField("readingReason", StringType(), True),
    StructField("dataPoints", ArrayType(
        StructType([
            StructField("register", StringType(), True),
            StructField("value", StringType(), True),
            StructField("unit", StringType(), True),
            StructField("measureTime", StringType(), True)
        ])
    ), True),
    StructField("status", ArrayType(StringType(), True)),
    StructField("profile", StringType(), True)
])

# read data from Kafka into a DataFrame
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "zonos.engrid.in:9092") \
    .option("subscribe", "ext_device-measurement_10121") \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# explode the dataPoints array column
kafka_df = kafka_df.selectExpr(
    "device",
    "measureTime",
    "receiveTime",
    "persistTime",
    "readingReason",
    "explode(dataPoints) as dataPoint",
    "status",
    "profile"
).select(
    "device",
    "measureTime",
    "receiveTime",
    "persistTime",
    "readingReason",
    "dataPoint.register",
    "dataPoint.value",
    "dataPoint.unit",
    "dataPoint.measureTime",
    "status",
    "profile"
).withColumn("value", col("value").cast(FloatType()))

# drop the unwanted column
kafka_df = kafka_df.drop('dataPoint.measureTime','status')
# rename the remaining columns
kafka_df = kafka_df.withColumnRenamed("measureTime", "mtime") \
    .withColumnRenamed("receiveTime", "rtime") \
    .withColumnRenamed("persistTime", "ptime") \
    .withColumnRenamed("readingReason", "readingreason")

# print the schema of the modified DataFrame
kafka_df.printSchema()

# start the streaming query
query = kafka_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()