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

# Define the schema for the Kafka message value
schema = StructType([
    StructField("persistTime", StringType()),
    StructField("previous", StructType([
        StructField("meteringPoint", StringType()),
        StructField("group", StringType()),
        StructField("serviceLevel", StringType()),
        StructField("devices", ArrayType(StructType([
            StructField("device", StringType()),
            StructField("assignTime", StringType()),
            StructField("removeTime", StringType())
        ]))),
        StructField("stateHistory", ArrayType(StructType([
            StructField("state", StringType()),
            StructField("activeSince", StringType())
        ]))),
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
            ]))
        ]))
    ])),
    StructField("current", StructType([
        StructField("meteringPoint", StringType()),
        StructField("group", StringType()),
        StructField("serviceLevel", StringType()),
        StructField("devices", ArrayType(StructType([
            StructField("device", StringType()),
            StructField("assignTime", StringType()),
            StructField("removeTime", StringType())
        ]))),
        StructField("stateHistory", ArrayType(StructType([
            StructField("state", StringType()),
            StructField("activeSince", StringType())
        ]))),
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
            ]))
        ]))
    ]))
])
# read data from Kafka into a DataFrame
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "zonos.engrid.in:9092") \
    .option("subscribe", "ext_device-measurement_10121") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")
    
kafka_df.printSchema()
query = kafka_df \
  .writeStream \
  .format("console") \
  .option("truncate", "false") \
  .start()

query.awaitTermination()
