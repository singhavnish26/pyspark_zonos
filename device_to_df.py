from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Flatten_JSON") \
    .getOrCreate()
    
sc = spark.sparkContext
sc.setLogLevel('ERROR')

# Define the schema for the json data
json_schema = StructType([
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
                StructField("latitude", DoubleType()),
                StructField("longitude", DoubleType())
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

# Read the data from the kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "zonos.engrid.in:9092") \
    .option("subscribe", "ext_device_10121") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the json data
# Parse the json data
parsed_df = df.select(
    from_json(col("value").cast("string"), json_schema).alias("data")
).select("data.*")

# Flatten the current field
flattened_df = parsed_df.select(
    "persistTime",
    col("current.device").alias("device"),
    col("current.type").alias("type"),
    col("current.group").alias("group"),
    col("current.inventoryState").alias("inventoryState"),
    col("current.managementState").alias("managementState"),
    col("current.communicationId").alias("communicationId"),
    col("current.manufacturer").alias("manufacturer"),
    col("current.description").alias("description"),
    col("current.model").alias("model"),
    col("current.location.geo.latitude").alias("latitude"),
    col("current.location.geo.longitude").alias("longitude"),
    col("current.location.address.city").alias("city"),
    col("current.location.address.postalCode").alias("postalCode"),
    col("current.location.address.street").alias("street"),
    col("current.location.address.houseNumber").alias("houseNumber"),
    col("current.location.address.floor").alias("floor"),
    col("current.location.address.company").alias("company"),
    col("current.location.address.country").alias("country"),
    col("current.location.address.reference").alias("reference"),
    col("current.location.address.timeZone").alias("timeZone"),
    col("current.location.address.region").alias("region"),
    col("current.location.address.district").alias("district"),
    col("current.location.logicalInstallationPoint").alias("logicalInstallationPoint")
)

# Print the dataframe as table
query = flattened_df \
    .writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
