from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType, FloatType, DateType, DoubleType
from pyspark.sql.functions import from_json, col, to_date, current_date, max
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession \
    .builder \
    .appName("joinDeviceTelemetry") \
    .config("spark.cassandra.connection.host", "192.168.56.125") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()
    
# Set the Spark log level to ERROR
sc = spark.sparkContext
sc.setLogLevel('ERROR')

# Define the Kafka topic name and host
kafka_host = "minor.zonos.engrid.in:9092"
topic1 = "ext_device_10121"
topic2 = "ext_device-telemetry_10121"


schema1 = StructType([
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
            ])),
            StructField("logicalInstallationPoint", StringType())
        ])),
        StructField("tags", StringType())
    ]))
])
schema2 = StructType([
    StructField("device", StringType(), True),
    StructField("lastReceiveTime", TimestampType(), True)
])

df1 = spark \
    .readStream \
    .format ("kafka") \
    .option ("kafka.bootstrap.servers", kafka_host) \
    .option("subscribe", topic1) \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

df2 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_host) \
    .option("subscribe", topic2) \
    .option("startingOffsets", "earliest") \
    .load()
device_df = df1.select(from_json(col("value"), schema1).alias("parsed_value"))
# Flatten the dataframe by exploding the nested structs
device_df = device_df.selectExpr("parsed_value.persistTime",
                                "parsed_value.current.device",
                                "parsed_value.current.type",
                                "parsed_value.current.group",
                                "parsed_value.current.inventoryState",
                                "parsed_value.current.managementState",
                                "parsed_value.current.communicationId",
                                "parsed_value.current.manufacturer",
                                "parsed_value.current.description",
                                "parsed_value.current.model",
                                "parsed_value.current.location.geo.latitude",
                                "parsed_value.current.location.geo.longitude",
                                "parsed_value.current.location.address.city",
                                "parsed_value.current.location.address.postalCode",
                                "parsed_value.current.location.address.street",
                                "parsed_value.current.location.address.houseNumber",
                                "parsed_value.current.location.address.floor",
                                "parsed_value.current.location.address.company",
                                "parsed_value.current.location.address.country",
                                "parsed_value.current.location.address.reference",
                                "parsed_value.current.location.address.timeZone",
                                "parsed_value.current.location.address.region",
                                "parsed_value.current.location.address.district",
                                "parsed_value.current.location.logicalInstallationPoint")

#Convert all the column names to lower case instead of camelCase
device_df = device_df.select([col(c).alias(c.lower()) for c in device_df.columns])
device_df_v2 = device_df.drop("group", "description", "model", "latitude", "longitude", "city", "postalcode", "street", "housenumber", "floor", "company", "country", "reference", "timezone", "region", "district", "logicalinstallationpoint")
telemetry_df = df2.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema2).alias("data")) \
    .select("data.*") \
    .withColumnRenamed("lastReceiveTime","last_telemetry")

# Join the two dataframes on the device column
joined_df = device_df_v2.join(telemetry_df, "device")

query = joined_df.writeStream \
    .outputMode("append") \
    .queryName("device") \
    .foreachBatch(lambda df, epochId: df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="device", keyspace="reporting") \
        .option("spark.cassandra.output.ignoreNulls", "true") \
        .save())
query.start()
spark.streams.awaitAnyTermination()