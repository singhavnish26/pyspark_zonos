from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType, FloatType, DateType, DoubleType
from pyspark.sql.functions import from_json, col, to_date, current_date
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession \
    .builder \
    .appName("KafkaMultiStreamToCassandra") \
    .config("spark.cassandra.connection.host", "13.232.25.194") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()
    
# Set the Spark log level to ERROR
sc = spark.sparkContext
sc.setLogLevel('ERROR')

# Define the Kafka topic name and host
kafka_host = "minor.zonos.engrid.in:9092"
topic1 = "ext_device-event_10121"
topic2 = "ext_device-telemetry_10121"
topic3 = "ext_device_10121"
topic4 = "ext_device-measurement_10121"
topic5 = "ext_register-statistic_10121"
topic6 = "ext_device-process_10121"
topic7 = "ext_device-parameter_10121"
topic8 = "ext_device-profile-facts_10121"

#Define Schema for the relevant kafka topics
schema1 = StructType([
    StructField("device", StringType(), True),
    StructField("event", StringType(), True),
    StructField("firstOccurrenceTime", TimestampType(), True),
    StructField("lastOccurrenceTime", TimestampType(), True),
    StructField("occurrenceCount", IntegerType(), True),
    StructField("receiveTime", TimestampType(), True),
    StructField("persistTime", TimestampType(), True),
    StructField("state", StringType(), True),
    StructField("context", StringType(), True)
])

schema2 = StructType([
    StructField("device", StringType(), True),
    StructField("lastReceiveTime", TimestampType(), True)
])

schema3 = StructType([
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

schema4 = StructType([
    StructField("device", StringType(), True),
    StructField("measureTime", TimestampType(), True),
    StructField("receiveTime", TimestampType(), True),
    StructField("persistTime", TimestampType(), True),
    StructField("readingReason", StringType(), True),
    StructField("dataPoints", ArrayType(
        StructType([
            StructField("register", StringType(), True),
            StructField("value", StringType(), True),
            StructField("unit", StringType(), True),
            StructField("measureTime", TimestampType(), True)
        ])
    ), True),
    StructField("status", ArrayType(StringType(), True)),
    StructField("profile", StringType(), True)
])

schema5 = StructType([
    StructField("device", StringType(), True),
    StructField("register", StringType(), True),
    StructField("date", DateType(), True),
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

schema6 = StructType([
    StructField("id", IntegerType()),
    StructField("type", StringType()),
    StructField("device", StringType()),
    StructField("persistTime", TimestampType()),
    StructField("initTime", TimestampType()),
    StructField("startTime", TimestampType()),
    StructField("previous", StructType([
        StructField("status", StringType())
    ])),
    StructField("current", StructType([
        StructField("status", StringType()),
        StructField("error", StringType())
    ])),
    StructField("completionTime", TimestampType()),
    StructField("executionType", StringType()),
    StructField("parameters", StructType([
        StructField("profileRead", StructType([
            StructField("profileId", StringType()),
            StructField("readingReasonCode", StringType())
        ]))
    ]))
])

schema7 = StructType([
    StructField("device", StringType(), True),
    StructField("parameter", StringType(), True),
    StructField("changeTime", TimestampType(), True),
    StructField("receiveTime", TimestampType(), True),
    StructField("persistTime", TimestampType(), True),
    StructField("newValue", StringType(), True),
    StructField("oldValue", StringType(), True)
])

schema8 = StructType([
    StructField("device", StringType(), True),
    StructField("profile", StringType(), True),
    StructField("gatheredAt", TimestampType(), True),
    StructField("actual", DoubleType(), True),
    StructField("expected", DoubleType(), True),
    StructField("completeness", StringType(), True),
    StructField("from", TimestampType(), True),
    StructField("to", TimestampType(), True)
])

# Configure kafka listeners for the relevant kafka topics
df1 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_host) \
    .option("subscribe", topic1) \
    .option("startingOffsets", "earliest") \
    .load()

df2 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_host) \
    .option("subscribe", topic2) \
    .option("startingOffsets", "earliest") \
    .load()

df3 = spark \
    .readStream \
    .format ("kafka") \
    .option ("kafka.bootstrap.servers", kafka_host) \
    .option("subscribe", topic3) \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

df4 = spark \
    .readStream \
    .format ("kafka") \
    .option ("kafka.bootstrap.servers", kafka_host) \
    .option("subscribe", topic4) \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), schema4).alias("data")) \
    .select("data.*")

df5 = spark \
    .readStream \
    .format ("kafka") \
    .option ("kafka.bootstrap.servers", kafka_host) \
    .option("subscribe", topic5) \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), schema5).alias("data")) \

df6 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_host) \
    .option("startingOffsets", "earliest") \
    .option("subscribe", topic6).load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema6).alias("parsed_value"))

df7 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_host) \
    .option("subscribe", topic7) \
    .option("startingOffsets", "earliest") \
    .load()

df8 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_host) \
    .option("subscribe", topic8) \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema8).alias("data")) \
    .select("data.*") 

event_df = df1.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema1).alias("data")) \
    .select("data.*") \
    .withColumn('current_date', to_date(col('persistTime'), 'yyyy-MM-dd')) \
    .drop("context")
event_df = event_df.select([col(c).alias(c.lower()) for c in event_df.columns])


telemetry_df = df2.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema2).alias("data")) \
    .select("data.*") \
    .withColumnRenamed("lastReceiveTime","lastreceivetime")

device_df = df3.select(from_json(col("value"), schema3).alias("parsed_value"))

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
device_df = device_df.withColumn('date', to_date(col('persistTime'), 'yyyy-MM-dd'))

measure_df = df4.selectExpr(
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
    "status",
    "profile"
).withColumn("value", col("value").cast(FloatType())) \
.drop("dataPoint.measureTime") \
.drop("status")
measure_df=measure_df.withColumn('receive_date', to_date(col('receiveTime'), 'yyyy-MM-dd'))
measure_df=measure_df.select([col(c).alias(c.lower()) for c in measure_df.columns])

regstat_df = df5.selectExpr("data.device", "data.register", "data.date", "data.meterReads.*", "data.deliveryDelay.*") \
    .withColumnRenamed("expected", "mr_expected") \
    .withColumnRenamed("received", "mr_received") \
    .withColumnRenamed("edited", "mr_edited") \
    .withColumnRenamed("invalidated", "mr_invalidated") \
    .withColumnRenamed("estimated", "mr_estimated") \
    .withColumnRenamed("minimum", "dd_minimum") \
    .withColumnRenamed("maximum", "dd_maximum") \
    .withColumnRenamed("average", "dd_average") 

process_df = df6.selectExpr(
    "parsed_value.id",
    "parsed_value.type",
    "parsed_value.device",
    "parsed_value.persistTime",
    "parsed_value.initTime",
    "parsed_value.startTime",
    "parsed_value.previous.status AS previous_status",
    "parsed_value.current.status AS current_status",
    "parsed_value.current.error AS current_error",
    "parsed_value.completionTime",
    "parsed_value.executionType",
    "parsed_value.parameters.profileRead.profileId AS profile_id",
    "parsed_value.parameters.profileRead.readingReasonCode AS reading_reason_code"
)

process_df = process_df.select([col(c).alias(c.lower()) for c in process_df.columns])
process_df = process_df.withColumn('date', to_date(col('persistTime'), 'yyyy-MM-dd'))

parameter_df = df7.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema7).alias("data")) \
    .select("data.*")
parameter_df = parameter_df.select([col(c).alias(c.lower()) for c in parameter_df.columns])
parameter_df = parameter_df.withColumn('date', to_date(col('receivetime'), 'yyyy-MM-dd'))

profile_df = df8.withColumnRenamed("from", "fromtime") \
                .withColumnRenamed("to", "totime")
profile_df = profile_df.select([col(c).alias(c.lower()) for c in profile_df.columns])
profile_df = profile_df.withColumn('date', to_date(col('gatheredat'), 'yyyy-MM-dd'))


query1 = event_df.writeStream \
    .outputMode("append") \
    .queryName("event") \
    .option("checkpointLocation", "/var/log/spark_ckpt/dev_event") \
    .foreachBatch(lambda df, epochId: df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="events", keyspace="reporting") \
        .save())

query2 = telemetry_df.writeStream \
    .outputMode("append") \
    .queryName("telemetry") \
    .option("checkpointLocation", "/var/log/spark_ckpt/dev_telemetry") \
    .foreachBatch(lambda df, epochId: df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="telemetry", keyspace="reporting") \
        .save())

query3 = device_df.writeStream \
    .outputMode("append") \
    .queryName("device") \
    .option("checkpointLocation", "/var/log/spark_ckpt/device") \
    .foreachBatch(lambda df, epochId: df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="device", keyspace="reporting") \
        .option("spark.cassandra.output.ignoreNulls", "true") \
        .save())

query4 = measure_df.writeStream \
    .outputMode("append") \
    .queryName("device_measurement") \
    .option("checkpointLocation", "/var/log/spark_ckpt/dev_measurement") \
    .foreachBatch(lambda df, epochId: df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="measurement", keyspace="reporting") \
        .option("spark.cassandra.output.ignoreNulls", "true") \
        .save())

query5 = regstat_df.writeStream \
    .outputMode("append") \
    .queryName("register_statistics") \
    .option("checkpointLocation", "/var/log/spark_ckpt/dev_reg_stat") \
    .foreachBatch(lambda df, epochId: df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="regstat", keyspace="reporting") \
        .option("spark.cassandra.output.ignoreNulls", "true") \
        .save())

query6 = process_df.writeStream \
    .outputMode("append") \
    .queryName("device_process") \
    .option("checkpointLocation", "/var/log/spark_ckpt/dev_process") \
    .foreachBatch(lambda df, epochId: df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="process", keyspace="reporting") \
        .option("spark.cassandra.output.ignoreNulls", "true") \
        .save())

query7 = parameter_df.writeStream \
    .outputMode("append") \
    .queryName("parameters") \
    .option("checkpointLocation", "/var/log/spark_ckpt/dev_parameter") \
    .foreachBatch(lambda df, epochId: df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="parameter", keyspace="reporting") \
        .option("spark.cassandra.output.ignoreNulls", "true") \
        .save())

query8 = profile_df.writeStream \
    .outputMode("append") \
    .queryName("profile_fact") \
    .option("checkpointLocation", "/var/log/spark_ckpt/dev_profile") \
    .foreachBatch(lambda df, epochId: df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="profile_fact", keyspace="reporting") \
        .option("spark.cassandra.output.ignoreNulls", "true") \
        .save())

query1.start()
query2.start()
query3.start()
query4.start()
query5.start()
query6.start()
query7.start()
query8.start()
spark.streams.awaitAnyTermination()

#regstat_df.printSchema()
### Print the data as a table
#query = profile_df \
#    .writeStream \
#    .format("console") \
#    .option("truncate", "false") \
#    .start()
#query.awaitTermination()
