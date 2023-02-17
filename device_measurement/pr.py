
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
    "status",
    "profile"
).withColumn("value", col("value").cast(FloatType()))
kafka_df.printSchema()
# drop the unwanted column
kafka_df = kafka_df.drop("dataPoint.measureTime")
kafka_df = kafka_df.drop("status")

# rename the remaining columns
kafka_df = kafka_df.withColumnRenamed("measureTime", "measuretime") \
    .withColumnRenamed("receiveTime", "receivetime") \
    .withColumnRenamed("persistTime", "persisttime") \
    .withColumnRenamed("readingReason", "readingreason")

# print the schema of the modified DataFrame
kafka_df.printSchema()



# Write the parsed DataFrame to Cassandra using foreachBatch
def write_to_cassandra(batch_df, batch_id):
    #batch_df.printSchema()
    try:
        batch_df.write \
          .format("org.apache.spark.sql.cassandra") \
          .options(table="measurement", keyspace="poc") \
          .mode("append") \
          .option("spark.cassandra.output.ignoreNulls", "true") \
          .save()
    except Exception as e:
        print(f"Error writing to Cassandra: {str(e)}")



kafka_df.writeStream \
  .foreachBatch(write_to_cassandra) \
  .outputMode("append") \
  .option("checkpointLocation", "/var/log/spark/checkpoints") \
  .start() \
  .awaitTermination()
