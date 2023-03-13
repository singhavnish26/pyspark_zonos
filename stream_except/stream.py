import logging
from configparser import ConfigParser
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType, FloatType, DateType, DoubleType
from pyspark.sql.functions import from_json, col, to_date, current_date
from pyspark.sql import SparkSession
from schemas import schema1, schema2

# Read configuration file
config = ConfigParser()
config.read('config.ini')

# Configure logging
logging.basicConfig(filename='app.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

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
topic1 = config.get('kafka', 'event_topic')
topic2 = config.get('kafka', 'telemetry_topic')

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

# Process event data
event_df = df1.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema1).alias("data")) \
    .select("data.*") \
    .withColumn('current_date', to_date(col('persistTime'), 'yyyy-MM-dd')) \
    .drop("context")
event_df = event_df.select([col(c).alias(c.lower()) for c in event_df.columns])

# Process telemetry data
telemetry_df = df2.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema2).alias("data")) \
    .select("data.*") \
    .withColumnRenamed("lastReceiveTime","lasttelemetry")

#Write Data to Cassandra
query1 = event_df.writeStream \
    .outputMode("append") \
    .queryName("event") \
    .option("checkpointLocation", event_query_checkpoint) \
    .foreachBatch(lambda df, epochId: 
        try:
            df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table="events", keyspace="reporting") \
                .save()
        except Exception as e:
            logger.error(f"Error occurred while processing query1: {e.__class__.__name__} - {str(e)}")
    )

query2 = telemetry_df.writeStream \
    .outputMode("append") \
    .queryName("event") \
    .option("checkpointLocation", telemetry_query_checkpoint) \
    .foreachBatch(lambda df, epochId: 
        try:
            df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table="telemetry", keyspace="reporting") \
                .save()
        except Exception as e:
            logger.error(f"Error occurred while processing query1: {e.__class__.__name__} - {str(e)}")
    )

query1.start()
query2.start()
spark.streams.awaitAnyTermination()
