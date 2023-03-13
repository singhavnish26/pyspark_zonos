import logging
from configparser import ConfigParser
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType, FloatType, DateType, DoubleType
from pyspark.sql.functions import from_json, col, to_date, current_date
from pyspark.sql import SparkSession
from schema import schema1, schema2
import logging

logger = logging.getLogger(__name__)


# Read configuration file
config = ConfigParser()
config.read('config.ini')

# Define the Kafka topic name and host
kafka_host = config.get('kafka', 'kafka_host')
topic1 = config.get('kafka', 'event_topic')
topic2 = config.get('kafka', 'telemetry_topic')

#Get Cassandra Host and Credentials
cassandra_host = config.get('cassandra', 'cass_host')
cassandra_user = config.get('cassandra', 'username')
cassandra_password = config.get('cassandra', 'password')

# Configure logging
logging.basicConfig(filename='app.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Create Spark session
spark = SparkSession \
    .builder \
    .appName("KafkaMultiStreamToCassandra") \
    .config("spark.cassandra.connection.host", cassandra_host) \
    .config("spark.cassandra.auth.username", cassandra_user) \
    .config("spark.cassandra.auth.password", cassandra_password) \
    .getOrCreate()

# Set the Spark log level to ERROR
sc = spark.sparkContext
sc.setLogLevel('ERROR')



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
def write_to_cassandra(df, epochId, table_name):
    try:
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table=table_name, keyspace="reporting") \
            .save()
    except Exception as e:
        logger.error(f"Error occurred while processing {table_name} query: {e.__class__.__name__} - {str(e)}")

event_query_checkpoint = config.get('checkpoint', 'event_query_checkpoint')
telemetry_query_checkpoint = config.get('checkpoint', 'telemetry_query_checkpoint')
query1 = event_df.writeStream \
    .outputMode("append") \
    .queryName("event") \
    .option("checkpointLocation", event_query_checkpoint) \
    .foreachBatch(lambda df, epochId: write_to_cassandra(df, epochId, "events"))

query2 = telemetry_df.writeStream \
    .outputMode("append") \
    .queryName("telemetry") \
    .option("checkpointLocation", telemetry_query_checkpoint) \
    .foreachBatch(lambda df, epochId: write_to_cassandra(df, epochId, "telemetry"))


"""telemetry_df.printSchema()
event_df.printSchema()"""

query1.start()
query2.start()
spark.streams.awaitAnyTermination()
