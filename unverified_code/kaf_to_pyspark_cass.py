from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryException

def create_spark_session():
    """
    Creates a Spark session
    :return: Spark session
    """
    spark = SparkSession \
        .builder \
        .appName("Flatten_JSON") \
        .getOrCreate()

    return spark

def create_json_schema():
    """
    Creates the schema for the json data
    :return: JSON schema
    """
    json_schema = StructType([
        StructField("device", StringType()),
        StructField("register", StringType()),
        StructField("persistTime", TimestampType()),
        StructField("unit", IntegerType()),
        StructField("consumptionDataPoints", StructType([
            StructField("time", TimestampType()),
            StructField("value", IntegerType()),
            StructField("quality", StringType())
        ]))
    ])

    return json_schema

def read_data_from_kafka(spark, json_schema):
    """
    Reads data from the kafka topic
    :param spark: Spark session
    :param json_schema: JSON schema
    :return: DataFrame containing the data from the kafka topic
    """
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "zonos.engrid.in:9092") \
        .option("subscribe", "ext_device-derived-measurement_10121") \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_df = df.select(
        from_json(col("value").cast("string"), json_schema).alias("data")
    ).select("data.*")

    return parsed_df

def flatten_data(parsed_df):
    """
    Flattens the nested JSON data
    :param parsed_df: DataFrame containing the parsed JSON data
    :return: DataFrame containing the flattened data
    """
    flattened_df = parsed_df.select(
        "persistTime",
        "device",
        "register",
        "unit",
        "consumptionDataPoints.time",
        "consumptionDataPoints.value",
        "consumptionDataPoints.quality"
    )

    return flattened_df

from pyspark.sql.functions import lit

def display_data(flattened_df, keyspace, table, cassandra_host, cassandra_user, cassandra_password):
    """
    Writes the flattened data to the specified Cassandra table
    :param flattened_df: DataFrame containing the flattened data
    :param keyspace: Cassandra keyspace to use
    :param table: Cassandra table to write to
    :param cassandra_host: Cassandra host to connect to
    :param cassandra_user: Cassandra username
    :param cassandra_password: Cassandra password
    """
    flattened_df = flattened_df \
        .withColumn("time", flattened_df.time.cast("timestamp")) \
        .withColumn("persistTime", flattened_df.persistTime.cast("timestamp"))

    flattened_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .option("keyspace", keyspace) \
        .option("table", table) \
        .option("spark.cassandra.connection.host", cassandra_host) \
        .option("spark.cassandra.auth.username", cassandra_user) \
        .option("spark.cassandra.auth.password", cassandra_password) \
        .save()

    print("Data saved to Cassandra")

