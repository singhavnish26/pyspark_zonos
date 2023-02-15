import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

logging.basicConfig(level=logging.ERROR)

# define the schema for the DataFrame
schema = StructType([
    StructField("eid", IntegerType(), True),
    StructField("ename", StringType(), True),
    StructField("sal", IntegerType(), True)
])

# create a list of tuples with the data
data = [
    (1, "avnish", 1000),
    (2, "abhishek", 2000),
    (3, "somnath", 3000),
    (4, "probal", 5000),
    (5, "jerry", 10000)
]

# create a SparkSession
spark = SparkSession.builder.appName("InsertIntocassandra").getOrCreate()

# set the logging level to ERROR only
spark.sparkContext.setLogLevel("ERROR")

# create a DataFrame from the data and the schema
try:
    df = spark.createDataFrame(data, schema)
except Exception as e:
    logging.error(f"Error creating DataFrame: {e}")
    df = None

if df is not None:
    # write the DataFrame to cassandra
    try:
        df.write.format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("keyspace","poc") \
            .option("table","emp") \
            .option ("spark.cassandra.auth.username", "cassandra") \
            .option ("spark.cassandra.auth.password", "cassandra") \
            .option("spark.cassandra.connection.host", "13.232.25.194") \
            .save()
        logging.error("Data inserted into cassandra.")
    except Exception as e:
        logging.error(f"Error inserting data into cassandra: {e}")
else:
    logging.error("Failed to create DataFrame.")
