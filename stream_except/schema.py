from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType, FloatType, DateType, DoubleType
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
