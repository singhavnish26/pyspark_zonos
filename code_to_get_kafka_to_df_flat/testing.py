from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession

def create_spark_session():
    spark = SparkSession \
        .builder \
        .appName("Flatten_JSON") \
        .getOrCreate()
    
    sc = spark.sparkContext
    sc.setLogLevel('ERROR')
    return spark


def create_schema():
    return StructType([
        StructField("persistTime", TimestampType()),
        StructField("previous", StructType([
            StructField("meteringPoint", StringType()),
            StructField("group", StringType()),
            StructField("serviceLevel", StringType()),
            StructField("devices", ArrayType(StructType([
                StructField("device", StringType()),
                StructField("assignTime", TimestampType()),
                StructField("removeTime", TimestampType(), True)
            ]))),
            StructField("stateHistory", ArrayType(StructType([
                StructField("state", StringType()),
                StructField("activeSince", TimestampType())
            ]))),
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
                ]))
            ]))
        ])),
        StructField("current", StructType([
            StructField("meteringPoint", StringType()),
            StructField("group", StringType()),
            StructField("serviceLevel", StringType()),
            StructField("devices", ArrayType(StructType([
                StructField("device", StringType()),
                StructField("assignTime", TimestampType()),
                StructField("removeTime", TimestampType(), True)
            ]))),
            StructField("stateHistory", ArrayType(StructType([
                StructField("state", StringType()),
                StructField("activeSince", TimestampType())
            ]))),
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
                ]))
            ]))
        ])
    ])
def read_and_flatten_json(spark, input_path):
    df = spark.read.json(input_path)
    df = df.select(
        col("persistTime").cast("timestamp"),
        from_json(col("previous"), create_schema()).alias("previous"),
        from_json(col("current"), create_schema()).alias("current")
    )
    
    df_previous = df.select("persistTime", "previous.*")
    df_current = df.select("persistTime", "current.*")
    
    df_previous = df_previous.select(
        "persistTime",
        "previous.meteringPoint",
        "previous.group",
        "previous.serviceLevel",
        "previous.location.geo.latitude",
        "previous.location.geo.longitude",
        "previous.location.address.*"
    )
    
    df_current = df_current.select(
        "persistTime",
        "current.meteringPoint",
        "current.group",
        "current.serviceLevel",
        "current.location.geo.latitude",
        "current.location.geo.longitude",
        "current.location.address.*"
    )
    
    df_previous = df_previous.withColumnRenamed("latitude", "previous_latitude") \
                             .withColumnRenamed("longitude", "previous_longitude")
    
    df_current = df_current.withColumnRenamed("latitude", "current_latitude") \
                           .withColumnRenamed("longitude", "current_longitude")
    
    df_final = df_previous.join(df_current, on="persistTime", how="outer")
    return df_final

if __name__ == "__main__":
    spark = create_spark_session()
    df_final = read_and_flatten_json(spark, "input_path")
    df_final.write.parquet("output_path")
    spark.stop()