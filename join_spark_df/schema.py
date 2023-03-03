from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType, FloatType, DateType, DoubleType
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