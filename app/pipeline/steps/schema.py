from pyspark.sql import types as T

DELTA_COLUMNS = ["code", "id", "group_id"]
CACHE_SCHEMA = T.StructType(
    [
        T.StructField("uuid", T.StringType(), False),
        T.StructField("id", T.StringType(), False),
        T.StructField("partition_ref", T.StringType(), False),
    ]
)

DELTA_SCHEMA = T.StructType(
    [
        T.StructField("code", T.StringType(), False),
        T.StructField("id", T.StringType(), False),
        T.StructField("sha2", T.StringType(), False),
    ]
)
