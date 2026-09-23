from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

tick_schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("event_time_ms", LongType(), True),
    StructField("price", DoubleType(), True),
    StructField("bid", DoubleType(), True),
    StructField("ask", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("label", StringType(), True),
])