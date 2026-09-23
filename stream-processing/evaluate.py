from pyspark.sql import SparkSession
from schema import tick_schema
from detection import detect_anomalies

spark = SparkSession.builder.appName("Evaluate").getOrCreate()

df = spark.read.csv("../data-ingestion/aapl_msft_googl_tsla_nvda_ticks_labeled.csv", header=True, schema=tick_schema)

detection_input = df.drop("label")
result = detect_anomalies(detection_input)

joined = result.join(df.select("ticker", "event_time_ms", "label"), on=["ticker", "event_time_ms"])
joined.groupBy("label", "is_anomaly").count().show()