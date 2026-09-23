from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from schema import tick_schema
from detection import detect_anomalies

spark = (
    SparkSession.builder
    .appName("StreamProcessing")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
    .getOrCreate()
)

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "market-ticks")
    .load()
)

parsed = raw.select(F.from_json(F.col("value").cast("string"), tick_schema).alias("data")).select("data.*")
parsed = parsed.withColumn("timestamp", (F.col("event_time_ms") / 1000).cast("timestamp"))

detection_input = parsed.drop("label")

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    result = detect_anomalies(batch_df)
    output = result.select("ticker", "timestamp", "price", "volume", "zscore", "vwap_divergence", "is_anomaly", "anomaly_type")
    output.show(truncate=False)

query = detection_input.writeStream.foreachBatch(process_batch).start()
query.awaitTermination()