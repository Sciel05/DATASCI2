import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    when,
)

from cassandra_sink import (
    write_anomalies_to_cassandra,
    write_raw_events_to_cassandra,
)


spark = (
    SparkSession.builder
    .appName("StreamingPersistenceTest")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Spark's built-in "rate" source continuously creates test rows.
stream_df = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 2)
    .load()
)


# Convert the generated stream into fake Task 2 output.
market_stream = (
    stream_df
    .withColumn(
        "ticker",
        when(col("value") % 2 == 0, "AAPL").otherwise("MSFT")
    )
    .withColumn("price", lit(250.0) + col("value"))
    .withColumn("volume", lit(500.0))
    .withColumn("zscore", when(col("value") % 2 == 0, 4.21).otherwise(1.20))
    .withColumn(
        "vwap_divergence",
        when(col("value") % 2 == 0, 0.037).otherwise(0.005)
    )
    .withColumn("is_anomaly", col("value") % 2 == 0)
    .withColumn(
        "anomaly_type",
        when(col("value") % 2 == 0, "price_shock").otherwise("normal")
    )
    .select(
        "ticker",
        "timestamp",
        "price",
        "volume",
        "zscore",
        "vwap_divergence",
        "is_anomaly",
        "anomaly_type",
    )
)


def persist_batch(batch_df, batch_id):
    print(f"\n=== Streaming batch {batch_id} ===")

    write_raw_events_to_cassandra(batch_df, batch_id)
    write_anomalies_to_cassandra(batch_df, batch_id)


query = (
    market_stream.writeStream
    .foreachBatch(persist_batch)
    .trigger(processingTime="2 seconds")
    .start()
)


print("Streaming test started...")

time.sleep(8)

query.stop()
spark.stop()

print("\nStructured Streaming → Cassandra test completed.")