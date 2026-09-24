from datetime import datetime

from pyspark.sql import SparkSession

from cassandra_sink import (
    write_anomalies_to_cassandra,
    write_raw_events_to_cassandra,
)
spark = (
    SparkSession.builder
    .appName("PersistenceSinkTest")
    .master("local[*]")
    .config("spark.cassandra.connection.host", "127.0.0.1")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()
)


test_data = [
    (
        "AAPL",
        datetime.now(),
        251.50,
        500.0,
        4.21,
        0.037,
        True,
        "price_shock",
    ),
    (
        "MSFT",
        datetime.now(),
        420.10,
        200.0,
        1.20,
        0.005,
        False,
        "normal",
    ),
]


columns = [
    "ticker",
    "timestamp",
    "price",
    "volume",
    "zscore",
    "vwap_divergence",
    "is_anomaly",
    "anomaly_type",
]


batch_df = spark.createDataFrame(test_data, columns)

print("\nFake Task 2 output:")
batch_df.show(truncate=False)

write_anomalies_to_cassandra(batch_df, 1)
write_raw_events_to_cassandra(batch_df, 1)

print("\nSpark → Cassandra test completed.")

spark.stop()