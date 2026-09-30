import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from persistence.cassandra_sink import (
    write_anomalies_to_cassandra,
    write_raw_events_to_cassandra,
)
spark = (
    SparkSession.builder
    .appName("PersistenceSinkTest")
    .master("local[1]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.cassandra.connection.host", "127.0.0.1")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()
)


event_time = datetime.now(timezone.utc)

test_data = [
    (
        "AAPL",
        event_time,
        251.50,
        500.0,
        4.21,
        0.037,
        True,
        "price_shock",
        2.8,
        6.2,
        0.0,
        1.35,
        "vwap,cusum_price",
        f"AAPL-{int(event_time.timestamp() * 1000)}",
        f"AAPL-{int(event_time.timestamp() * 1000)}-0",
    ),
    (
        "MSFT",
        event_time,
        420.10,
        200.0,
        1.20,
        0.005,
        False,
        "normal",
        0.2,
        0.0,
        0.0,
        0.24,
        None,
        None,
        f"MSFT-{int(event_time.timestamp() * 1000)}-0",
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
    "ewma_divergence",
    "cusum_price",
    "cusum_volume",
    "risk_score",
    "signals",
    "incident_id",
    "tick_id",
]


batch_df = spark.createDataFrame(test_data, columns)

print("\nFake Task 2 output:")
batch_df.show(truncate=False)

write_anomalies_to_cassandra(batch_df, 1)
write_raw_events_to_cassandra(batch_df, 1)

# Verify the appended Objective 2 fields made it through Spark and Cassandra.
from cassandra.cluster import Cluster

cluster = Cluster(
    [os.getenv("CASSANDRA_HOST", "127.0.0.1")],
    port=int(os.getenv("CASSANDRA_PORT", "9042")),
)
try:
    session = cluster.connect("surveillance")
    stored_event_time = event_time.replace(
        microsecond=(event_time.microsecond // 1000) * 1000
    )
    stored = session.execute(
        "SELECT risk_score, signals, incident_id FROM anomalies "
        "WHERE ticker = %s AND event_time = %s AND tick_id = %s",
        (
            "AAPL",
            stored_event_time,
            f"AAPL-{int(event_time.timestamp() * 1000)}-0",
        ),
    ).one()
    assert stored is not None
    assert stored.risk_score == 1.35
    assert stored.signals == "vwap,cusum_price"
    assert stored.incident_id == f"AAPL-{int(event_time.timestamp() * 1000)}"
finally:
    cluster.shutdown()

print("\nSpark -> Cassandra test completed.")

spark.stop()
