"""Cassandra sinks for the Objective 2 Structured Streaming output.

The refined detector appends risk and incident metadata to the original
eight-field contract. These helpers accept both layouts so an older producer
can still be drained while checkpoints and deployments are upgraded.
"""

import os
import time
from datetime import datetime, timezone
from itertools import chain


ANOMALY_COLUMNS = (
    "ticker",
    "event_time",
    "tick_id",
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
)

_OPTIONAL_TYPES = {
    "zscore": "double",
    "vwap_divergence": "double",
    "anomaly_type": "string",
    "ewma_divergence": "double",
    "cusum_price": "double",
    "cusum_volume": "double",
    "risk_score": "double",
    "signals": "string",
    "incident_id": "string",
    "tick_id": "string",
}


def prepare_detector_output(batch_df):
    """Normalize either detector contract to the refined Cassandra shape."""
    from pyspark.sql import functions as F

    # Cassandra-driver receives Python ``datetime`` values from executors.
    # Force Spark's conversion boundary to UTC so hosts in non-UTC zones do
    # not silently shift the event-time clustering key.
    batch_df.sparkSession.conf.set("spark.sql.session.timeZone", "UTC")
    df = batch_df
    if "timestamp" in df.columns:
        df = df.withColumnRenamed("timestamp", "event_time")
    elif "event_time" not in df.columns:
        raise ValueError("Objective 2 output must contain timestamp or event_time")

    for name, spark_type in _OPTIONAL_TYPES.items():
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast(spark_type))

    # Old Objective 2 rows did not carry a stable identifier. Generate the
    # same shape as the refined detector so the Cassandra primary key remains
    # non-null during a rolling upgrade. Refined rows keep their supplied id.
    event_time_ms = (F.col("event_time").cast("double") * F.lit(1000)).cast("long")
    fallback_tick_id = F.concat_ws(
        "-",
        F.col("ticker"),
        event_time_ms,
        F.lit("0"),
    )
    return (
        df.withColumn("_event_time_ms", event_time_ms)
        .withColumn("tick_id", F.coalesce(F.col("tick_id"), fallback_tick_id))
    )


def _cassandra_values(row, columns):
    """Build driver values without passing timezone-naive Spark datetimes."""
    event_time = datetime.fromtimestamp(row["_event_time_ms"] / 1000.0, timezone.utc)
    return tuple(event_time if name == "event_time" else row[name] for name in columns)


# Inserts in flight at once per partition, and the per-insert timeout. One
# synchronous insert at a time timed out on a full day of ticks against a
# single small Cassandra node; concurrent inserts with a longer timeout don't.
WRITE_CONCURRENCY = int(os.getenv("CASSANDRA_WRITE_CONCURRENCY", "64"))
WRITE_TIMEOUT_S = float(os.getenv("CASSANDRA_WRITE_TIMEOUT", "30"))


def _execute_all(session, statement, params):
    from cassandra.concurrent import execute_concurrent_with_args

    session.default_timeout = WRITE_TIMEOUT_S
    execute_concurrent_with_args(
        session, statement, params, concurrency=WRITE_CONCURRENCY, raise_on_first_error=True
    )


# One session per Python worker process, reused across partitions and
# micro-batches (Spark reuses its Python workers). Connecting per partition
# meant a DNS lookup and a new connection pool thousands of times per run,
# and a single failed lookup killed the streaming query.
_SESSIONS = {}
CONNECT_ATTEMPTS = 5


def _session():
    from cassandra.cluster import Cluster

    key = _connection_settings()
    session = _SESSIONS.get(key)
    if session is not None and not session.is_shutdown:
        return session
    host, port, keyspace = key
    for attempt in range(CONNECT_ATTEMPTS):
        cluster = Cluster([host], port=port)
        try:
            session = cluster.connect(keyspace)
            break
        except Exception:
            cluster.shutdown()
            if attempt == CONNECT_ATTEMPTS - 1:
                raise
            time.sleep(2 ** attempt)  # 1, 2, 4, 8 s: rides out a brief DNS or network blip
    _SESSIONS[key] = session
    return session


def _connection_settings():
    return (
        os.getenv("CASSANDRA_HOST", "127.0.0.1"),
        int(os.getenv("CASSANDRA_PORT", "9042")),
        os.getenv("CASSANDRA_KEYSPACE", "surveillance"),
    )


def write_anomalies_to_cassandra(batch_df, batch_id):
    """Write flagged refined-detector rows to ``surveillance.anomalies``."""
    print(f"Processing anomaly batch {batch_id}")

    anomalies_df = (
        prepare_detector_output(batch_df)
        .filter("is_anomaly = true")
        .select(*ANOMALY_COLUMNS, "_event_time_ms")
    )

    def write_partition(rows):
        rows = iter(rows)
        try:
            first_row = next(rows)
        except StopIteration:
            return

        session = _session()
        insert_statement = session.prepare(
            """
            INSERT INTO anomalies (
                ticker, event_time, tick_id, price, volume,
                zscore, vwap_divergence, is_anomaly, anomaly_type,
                ewma_divergence, cusum_price, cusum_volume,
                risk_score, signals, incident_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        _execute_all(
            session,
            insert_statement,
            (_cassandra_values(row, ANOMALY_COLUMNS) for row in chain((first_row,), rows)),
        )

    anomalies_df.foreachPartition(write_partition)
    print(f"Batch {batch_id} written to Cassandra.")


def write_raw_events_to_cassandra(batch_df, batch_id):
    """Write every processed market event to ``surveillance.raw_events``."""
    print(f"Processing raw event batch {batch_id}")

    raw_columns = ("ticker", "event_time", "tick_id", "price", "volume")
    raw_events_df = prepare_detector_output(batch_df).select(*raw_columns, "_event_time_ms")

    def write_partition(rows):
        rows = iter(rows)
        try:
            first_row = next(rows)
        except StopIteration:
            return

        session = _session()
        insert_statement = session.prepare(
            """
            INSERT INTO raw_events (ticker, event_time, tick_id, price, volume)
            VALUES (?, ?, ?, ?, ?)
            """
        )
        _execute_all(
            session,
            insert_statement,
            (_cassandra_values(row, raw_columns) for row in chain((first_row,), rows)),
        )

    raw_events_df.foreachPartition(write_partition)
    print(f"Raw event batch {batch_id} written to Cassandra.")
