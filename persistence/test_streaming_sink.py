"""Spark Structured Streaming -> foreachBatch -> Cassandra.

A fixed input (three parquet files, read one per micro-batch) instead of
the rate source and a sleep: the query runs until every file is processed
(availableNow), so the test knows exactly which rows must be stored.
"""

import os
from datetime import datetime, timedelta, timezone

import pytest

from persistence.cassandra_sink import (
    write_anomalies_to_cassandra,
    write_raw_events_to_cassandra,
)

pytestmark = pytest.mark.skipif(
    os.name == "nt" and not os.getenv("HADOOP_HOME"),
    reason="streaming checkpoints on Windows need winutils (HADOOP_HOME); run in the Docker stack",
)

START = datetime(2026, 9, 16, 14, 0, 0, tzinfo=timezone.utc)
N_FILES, TICKS_PER_FILE = 3, 3


def tick(i):
    ticker = "PYTEST_S1" if i % 2 == 0 else "PYTEST_S2"
    ts = START + timedelta(milliseconds=250 * i)
    ms = int(ts.timestamp() * 1000)
    flagged = i % 3 == 0
    return {
        "ticker": ticker,
        "timestamp": ts,
        "price": 250.0 + i,
        "volume": 500.0 + i,
        "zscore": 5.5 if flagged else 0.4,
        "vwap_divergence": 0.02 if flagged else 0.001,
        "is_anomaly": flagged,
        "anomaly_type": "price_shock" if flagged else None,
        "ewma_divergence": 1.0,
        "cusum_price": 0.0,
        "cusum_volume": 0.0,
        "risk_score": 1.1 if flagged else 0.08,
        "signals": "zscore" if flagged else None,
        "incident_id": f"{ticker}-{ms}" if flagged else None,
        "tick_id": f"{ticker}-{ms}-0",
    }


def test_stream_persists_every_tick_across_micro_batches(spark, cassandra_session, tmp_path):
    import pandas as pd

    ticks = [tick(i) for i in range(N_FILES * TICKS_PER_FILE)]
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    for f in range(N_FILES):
        chunk = pd.DataFrame(ticks[f * TICKS_PER_FILE:(f + 1) * TICKS_PER_FILE])
        # Spark can't read pandas' default nanosecond timestamps.
        chunk.to_parquet(input_dir / f"part-{f}.parquet", index=False, coerce_timestamps="us")

    schema = spark.read.parquet(str(input_dir)).schema
    batches = []

    def persist_batch(batch_df, batch_id):
        batches.append(batch_df.count())
        write_raw_events_to_cassandra(batch_df, batch_id)
        write_anomalies_to_cassandra(batch_df, batch_id)

    query = (
        spark.readStream.schema(schema)
        .option("maxFilesPerTrigger", 1)
        .parquet(str(input_dir))
        .writeStream.foreachBatch(persist_batch)
        .option("checkpointLocation", str(tmp_path / "checkpoint"))
        .trigger(availableNow=True)
        .start()
    )
    assert query.awaitTermination(timeout=180), "streaming query did not finish"
    assert query.exception() is None

    # One micro-batch per file, so the sink ran across batch boundaries.
    assert batches == [TICKS_PER_FILE] * N_FILES

    stored_raw = {
        r.tick_id: (r.price, r.volume)
        for ticker in ("PYTEST_S1", "PYTEST_S2")
        for r in cassandra_session.execute("SELECT * FROM raw_events WHERE ticker = %s", (ticker,))
    }
    assert stored_raw == {t["tick_id"]: (t["price"], t["volume"]) for t in ticks}

    stored_anomalies = {
        r.tick_id: (r.anomaly_type, r.zscore, r.incident_id)
        for ticker in ("PYTEST_S1", "PYTEST_S2")
        for r in cassandra_session.execute("SELECT * FROM anomalies WHERE ticker = %s", (ticker,))
    }
    assert stored_anomalies == {
        t["tick_id"]: (t["anomaly_type"], t["zscore"], t["incident_id"])
        for t in ticks if t["is_anomaly"]
    }
