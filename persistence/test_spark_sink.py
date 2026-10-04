"""Spark batch -> Cassandra through the sink functions the detector uses."""

from datetime import datetime, timezone

from persistence.cassandra_sink import (
    write_anomalies_to_cassandra,
    write_raw_events_to_cassandra,
)

COLUMNS = [
    "ticker", "timestamp", "price", "volume", "zscore", "vwap_divergence",
    "is_anomaly", "anomaly_type", "ewma_divergence", "cusum_price",
    "cusum_volume", "risk_score", "signals", "incident_id", "tick_id",
]

EVENT_TIME = datetime(2026, 9, 16, 14, 30, 0, 250000, tzinfo=timezone.utc)
MS = int(EVENT_TIME.timestamp() * 1000)


def detector_rows():
    return [
        ("PYTEST_B1", EVENT_TIME, 251.50, 500.0, 4.21, 0.037, True, "price_shock",
         2.8, 6.2, 0.0, 1.35, "vwap,cusum_price", f"PYTEST_B1-{MS}", f"PYTEST_B1-{MS}-0"),
        ("PYTEST_B2", EVENT_TIME, 420.10, 200.0, 1.20, 0.005, False, None,
         0.2, 0.0, 0.0, 0.24, None, None, f"PYTEST_B2-{MS}-0"),
    ]


def select(session, table, ticker):
    """The ticker's rows in EVENT_TIME's (ticker, day) partition."""
    return list(session.execute(f"SELECT * FROM {table} WHERE ticker = %s AND day = %s",
                                (ticker, EVENT_TIME.date())))


def test_batch_writes_all_ticks_and_only_flagged_anomalies(spark, cassandra_session):
    batch_df = spark.createDataFrame(detector_rows(), COLUMNS)

    # Written twice, as a retried micro-batch would be: tick_id is part of
    # both primary keys, so the retry overwrites instead of duplicating.
    for _ in range(2):
        write_raw_events_to_cassandra(batch_df, 1)
        write_anomalies_to_cassandra(batch_df, 1)

    raw_b1 = select(cassandra_session, "raw_events", "PYTEST_B1")
    raw_b2 = select(cassandra_session, "raw_events", "PYTEST_B2")
    assert [(r.tick_id, r.price, r.volume) for r in raw_b1] == [(f"PYTEST_B1-{MS}-0", 251.50, 500.0)]
    assert [(r.tick_id, r.price, r.volume) for r in raw_b2] == [(f"PYTEST_B2-{MS}-0", 420.10, 200.0)]
    assert raw_b1[0].event_time == EVENT_TIME.replace(tzinfo=None)
    # the sink derives the day partition from the UTC event time
    assert raw_b1[0].day.date() == EVENT_TIME.date()

    anomalies = select(cassandra_session, "anomalies", "PYTEST_B1")
    assert len(anomalies) == 1
    a = anomalies[0]
    assert a.event_time == EVENT_TIME.replace(tzinfo=None)
    assert a.day.date() == EVENT_TIME.date()
    assert a.tick_id == f"PYTEST_B1-{MS}-0"
    assert (a.price, a.volume, a.zscore, a.vwap_divergence) == (251.50, 500.0, 4.21, 0.037)
    assert (a.ewma_divergence, a.cusum_price, a.cusum_volume, a.risk_score) == (2.8, 6.2, 0.0, 1.35)
    assert a.is_anomaly is True
    assert a.anomaly_type == "price_shock"
    assert a.signals == "vwap,cusum_price"
    assert a.incident_id == f"PYTEST_B1-{MS}"

    # The unflagged tick is stored as a raw event only.
    assert select(cassandra_session, "anomalies", "PYTEST_B2") == []
