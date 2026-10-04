"""Python driver -> Cassandra: an anomaly row round-trips unchanged."""

from datetime import datetime, timezone


def test_anomaly_round_trip(cassandra_session):
    event_time = datetime(2026, 9, 16, 14, 30, 0, 123000, tzinfo=timezone.utc)
    tick_id = "PYTEST_A-1789569000123-0"
    cassandra_session.execute(
        """
        INSERT INTO anomalies (
            ticker, day, event_time, tick_id, price, volume, zscore, vwap_divergence,
            is_anomaly, anomaly_type, risk_score, signals, incident_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            "PYTEST_A", event_time.date(), event_time, tick_id, 180.25, 5000.0, 4.21, 0.037,
            True, "price_shock", 1.25, "vwap", "PYTEST_A-1789569000123",
        ),
    )

    rows = list(cassandra_session.execute(
        "SELECT * FROM anomalies WHERE ticker = %s AND day = %s", ("PYTEST_A", event_time.date())
    ))

    assert len(rows) == 1
    row = rows[0]
    # The driver returns timestamps as naive UTC datetimes.
    assert row.event_time == event_time.replace(tzinfo=None)
    assert row.tick_id == tick_id
    assert (row.price, row.volume, row.zscore, row.vwap_divergence) == (180.25, 5000.0, 4.21, 0.037)
    assert row.is_anomaly is True
    assert row.anomaly_type == "price_shock"
    assert row.risk_score == 1.25
    assert row.signals == "vwap"
    assert row.incident_id == "PYTEST_A-1789569000123"
    # Columns not written stay null rather than defaulting.
    assert row.ewma_divergence is None and row.cusum_price is None
