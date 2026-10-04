"""The (ticker, day) partitioning: rows land in their UTC day's partition,
and the dashboard client reads a window that crosses UTC midnight from both
days' partitions."""

import os
from datetime import datetime, timedelta, timezone

from persistence.alerting.config import AlertConfig

MIDNIGHT = datetime(2026, 9, 17, 0, 0, 0, tzinfo=timezone.utc)
# ten ticks a minute apart, five on each side of midnight
TICKS = [MIDNIGHT + timedelta(minutes=m) for m in range(-5, 5)]


def insert(session, table, ticker, ts, extra=""):
    columns = "ticker, day, event_time, tick_id, price, volume" + (", " + extra if extra else "")
    values = [ticker, ts.date(), ts, f"{ticker}-{int(ts.timestamp() * 1000)}-0", 100.0, 10.0]
    if extra:
        values += [True, "price_shock", 1.5]
    marks = ", ".join(["%s"] * len(values))
    session.execute(f"INSERT INTO {table} ({columns}) VALUES ({marks})", values)


def client():
    from persistence.persistence_client import CassandraClient

    return CassandraClient(AlertConfig(
        cassandra_host=os.getenv("CASSANDRA_HOST", "127.0.0.1"),
        cassandra_port=int(os.getenv("CASSANDRA_PORT", "9042")),
        cassandra_keyspace=os.environ["CASSANDRA_KEYSPACE"],  # conftest's throwaway keyspace
    ))


def test_rows_land_in_their_utc_days_partition(cassandra_session):
    for ts in TICKS:
        insert(cassandra_session, "raw_events", "PYTEST_D1", ts)
    before = MIDNIGHT.date() - timedelta(days=1)
    counts = {
        day: cassandra_session.execute(
            "SELECT count(*) FROM raw_events WHERE ticker = %s AND day = %s", ("PYTEST_D1", day)).one()[0]
        for day in (before, MIDNIGHT.date())
    }
    assert counts == {before: 5, MIDNIGHT.date(): 5}


def test_client_reads_a_window_across_midnight(cassandra_session):
    for ts in TICKS:
        insert(cassandra_session, "raw_events", "PYTEST_D2", ts)
        insert(cassandra_session, "anomalies", "PYTEST_D2", ts, extra="is_anomaly, anomaly_type, risk_score")
    c = client()
    window = (MIDNIGHT - timedelta(minutes=3), MIDNIGHT + timedelta(minutes=10))

    raw = c._raw_since("PYTEST_D2", *window)
    assert [r.event_time for r in raw] == [t.replace(tzinfo=None) for t in TICKS[2:]]  # oldest first

    anomalies = c._anomalies_since("PYTEST_D2", *window, limit=100)
    assert [r.event_time for r in anomalies] == [t.replace(tzinfo=None) for t in reversed(TICKS[2:])]
    # the limit stops at the newest rows, which are in the later day
    assert len(c._anomalies_since("PYTEST_D2", *window, limit=3)) == 3

    # the newest row is found through the newest day partition
    assert c._latest_event_time("raw_events", "PYTEST_D2") == TICKS[-1]
    assert c._latest_event_time("raw_events", "PYTEST_NONE") is None
