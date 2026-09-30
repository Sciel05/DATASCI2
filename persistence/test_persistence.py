import os
from cassandra.cluster import Cluster
from datetime import datetime, timezone


# Connect to Cassandra running in Docker
cluster = Cluster(
    [os.getenv("CASSANDRA_HOST", "127.0.0.1")],
    port=int(os.getenv("CASSANDRA_PORT", "9042")),
)

# Connect directly to our project keyspace
session = cluster.connect("surveillance")

print("Connected to Cassandra successfully.")


# Create a test anomaly similar to what Task 2 will eventually produce
event_time = datetime.now(timezone.utc)

insert_anomaly = session.prepare("""
    INSERT INTO anomalies (
        ticker,
        event_time,
        tick_id,
        price,
        volume,
        zscore,
        vwap_divergence,
        is_anomaly,
        anomaly_type,
        risk_score,
        signals,
        incident_id
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

session.execute(
    insert_anomaly,
    (
        "AAPL",
        event_time,
        f"AAPL-{int(event_time.timestamp() * 1000)}-0",
        180.25,
        5000.0,
        4.21,
        0.037,
        True,
        "price_shock",
        1.25,
        "vwap",
        f"AAPL-{int(event_time.timestamp() * 1000)}",
    )
)

print("Test anomaly inserted successfully.")


# Read the data back from Cassandra
select_anomalies = session.prepare("""
    SELECT *
    FROM anomalies
    WHERE ticker = ?
    LIMIT 5
""")

rows = session.execute(select_anomalies, ("AAPL",))

print("\nStored AAPL anomalies:")

for row in rows:
    print(row)


# Close the Cassandra connection
cluster.shutdown()

print("\nConnection closed.")
