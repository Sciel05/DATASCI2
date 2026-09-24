CREATE KEYSPACE IF NOT EXISTS surveillance
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
}
AND durable_writes = true;

USE surveillance;

CREATE TABLE IF NOT EXISTS raw_events (
    ticker text,
    event_time timestamp,
    price double,
    volume double,
    side text,
    PRIMARY KEY ((ticker), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);

CREATE TABLE IF NOT EXISTS anomalies (
    ticker text,
    event_time timestamp,
    zscore double,
    vwap_divergence double,
    anomaly_type text,
    PRIMARY KEY ((ticker), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);