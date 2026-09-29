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
    tick_id text,
    price double,
    volume double,
    side text,
    PRIMARY KEY ((ticker), event_time, tick_id)
) WITH CLUSTERING ORDER BY (event_time DESC, tick_id ASC);

CREATE TABLE IF NOT EXISTS anomalies (
    ticker text,
    event_time timestamp,
    tick_id text,
    price double,
    volume double,
    zscore double,
    vwap_divergence double,
    is_anomaly boolean,
    anomaly_type text,
    ewma_divergence double,
    cusum_price double,
    cusum_volume double,
    risk_score double,
    signals text,
    incident_id text,
    PRIMARY KEY ((ticker), event_time, tick_id)
) WITH CLUSTERING ORDER BY (event_time DESC, tick_id ASC);
