-- 3 replicas of every row in the one datacenter: on the 3-node Docker
-- cluster each node holds a full copy, and LOCAL_QUORUM (2 of 3) reads and
-- writes keep working with any one node down. NetworkTopologyStrategy, not
-- SimpleStrategy, so a second datacenter can be added without changing
-- the keyspace's strategy. 'datacenter1' is Cassandra's default DC name.
-- schema_tools.py lowers the factor on a smaller cluster (e.g. 1 node).
CREATE KEYSPACE IF NOT EXISTS surveillance
WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'datacenter1': 3
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
