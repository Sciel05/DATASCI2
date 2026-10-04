-- UPGRADE NOTE (2026-10-04), for teammates and coding agents: both tables
-- are now partitioned by (ticker, day) instead of (ticker). Cassandra can't
-- change a primary key in place, so a keyspace created before this change
-- has to be recreated, which DELETES its data:
--
--     python -m persistence.schema_tools --reset     (from the repo root)
--
-- In the Docker stack: docker compose -f stream-processing/docker/docker-compose.yml
--     exec -w /workspace pyspark python3 -m persistence.schema_tools --reset
-- Until then the detector (--cassandra-sink) stops at start-up with
-- "raw_events is missing ['day']" and the dashboard can't connect
-- ("Undefined column name day"). Both messages point back here. The data
-- is replayable test data: re-run data-ingestion/producer.py to refill it.
-- Don't work around the error by reverting the key or adding ALLOW FILTERING.

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

-- Partitioned by (ticker, day): one partition per ticker per UTC day, so
-- a partition stops growing at the end of its day instead of holding a
-- ticker's whole history. day is the UTC date of event_time (a US session,
-- 13:30-20:00 UTC, never crosses UTC midnight). Readers name the day(s)
-- their time window covers (see persistence_client.py).
CREATE TABLE IF NOT EXISTS raw_events (
    ticker text,
    day date,
    event_time timestamp,
    tick_id text,
    price double,
    volume double,
    side text,
    PRIMARY KEY ((ticker, day), event_time, tick_id)
) WITH CLUSTERING ORDER BY (event_time DESC, tick_id ASC);

CREATE TABLE IF NOT EXISTS anomalies (
    ticker text,
    day date,
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
    PRIMARY KEY ((ticker, day), event_time, tick_id)
) WITH CLUSTERING ORDER BY (event_time DESC, tick_id ASC);
