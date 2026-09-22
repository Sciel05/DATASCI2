# Persistence & Alerting (Objective 3)

**Status: not started.**

Owns: Cassandra schema + sink, and the alert-facing dashboard. Consumes
the anomaly stream produced by `stream-processing/` (Objective 2).

## What this needs to do

Per the proposal's Specific Objective 3 ("Distributed Persistence &
Alerting"):

- Cassandra schema, partitioned by ticker and clustered by event time
  (fast per-asset lookups, time-ordered within each partition):

  ```sql
  CREATE TABLE raw_events (
    ticker text, event_time timestamp, price double, volume double, side text,
    PRIMARY KEY (ticker, event_time)
  );
  CREATE TABLE anomalies (
    ticker text, event_time timestamp, zscore double, vwap_divergence double,
    anomaly_type text,
    PRIMARY KEY (ticker, event_time)
  );
  ```

- Sink from Spark via `foreachBatch` (Structured Streaming doesn't write
  to Cassandra directly):

  ```python
  def write_to_cassandra(batch_df, batch_id):
      batch_df.write.format("org.apache.spark.sql.cassandra") \
          .options(table="anomalies", keyspace="surveillance").mode("append").save()
  query = anomaly_stream.writeStream.foreachBatch(write_to_cassandra).start()
  ```

- A dashboard (Streamlit is the fastest path for an MVP) that polls the
  `anomalies` table every few seconds and surfaces new flags. True
  push-alerts (websocket) are a stretch goal, not required.

## Setup

TBD by whoever picks this up — add Cassandra version, keyspace setup
script, and dashboard run instructions here once decided.
