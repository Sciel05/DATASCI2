# Persistence & Alerting (Objective 3)

**Status: Persistence implemented and tested. Alerting/dashboard integration pending.**

This module handles distributed persistence for the Real-Time Market
Surveillance Pipeline.

It consumes processed market data from `stream-processing/` (Objective 2)
and stores:

- all processed market events in Cassandra
- detected anomalies in a separate Cassandra table

Alerting/dashboard integration will consume the stored anomaly records.

## Architecture

```text
Spark Structured Streaming
        |
        v
foreachBatch()
        |
        +--> raw events --> Cassandra: surveillance.raw_events
        |
        +--> anomalies --> Cassandra: surveillance.anomalies


```

The persistence layer uses the Python `cassandra-driver` inside Spark
partitions to write records to Cassandra.

## Environment

Tested with:

- Cassandra 4.1.12
- Docker
- Java 17
- PySpark 3.5.9
- Python 3.12 for local tests
- Spark 3.5.9 Linux Docker image for Structured Streaming tests

## Database Schema

The Cassandra schema is stored in:

```text
persistence/schema.cql
```

It creates the `surveillance` keyspace and two tables:

- `raw_events` - stores all processed market events
- `anomalies` - stores only detected anomalies

Both tables use `ticker` as the partition key and `event_time` as the
clustering key.

The current Objective 2 output does not include a `side` field, so
`raw_events.side` may remain `null` until that field is provided upstream.

## Initialize Cassandra Schema

Copy the schema file into the Cassandra container:

```powershell
docker cp .\persistence\schema.cql surveillance-cassandra:/schema.cql
```

Apply it:

```powershell
docker exec -it surveillance-cassandra cqlsh -f /schema.cql
```

## Python Dependencies

Dependencies are listed in:

```text
persistence/requirements.txt
```

Install them with:

```powershell
python -m pip install -r .\persistence\requirements.txt
```

Current versions include:

```text
cassandra-driver==3.30.1
pyasyncore
pyspark==3.5.9
```

## Persistence Functions

The main persistence code is located in:

```text
persistence/cassandra_sink.py
```

It provides:

```python
write_raw_events_to_cassandra(batch_df, batch_id)
write_anomalies_to_cassandra(batch_df, batch_id)
```

`write_raw_events_to_cassandra()` stores all incoming processed events.

`write_anomalies_to_cassandra()` filters rows where:

```python
is_anomaly == true
```

and stores only detected anomalies.

The Cassandra host can be configured using the `CASSANDRA_HOST`
environment variable.

If it is not set, the default is:

```text
127.0.0.1
```

For Docker-to-Docker communication, it can be set to:

```text
surveillance-cassandra
```

## Tests

### Python to Cassandra

```powershell
python .\persistence\test_persistence.py
```

Tests direct Python insertion and querying of Cassandra.

### Spark Batch to Cassandra

```powershell
python .\persistence\test_spark_sink.py
```

Tests a simulated Objective 2 Spark DataFrame.

The test verifies that:

```text
raw_events:
AAPL -> stored
MSFT -> stored

anomalies:
AAPL -> stored
MSFT -> filtered out
```

### Spark Structured Streaming to Cassandra

```text
persistence/test_streaming_sink.py
```

This test uses Spark's built-in `rate` source to generate a simulated
continuous stream.

It verifies:

```text
Spark Structured Streaming
        |
        v
foreachBatch()
        |
        v
Persistence functions
        |
        v
Cassandra
```

Structured Streaming was tested using the Docker image:

```text
spark:3.5.9-scala2.12-java17-python3-ubuntu
```

Docker was used because local Windows Structured Streaming checkpoint
creation requires additional Hadoop Windows support.

## Verified Results

The following paths have been successfully tested:

```text
Python -> Cassandra
Spark DataFrame -> Cassandra
Spark Structured Streaming -> foreachBatch -> Cassandra
raw event persistence
anomaly filtering
anomaly persistence
persistent Cassandra Docker storage
```

## Remaining Work

The persistence layer is ready for integration with the actual
Objective 2 Spark stream.

Remaining work includes:

- integrate with the final Objective 2 stream
- connect stored anomalies to the alerting/dashboard component
- complete dashboard or notification integration
- perform full end-to-end pipeline testing
