# Persistence & Alerting (Objective 3)

**Status: Persistence, Cassandra streaming sinks, Alerting engine, and Surveillance Dashboard implemented and verified.**

This module handles distributed persistence, real-time alert triage, and surveillance visualization for the Real-Time Market Surveillance Pipeline.

It consumes processed market data from `stream-processing/` (Objective 2) and provides:
- High-throughput Cassandra persistence for all market events (`raw_events`)
- Filtered Cassandra persistence for statistical anomalies (`anomalies`)
- Sub-second alerting engine with burst debouncing, severity tiers, and webhook/audit logging
- High-density Streamlit financial surveillance desk with interactive timeline co-plots and asset health matrices

---

## Architecture

```text
       +---------------------------------------------+
       |   Spark Structured Streaming (Objective 2)  |
       +---------------------------------------------+
                              |
                              v
                       foreachBatch()
                              |
              +---------------+---------------+
              |                               |
              v                               v
     write_raw_events()              write_anomalies()
              |                               |
              v                               v
     Cassandra: raw_events           Cassandra: anomalies
     (all market ticks)              (is_anomaly = true)
                                              |
                                              v
                              +-------------------------------+
                              |    PersistenceClient (O(1))   |
                              |  (Partition-scoped queries)   |
                              +-------------------------------+
                                              |
                        +---------------------+---------------------+
                        |                                           |
                        v                                           v
             +--------------------+                      +--------------------+
             |   Alerting Daemon  |                      | Surveillance Desk  |
             |  (run_alerting.py) |                      |  (dashboard/app)   |
             +--------------------+                      +--------------------+
              |        |        |                         |         |
              v        v        v                         v         v
           Console  alerts.log Webhooks               KPI Cards  Co-Plots
```

---

## Directory Structure & Files

```text
persistence/
├── README.md                      # Unified architecture, setup, and run guide
├── requirements.txt               # All module dependencies (Cassandra, PySpark, Streamlit, Plotly)
├── schema.cql                     # Cassandra keyspace and table definitions
├── cassandra_sink.py              # Spark micro-batch partition sinks (raw_events & anomalies)
├── persistence_client.py          # O(1) Cassandra partition client + CSV mock simulator
├── run_alerting.py                # Standalone background alerting daemon
├── test_persistence.py            # Direct Python -> Cassandra insertion & query test
├── test_spark_sink.py             # Spark DataFrame -> Cassandra batch persistence test
├── test_streaming_sink.py         # Spark Structured Streaming -> foreachBatch -> Cassandra test
├── test_alerting_pipeline.py      # Unit tests for debouncing, cooldown, severity, and mock math
├── alerting/                      # Real-time alert processing package
│   ├── __init__.py
│   ├── config.py                  # Thresholds, cooldowns, and cluster connection settings
│   ├── alert_engine.py            # Severity classification, burst debouncing, latency tracking
│   └── notifier.py                # Stdout formatting, JSON-lines log, Discord/Slack webhooks
└── dashboard/                     # Financial surveillance desk
    ├── __init__.py
    └── app.py                     # Streamlit application (dark terminal theme, Plotly co-plots)
```

---

## Environment & Requirements

Tested across:
- **Cassandra:** 4.1.12 (Docker)
- **PySpark:** 3.5.9
- **Java:** OpenJDK 17
- **Python:** 3.10+ / 3.12
- **Streamlit:** 1.42+
- **Plotly:** 6.0+

Install dependencies:
```powershell
python -m pip install -r persistence/requirements.txt
```

---

## Database Setup (Apache Cassandra)

### 1. Keyspace & Table Schema (`persistence/schema.cql`)

```sql
CREATE KEYSPACE IF NOT EXISTS surveillance
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
} AND durable_writes = true;

USE surveillance;

-- All processed market events
CREATE TABLE IF NOT EXISTS raw_events (
    ticker text,
    event_time timestamp,
    price double,
    volume double,
    side text,
    PRIMARY KEY ((ticker), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);

-- Detected statistical anomalies
CREATE TABLE IF NOT EXISTS anomalies (
    ticker text,
    event_time timestamp,
    price double,
    volume double,
    zscore double,
    vwap_divergence double,
    anomaly_type text,
    PRIMARY KEY ((ticker), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);
```

Both tables use `ticker` as the partition key and `event_time DESC` as the clustering key, enabling $O(1)$ partition lookups ordered chronologically without requiring multi-partition full scans or `ALLOW FILTERING`.

### 2. Initialize Cassandra in Docker

Copy and apply the schema to the running Cassandra container:
```powershell
docker cp .\persistence\schema.cql surveillance-cassandra:/schema.cql
docker exec -it surveillance-cassandra cqlsh -f /schema.cql
```

Verify tables in `cqlsh`:
```sql
DESCRIBE KEYSPACE surveillance;
```

---

## Running the Components

### 1. Surveillance Dashboard (Streamlit Web UI)

From the project root:
```powershell
streamlit run persistence/dashboard/app.py
```

The web dashboard opens at `http://localhost:8501`.

- **Mock Simulator Mode (Default):** Runs an advancing replay clock over the labeled CSV dataset, calculating real rolling statistics and triggering genuine simulated anomalies. Ideal for local development, grading review, and demos without needing a running Cassandra container.
- **Live Cassandra Mode:** Set the toggle in the sidebar or launch with:
  ```powershell
  $env:USE_MOCK="False"; streamlit run persistence/dashboard/app.py
  ```

### 2. Alerting Daemon (Background Process)

The alerting poller continuously monitors for newly persisted anomalies, debounces rapid bursts, classifies severity, and dispatches notifications:

```powershell
# Run with Mock Simulator (offline ground-truth replay)
python persistence/run_alerting.py

# Run against Live Cassandra
python persistence/run_alerting.py --live

# Specify custom poll interval and lookback
python persistence/run_alerting.py --interval 2.0 --lookback 10
```

Alerts are formatted to `stdout` and appended as structured JSON-lines records to `persistence/alerts.log`.

---

## Testing & Verification Suite

### Persistence Layer Tests

1. **Direct Python -> Cassandra:**
   ```powershell
   python persistence/test_persistence.py
   ```
   Validates basic Cassandra connection, prepared statement insertion, and partition query.

2. **Spark Batch DataFrame -> Cassandra:**
   ```powershell
   python persistence/test_spark_sink.py
   ```
   Validates schema mapping, `is_anomaly = true` filtering, and partition writing via PySpark.

3. **Spark Structured Streaming -> foreachBatch -> Cassandra:**
   ```powershell
   # Run inside Spark 3.5.9 Docker container
   python persistence/test_streaming_sink.py
   ```
   Validates continuous streaming micro-batches, checkpointing, and foreachPartition persistence.

### Alerting & Dashboard Tests

4. **Alerting Pipeline Unit Tests:**
   ```powershell
   python persistence/test_alerting_pipeline.py -v
   ```
   Validates:
   - `test_burst_debouncing_and_escalation`: Multi-tick burst aggregation and severity escalation.
   - `test_cooldown_enforcement`: Suppression of redundant alerts during active cooldown windows.
   - `test_mock_client_math_grounding`: Rolling statistics, Z-score, and VWAP divergence calculations.
   - `test_notifier_audit_logging`: Structured JSON record integrity and latency fields.
   - `test_severity_classification`: Threshold boundaries for CRITICAL, WARNING, and INFO tiers.

---

## Key Architecture & Design Highlights

1. **Partition-Scoped Cassandra Queries:**
   Instead of cluster-wide table scans (`SELECT * FROM anomalies WHERE event_time >= ? ALLOW FILTERING`), queries are strictly partitioned by ticker (`WHERE ticker = ? AND event_time >= ? LIMIT N`). This ensures constant $O(1)$ partition retrieval time even as the database scales to millions of records.
2. **Burst Debouncing / Incident Aggregation:**
   High-frequency streaming can flag dozens of consecutive ticks during a single price shock. The alerting engine aggregates anomalous ticks within a configurable time window (default 10s) into a unified incident, preserving peak metrics while preventing alert fatigue.
3. **End-to-End Latency Tracking for Objective 4:**
   Every generated alert records `detection_latency_ms = (alert_created_at - anomaly_event_time)`, providing direct data points for median and p95 streaming latency evaluation.
4. **Resilient Dual-Mode Client:**
   `PersistenceClient` dynamically inspects Cassandra schema metadata to adapt query fields gracefully, while providing automatic in-process mock replay fallback for zero-dependency local runs.
