# Persistence & Alerting (Objective 3)

**Owners:** Villanueva, Hector Angelo & Gomugda, Kyle Joniel  
**Last Updated:** 2026-10-02 20:34:00 +08:00  
**Status:** Persistence, refined Objective 2 integration, alerting engine, connection-pooled client, and synchronized multi-pane surveillance dashboard implemented and verified.

---

## Recent Updates (October 2, 2026)

**Update Timestamp:** `2026-10-02 20:34:00 +08:00`  
**Focus:** Objective 3 Live Dashboard Upgrade, VWAP Analytics, & Cassandra Connection Stability

### Key Additions & Improvements:
1. **Synchronized 3-Pane TradingView Lightweight Charts:**
   - Added [`tradingview_component.py`](file:///c:/Users/user/Downloads/DATASCI2/persistence/dashboard/tradingview_component.py) and local chart server [`chart_server.py`](file:///c:/Users/user/Downloads/DATASCI2/persistence/dashboard/chart_server.py) with bundled [`lightweight-charts.js`](file:///c:/Users/user/Downloads/DATASCI2/persistence/dashboard/lightweight-charts.js).
   - Renders synchronized Candlesticks + VWAP, Volume histogram, rolling Z-score oscillator, and VWAP Divergence % panes.
2. **Classic Plotly VWAP Divergence & VWAP Price Overlay:**
   - Upgraded native Plotly charts in [`app.py`](file:///c:/Users/user/Downloads/DATASCI2/persistence/dashboard/app.py) to a 4-pane co-plot layout.
   - Added rolling VWAP price curve directly onto the price chart, plus a dedicated VWAP Divergence subplot.
3. **Live Candle Countdown Timer & Ingestion Telemetry:**
   - Added a real-time countdown badge ("Next candle in Xs") showing exact seconds remaining in the active 30s candle period.
   - Added live Cassandra data age calculation, active ticker counter, and anomaly detection stats.
4. **Cassandra Connection Pooling Singleton:**
   - Refactored [`persistence_client.py`](file:///c:/Users/user/Downloads/DATASCI2/persistence/persistence_client.py) to use thread-safe client singletons (`_CLIENT_SINGLETONS`).
   - Completely resolved Cassandra driver socket exhaustion (`Too many open files`) during high-frequency auto-refreshes.
5. **Continuous Live Streaming Replay:**
   - Added `--live` and `--loop` flags to [`producer.py`](file:///c:/Users/user/Downloads/DATASCI2/data-ingestion/producer.py) to anchor tick timestamps to real-time wall clock during live demonstrations.

---

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
    ├── app.py                     # Streamlit application (dark terminal theme, Plotly & TradingView co-plots)
    ├── tradingview_component.py   # Synchronized 3-pane candlestick & indicator embedder
    ├── chart_server.py            # Local HTTP server for TradingView frontend
    └── lightweight-charts.js      # Bundled TradingView Lightweight Charts library
```

---

## Environment & Requirements

Tested across:
- **Cassandra:** 4.1.12 (Docker)
- **PySpark:** 4.2.0 for the refined detector (the sink is also regression-tested on 3.5.9)
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
    'class': 'NetworkTopologyStrategy',
    'datacenter1': 3
} AND durable_writes = true;

USE surveillance;

-- All processed market events
CREATE TABLE IF NOT EXISTS raw_events (
    ticker text,
    event_time timestamp,
    tick_id text,
    price double,
    volume double,
    side text,
    PRIMARY KEY ((ticker), event_time, tick_id)
) WITH CLUSTERING ORDER BY (event_time DESC, tick_id ASC);

-- Detected statistical anomalies
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
```

Both tables use `ticker` as the partition key and `(event_time, tick_id)` as clustering keys. The deterministic `tick_id` preserves distinct ticks in the same millisecond and makes Spark micro-batch retries idempotent. Queries remain partition-scoped and time ordered without `ALLOW FILTERING`.

The primary key changed from the original schema, and `schema.cql` only uses `CREATE ... IF NOT EXISTS`, so re-applying it keeps an older table as it is. `detect_anomalies.py --cassandra-sink` therefore checks the tables' columns before it starts and stops with a message naming what is missing. To recreate the keyspace (this deletes its data; Cassandra cannot alter primary-key columns in place):

```bash
python -m persistence.schema_tools --reset    # from the repo root; --check only checks
```

### 2. Start Cassandra (Docker stack)

Cassandra 4.1 is part of the Docker stack in `stream-processing/docker/`, next to Kafka and Spark:

```bash
cd stream-processing/docker
docker compose up -d --build
```

The stack runs a **3-node Cassandra cluster** (`cassandra1`, `cassandra2`, `cassandra3`, one datacenter). The nodes join one at a time, so the first start takes a few minutes. The keyspace keeps 3 replicas of every row, and every client (sink, dashboard, tests) reads and writes at `LOCAL_QUORUM` (2 of 3), so the pipeline keeps running with any one node down. The reasoning and the measured node-failure drill are in [`evaluation/README.md`](../evaluation/README.md).

The one-shot `cassandra-init` service applies `schema.cql` once all three nodes are healthy. From the host, `cassandra1` is on `localhost:9043`. Inside the stack the `pyspark` container uses all three nodes as contact points (`CASSANDRA_HOST=cassandra1,cassandra2,cassandra3`, `CASSANDRA_CONSISTENCY=LOCAL_QUORUM`). Each node keeps its data in its own volume (`cassandra1-data` ...). Check the ring and the tables with:

```bash
docker compose exec cassandra1 nodetool status      # three UN (Up/Normal) lines
docker compose exec cassandra1 cqlsh -e "DESCRIBE KEYSPACE surveillance"
```

`CASSANDRA_HOST` accepts a comma-separated list everywhere. On a single local Cassandra, apply the schema with `python -m persistence.schema_tools`, not `cqlsh -f`: it lowers the replication factor to the number of nodes (a quorum of 3 replicas can't be met by one node).

---

## Running the Components

### 1. Surveillance Dashboard (Streamlit Web UI)

In the Docker stack (port 8501 is published):
```bash
docker exec -w /workspace docker-pyspark-1 streamlit run persistence/dashboard/app.py --server.address 0.0.0.0
```

or natively from the project root: `streamlit run persistence/dashboard/app.py`. The web dashboard opens at `http://localhost:8501`.

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

### 3. Refined detector directly to Cassandra

From the project root, pass the explicit sink destination to Objective 2:

```powershell
python stream-processing/detect_anomalies.py --cassandra-sink
```

Every scored tick is stored in `raw_events`; rows with `is_anomaly = true` are also stored in `anomalies` with `risk_score`, `signals`, `incident_id`, and `tick_id`. The sink forces event-time conversion through UTC before handing timestamps to the Cassandra driver.

---

## Testing & Verification Suite

All tests run with pytest, in one command:

```bash
docker exec -w /workspace docker-pyspark-1 python3 -m pytest persistence/ -v
```

(or `pytest persistence/` from the repo root natively). Tests that touch Cassandra each run in a throwaway keyspace `surveillance_test_<id>` built from `schema.cql` and dropped afterwards, so they never write into `surveillance`. Without a reachable Cassandra they are skipped with a reason; inside the Docker stack `REQUIRE_CASSANDRA=1` makes that a failure instead. The streaming test is skipped on native Windows (Spark checkpoints need winutils there).

### Persistence Layer Tests

1. **Direct Python -> Cassandra** (`test_persistence.py`): an anomaly row round-trips with every field unchanged.

2. **Spark Batch DataFrame -> Cassandra** (`test_spark_sink.py`): both ticks land in `raw_events`, only the flagged one in `anomalies` with all its fields; writing the same batch twice (a retried micro-batch) leaves one row per tick.

3. **Spark Structured Streaming -> foreachBatch -> Cassandra** (`test_streaming_sink.py`): a fixed input of 9 ticks in 3 files runs as 3 micro-batches to completion (`availableNow`); Cassandra must then hold exactly those 9 ticks and the 3 flagged ones.

End to end (producer -> Kafka -> detector `--cassandra-sink` -> Cassandra, compared tick by tick with the offline detector) is `stream-processing/parity_check.py run --day 2026-09-16 --sink cassandra`; see `stream-processing/README.md`.

### Alerting & Dashboard Tests

4. **Alerting Pipeline Unit Tests** (`test_alerting_pipeline.py`). Validates:
   - `test_burst_debouncing_and_escalation`: Multi-tick burst aggregation and severity escalation.
   - `test_cooldown_enforcement`: Suppression of redundant alerts during active cooldown windows.
   - `test_mock_client_math_grounding`: Rolling statistics, Z-score, and VWAP divergence calculations.
   - `test_notifier_audit_logging`: Structured JSON record integrity and latency fields.
   - `test_severity_classification`: Threshold boundaries for CRITICAL, WARNING, and INFO tiers.

5. **Objective 2 Contract Bridge** (`test_objective2_contract.py`). Validates both the original eight-field stream and the refined risk/incident fields through Spark sink normalization.

---

## Key Architecture & Design Highlights

1. **Partition-Scoped Cassandra Queries:**
   Instead of cluster-wide table scans (`SELECT * FROM anomalies WHERE event_time >= ? ALLOW FILTERING`), queries are strictly partitioned by ticker (`WHERE ticker = ? AND event_time >= ? LIMIT N`). This ensures constant $O(1)$ partition retrieval time even as the database scales to millions of records.
2. **Burst Debouncing / Incident Aggregation:**
   High-frequency streaming can flag dozens of consecutive ticks during a single price shock. Refined rows are grouped by Objective 2's stable `incident_id`; legacy rows retain Kyle's configurable time-window fallback. Peak risk and fired signals are preserved while preventing alert fatigue.
3. **End-to-End Latency Tracking for Objective 4:**
   Every generated alert records `detection_latency_ms = (alert_created_at - anomaly_event_time)`, providing direct data points for median and p95 streaming latency evaluation.
4. **Resilient Dual-Mode Client:**
   `PersistenceClient` dynamically inspects Cassandra schema metadata to adapt query fields gracefully, while providing automatic in-process mock replay fallback for zero-dependency local runs.
5. **Cassandra Connection Pooling via Client Singletons:**
   Active Cassandra driver cluster sessions are managed in a thread-safe singleton cache (`_CLIENT_SINGLETONS`). This prevents driver connection leaks and socket/file-descriptor exhaustion (`Too many open files`) during high-frequency UI auto-refreshes.
6. **Synchronized Technical & Statistical Co-Plots:**
   Integrates hardware-accelerated TradingView Lightweight Charts alongside native Plotly subplots, providing synchronized Candlestick price action with rolling VWAP overlay, Volume histogram, rolling Z-score oscillator, and VWAP Divergence % with live candle countdown telemetry.
