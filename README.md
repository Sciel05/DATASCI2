# Real-Time Market Surveillance Pipeline

Statistical anomaly detection in high-frequency trading data, using
Apache Kafka, Apache Spark Structured Streaming, and Apache Cassandra.

Group project — Apache Kafka/Spark/Cassandra course.

**Group members:** Catacutan, Elijah · Villanueva, Hector Angelo ·
Pelayo, Agatha Fei · Narciso, Frank Exequiel · Gomugda, Kyle Joniel

## Objectives & status

| # | Objective | Owner(s) | Status |
|---|---|---|---|
| 1 | Event Ingestion & Partitioning | Pelayo, Agatha Fei | Done — see `data-ingestion/` |
| 2 | Real-Time Stream Processing & Detection | Narciso, Frank Exequiel | Refined detector implemented — see `stream-processing/` |
| 3 | Distributed Persistence & Alerting | Villanueva, Hector Angelo & Gomugda, Kyle Joniel | Synchronized dashboard & persistence verified — see `persistence/` |
| 4 | Pipeline Integration & Evaluation | Catacutan, Elijah | End-to-end evaluation on a 3-node Cassandra cluster done (completeness, accuracy, throughput, latency, node failure); recorded demo run pending — see `evaluation/` |

## Repo structure

```
market-surveillance-pipeline/
├── data-ingestion/       # Objective 1 — Kafka producer, dataset, partitioning
├── stream-processing/    # Objective 2 — Spark Structured Streaming, Z-score/VWAP detection
├── persistence/          # Objective 3 — Cassandra schema, sink, alerting daemon & dashboard
└── evaluation/           # Objective 4 — latency/throughput results, integration
```

- `data-ingestion/`: Working Kafka producer with synthetic & labeled market tick generation.
- `stream-processing/`: Spark Structured Streaming detection pipeline (Z-score, VWAP divergence).
- `persistence/`: Apache Cassandra schema + streaming sinks (`cassandra_sink.py`), plus real-time alerting engine (`alerting/`), background daemon (`run_alerting.py`), and Streamlit financial surveillance desk (`dashboard/app.py`).
- `evaluation/`: End-to-end evaluation (`run_evaluation.py`, `failover_drill.py`) and results: completeness, precision/recall, throughput, latency, node failure.

## Architecture

```
[yfinance real bars] -> [tick upsampling + anomaly injection]
        -> [Kafka: market-ticks topic, partitioned by ticker]
        -> [Spark Structured Streaming: per-ticker rolling Z-score / VWAP]
        -> [Cassandra, 3 nodes, RF=3, LOCAL_QUORUM: raw_events + anomalies tables]
        -> [Alerting Daemon + Streamlit Surveillance Dashboard]
```

Each stage's own README (inside its folder) has the actual setup and run
instructions. Start with `data-ingestion/README.md` or `persistence/README.md`.

## Getting started

1. Clone the repo.
2. Install dependencies for your module (e.g. `pip install -r persistence/requirements.txt`).
3. Run the Streamlit surveillance dashboard:
   ```bash
   streamlit run persistence/dashboard/app.py
   ```
4. Run the alerting daemon:
   ```bash
   python persistence/run_alerting.py
   ```

