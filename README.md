# Real-Time Market Surveillance Pipeline

Statistical anomaly detection in high-frequency trading data, using
Apache Kafka, Apache Spark Structured Streaming, and Apache Cassandra.

Group project — Apache Kafka/Spark/Cassandra course.

**Group members:** Catacutan, Elijah · Villanueva, Hector Angelo ·
Pelayo, Agatha Fei · Narciso, Frank Exequiel · Gomugda, Kyle Joniel

## Objectives & status

| # | Objective | Owner(s) | Status |
|---|---|---|---|
| 1 | Event Ingestion & Partitioning | | Done — see `data-ingestion/` |
| 2 | Real-Time Stream Processing & Detection | | ⬜ Not started |
| 3 | Distributed Persistence & Alerting | | ⬜ Not started |
| 4 | Pipeline Integration & Evaluation | | ⬜ Not started |

## Repo structure

```
market-surveillance-pipeline/
├── data-ingestion/       # Objective 1 — Kafka producer, dataset, partitioning
├── stream-processing/    # Objective 2 — Spark Structured Streaming, Z-score/VWAP detection
├── persistence/          # Objective 3 — Cassandra schema, sink, dashboard
└── evaluation/           # Objective 4 — latency/throughput results, integration
```

(Only `data-ingestion/` has working code so far — `stream-processing/`,
`persistence/`, and `evaluation/` each have a README describing what
that objective needs to build; add code there as you pick it up.)

## Architecture

```
[yfinance real bars] -> [tick upsampling + anomaly injection]
        -> [Kafka: market-ticks topic, partitioned by ticker]
        -> [Spark Structured Streaming: per-ticker rolling Z-score / VWAP]
        -> [Cassandra: raw_events + anomalies tables]
        -> [Dashboard: polls anomalies table]
```

Each stage's own README (inside its folder) has the actual setup and run
instructions. Start with `data-ingestion/README.md`.

## Getting started

1. Clone the repo.
2. Pick your objective's folder, read its README.
3. `data-ingestion/` already produces a live `market-ticks` Kafka topic —
   point your Spark job at it once Objective 1 is running locally.
