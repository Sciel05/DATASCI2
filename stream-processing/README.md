# Stream Processing (Objective 2)

**Status: in progress — Kafka read, schema parsing, output wiring, and testing done by NOTHIN; core flatMapGroupsWithState detection logic pending from Agatha.**

Owns: real-time detection logic in Apache Spark Structured Streaming.
Consumes from the `market-ticks` Kafka topic that `data-ingestion/`
produces. Its output feeds `persistence/` (Objective 3).

## What this needs to do

Per the proposal's Specific Objective 2 ("Real-Time Stream Processing &
Detection"):

- Read from Kafka (`market-ticks` topic — see `data-ingestion/README.md`
  for the payload schema: `ticker, event_time_ms, price, bid, ask,
  volume, label` — note: no `side` field exists in the actual data,
  despite earlier docs suggesting otherwise. `label` is **ground truth
  only** — do not read it as a detection input).
- Maintain a per-ticker rolling statistical baseline (trailing mean/std
  of price) — use `flatMapGroupsWithState` keyed by `ticker` for a true
  trailing baseline, not just fixed time windows, per the proposal's
  emphasis on evaluating each event against "the asset's own recent
  behavior." **Current implementation uses a `rangeBetween` time window
  (2 minutes) as an interim placeholder — this is NOT the final
  approach and should be replaced with `flatMapGroupsWithState`.**
- Compute, per event:
  - **Z-score**: `(price - rolling_mean) / rolling_stddev`
  - **VWAP divergence**: `abs(price - vwap) / vwap`
- Flag anomalies where these exceed a threshold (start around Z > 3,
  tune against the labeled dataset in `data-ingestion/`).
- Implement the wash-trading/spoofing heuristic separately (simpler
  windowed aggregation): high volume + near-zero net price movement in a
  short window. This is explicitly scoped as a simplified heuristic, not
  a validated detector — don't over-invest here. **A working first-pass
  version exists in `detection.py`.**
- Emit `{ticker, timestamp, price, volume, zscore, vwap_divergence,
  is_anomaly, anomaly_type}` — this is what `persistence/` will sink to
  Cassandra.

## Evaluating against ground truth

`data-ingestion/aapl_msft_googl_tsla_nvda_ticks_labeled.csv` has a
`label` column (`normal` / `price_shock` / `wash_trade`) for exactly
this purpose — join your detector's output back against it (by
`ticker` + `event_time_ms`) to compute precision/recall for the
Objective 4 evaluation. Don't feed `label` into the Spark job itself.

**Window-size tuning results so far** (using the interim `rangeBetween`
approach, tested via `evaluate.py`):

| Window | Recall | Precision | F1 |
|---|---|---|---|
| 50-row (original) | ~9.2% | ~5.7% | ~7.0% |
| 5-minute | ~9.2% | ~5.7% | ~7.0% |
| 2-minute | ~27.9% | ~8.9% | ~13.5% (best) |
| 1-minute | ~47.4% | ~6.9% | ~12.0% |

2-minute gives the best precision/recall balance of the options tested.
This is all against the interim window-based approach — numbers will
change once `flatMapGroupsWithState` replaces it.

## Setup

- Spark version: **4.2.0** (see `requirements.txt`: `pyspark==4.2.0`)
- Kafka connector: `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0`,
  loaded via `spark.jars.packages` config in `pipeline.py` (no need to
  pass `--packages` manually on the command line)
- Kafka topic: `market-ticks`, created with 8 partitions:
