# Pipeline Integration & Evaluation (Objective 4)

**Status: first full evaluation done (2026-10-04)** on the Docker stack
with the 3-node Cassandra cluster, and again with 3 Kafka brokers. Still to do: the 10–15 min recorded demo
run with dashboard screenshots (see [Remaining work](#remaining-work)).

Per the proposal's Specific Objective 4, this measures the integrated
pipeline end to end: `data-ingestion` → Kafka → `stream-processing` (Spark)
→ `persistence` (Cassandra) → dashboard and alerting. Every number below is
read back **from Cassandra**, i.e. what the pipeline actually stored, at
`LOCAL_QUORUM`.

## How to reproduce

With the stack up (`cd stream-processing/docker && docker compose up -d`),
from the repository root:

```bash
# full labeled dataset, original timestamps: completeness, accuracy, throughput
docker compose -f stream-processing/docker/docker-compose.yml exec pyspark \
    python3 evaluation/run_evaluation.py batch

# live run (producer --live) with cassandra3 stopped for 2 minutes in the middle:
# latency, completeness under a node failure, RF=1 vs RF=3 probe (run on the host)
python evaluation/failover_drill.py

# on the 3-broker stack (docker-compose.kafka3.yml), stopping a Kafka broker
python evaluation/failover_drill.py --kafka3 --node kafka3
```

Each run uses its own Kafka topic and Cassandra keyspace (built from
`persistence/schema.cql`, so 3 replicas) and drops them afterwards. Results
land in `evaluation/results/` (`batch_<id>.json`, `failover_<id>.json`,
`live_<id>.json`; the per-tick latency CSV of a live run is not committed).

| Script | Does |
|---|---|
| `run_evaluation.py batch` | Producer `--max-rate` → Kafka (8 partitions) → detector `--trigger-once --cassandra-sink` (10,000 records per micro-batch) → reads `raw_events` / `anomalies` back and scores them against the CSV labels |
| `run_evaluation.py live` | Detector running continuously; producer `--live --loop --speed 100` (~110 ticks/s) for N minutes; latency per tick = Cassandra `WRITETIME` − the tick's event time |
| `run_evaluation.py probe` | Writes/reads 200 keys at `LOCAL_QUORUM` in an RF=1 and an RF=3 keyspace |
| `failover_drill.py` | Runs `live` for 6 min, stops `cassandra3` at minute 2, starts it at minute 4, probes in every phase, splits latency by phase |

**Test setup:** Windows 11 laptop, Docker Desktop (WSL2 VM: 16 vCPUs,
6.7 GB RAM) running Kafka (1 broker; 3 in section 6), 3 × Cassandra 4.1 (384 MB heap each)
and Spark 4.2 in `local[4]` mode (`stream-processing/docker/spark-conf/`).
Dataset: `aapl_msft_googl_tsla_nvda_ticks_labeled.csv`, 140,751 ticks over
5 tickers and 5 trading days, with 581 injected `price_shock` and 595
`wash_trade` events.

## Results

Runs: batch `35eae9d1`, failover drill `368cb371`
(`evaluation/results/`).

### 1. Completeness — no tick lost

| Run | Produced to Kafka | Stored in `raw_events` | Missing | Anomaly rows |
|---|---|---|---|---|
| Batch (whole dataset) | 140,751 | 140,751 | **0** | 4,157 |
| Live, with a node down for 2 min | 40,271 | 40,271 | **0** | 1,153 |

Every stored tick also matches a dataset tick (no duplicates or strays): the
sink's writes are idempotent (`tick_id` is part of the primary key), so a
replayed micro-batch overwrites instead of duplicating.

### 2. Detection accuracy (batch run)

`inject_anomalies.py` labels only the **first** tick of an injected event,
but a price shock decays over the next 5 ticks and a wash-trade burst lasts
4–9 ticks, and the detector often fires a tick or two into the event. So
accuracy is reported at three levels:

- **Tick** (strict): a flag counts only on the labeled tick itself.
- **Event**: an event is found if a flag of the right type lands in its
  window (5 ticks for a shock, 9 for a wash trade); a flag is correct if it
  lands in a window of its type.
- **Incident**: one alert = one `incident_id` (what the alerting engine and
  dashboard show); it is correct if any of its flags is.

| | Event precision | Event recall | Event F1 | Incident precision | Tick precision / recall |
|---|---|---|---|---|---|
| **Any anomaly** | 0.78 | 0.72 (844 / 1,176) | 0.75 | 0.72 (795 / 1,103) | 0.16 / 0.58 |
| `wash_trade` | **0.94** | **0.99** (590 / 595) | **0.96** | 0.81 (567 / 698) | 0.15 / 0.73 |
| `price_shock` | 0.34 | 0.43 (249 / 581) | 0.38 | 0.48 (247 / 514) | 0.20 / 0.42 |

- **Wash trades are detected almost perfectly**: 590 of 595 events, and 94%
  of wash-trade flags are inside a real wash-trade burst.
- **Price shocks are the weak spot**: under half the injected shocks are
  typed as `price_shock`, and two thirds of `price_shock` flags fall outside
  a shock window. The injected shocks are 4–8 local sigma and decay within
  5 ticks, so on volatile tickers they sit close to normal noise; the
  EWMA/CUSUM ramp signals also fire on ordinary drifts. Tuning
  `--zscore-threshold` / `--cusum-h` against these numbers is the obvious
  next step for Objective 2.
- The low tick-level precision (0.16) is mostly an artefact of first-tick
  labelling: one event typically produces several flags on consecutive
  ticks (4,157 flags for 1,103 incidents), and only one of them is the
  labeled tick. The incident grouping is what keeps the alert count close
  to the event count.

### 3. Throughput (batch run)

| Stage | Rate |
|---|---|
| Producer → Kafka (`--max-rate`, lz4, keyed by ticker) | **10,498 msgs/s** (140,751 in 13.4 s) |
| Spark detector + Cassandra sink, sustained (first to last write) | **1,264 ticks/s** (140,751 in 111 s) |
| Whole Spark job incl. JVM start and Kafka package resolution | 211 s |

The detector is the bottleneck by an order of magnitude: its per-ticker
state function (`applyInPandasWithState`) runs in 4 Python workers, and the
sink writes every tick to 3 replicas. 1,264 ticks/s is ~1,000× the
dataset's real average rate (~1.2 ticks/s) and ~11× the live demo rate.
Measured by the Cassandra write timestamps rather than Kafka consumer lag:
Structured Streaming tracks its Kafka offsets in its checkpoint, not in a
consumer group, so `kafka-consumer-groups.sh` has nothing to show.

### 4. Latency (live run, producer `--live --speed 100`, ~112 ticks/s)

Producer event time (the tick's scheduled send time) → Cassandra write time
(`WRITETIME`, set by the Spark sink), per tick:

| Phase | Ticks | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| Cold start (first 60 s) | 9,060 | 12.2 s | 28.7 s | 30.3 s | 31.1 s |
| **Steady state** (after the first 60 s, whole run) | 31,211 | **1.83 s** | **2.97 s** | 3.49 s | 4.66 s |
| …while `cassandra3` was down (119 s) | 11,119 | 1.70 s | 2.65 s | 3.01 s | 3.48 s |
| …after it was restarted | 14,669 | 1.90 s | 3.13 s | 3.68 s | 4.66 s |
| Anomalies table, steady state | 930 | 2.59 s | 4.05 s | 4.62 s | 5.05 s |

- **Steady-state end-to-end latency is ~1.8 s median, under 3 s at p95.**
  Almost all of it is the Spark micro-batch (each batch waits for the
  previous one to finish); Kafka and Cassandra add milliseconds.
- **The cold start** is the first micro-batches: Spark plans the stateful
  query and starts its Python workers while ticks queue in Kafka. It is
  reported separately because it is a one-off per detector start, not a
  property of the running pipeline.
- Anomaly rows land ~0.7 s after the tick rows because the sink writes
  `raw_events` first, then `anomalies`, in the same micro-batch.

### 5. Node failure: why 3 nodes (failover drill)

`cassandra3` was stopped for **119 s** in the middle of the live run, then
started again; it was back to Up/Normal **34 s** after starting.

| | Before | `cassandra3` down | After restart |
|---|---|---|---|
| Pipeline writes (RF=3, `LOCAL_QUORUM`) | ok | **ok: 0 errors, 0 lost ticks, p50 1.70 s** | ok |
| Probe: 200 writes, RF=3 keyspace, `LOCAL_QUORUM` | 0 failed | **0 failed** | 0 failed |
| Probe: 200 writes, RF=1 keyspace (the old layout) | 0 failed | **82 failed (41%)** | 0 failed |

With one replica per row, every row whose token range the stopped node owns
is unwritable until it comes back — here 82 of 200 keys. With the old
single-node stack, the same failure would have stopped the whole pipeline:
the sink raises on the first failed write and the streaming query dies. With
3 replicas and quorum writes, the outage was invisible to the pipeline and
the dashboard. Writes the stopped node missed are replayed to it by the
other two (hinted handoff) when it rejoins; reads stay correct meanwhile
because a quorum always includes an up-to-date replica.

### 6. Three Kafka brokers (optional stack)

Same tests on the 3-broker stack (`docker-compose.kafka3.yml`: 3 KRaft
brokers, topics with 3 replicas and `min.insync.replicas=2`, producer
`acks=all` + idempotence). Runs: batch `6b3260e8`, drill `a227e825`
(broker `kafka3` stopped).

| | 1 broker | 3 brokers |
|---|---|---|
| Completeness, whole dataset | 140,751 / 140,751 | 140,751 / 140,751 |
| Accuracy | batch `35eae9d1` | identical to it (same input, same detector settings: both runs predate the price-shock tuning) |
| Producer → Kafka | 10,498 msgs/s | **2,210 msgs/s** |
| Detector + sink, sustained | 1,264 ticks/s | 1,104 ticks/s |
| Live latency, steady state p50 / p95 | 1.83 s / 2.97 s | 2.08 s / 3.40 s |
| Kafka memory (all brokers) | 0.82 GB (1 GB default heap) | 0.86 GB (3 × 256 MB heap) |

**Broker `kafka3` stopped for 118 s mid-run** (back 35 s after starting):

| | Before | `kafka3` down | After restart |
|---|---|---|---|
| Ticks stored | ok | **0 lost** (40,311 sent = 40,311 stored, the exact first 40,311 dataset ticks per ticker) | ok |
| Latency p50 / p95 | — | 1.98 s / 3.20 s | 2.12 s / 3.54 s |

The partitions `kafka3` led moved to the other two brokers; the producer
(retrying idempotently) and the Spark consumer followed the new leaders
without errors or a latency spike. With a single broker the same stop halts
ingestion outright: the producer can't deliver and the detector has nothing
to read until it returns.

The cost is producer throughput (about 5× lower): with `acks=all` each send
waits for 2 of 3 brokers. 2,210 msgs/s is still ~20× the live demo rate,
and the detector (1,100 ticks/s) remains the bottleneck either way.

## The 3-node design and why

Configured in `stream-processing/docker/docker-compose.yml` and
`persistence/schema.cql`; clients connect through `persistence/schema_tools.py`.

| Choice | Setting | Why |
|---|---|---|
| **3 nodes** | `cassandra1..3`, one datacenter, seed `cassandra1` | The smallest cluster where a quorum (2) survives losing a node. 2 nodes with RF=2 have a quorum of 2, so losing either node stops quorum writes. More than 3 doesn't fit next to Kafka and Spark in the 6.7 GB Docker VM. |
| **RF = 3** | `NetworkTopologyStrategy`, `'datacenter1': 3` | Every node holds a full copy, so any one node can be lost without losing data or availability. `NetworkTopologyStrategy` rather than `SimpleStrategy` is the production default and lets a second datacenter be added without changing the keyspace's strategy. |
| **`LOCAL_QUORUM`** reads and writes | `CASSANDRA_CONSISTENCY`, default in every client | Write quorum (2) + read quorum (2) > RF (3), so a read always overlaps the latest acknowledged write: the dashboard never shows a stale anomaly, and the pipeline still tolerates one node down. `ONE` would also survive the failure but allows stale reads; `ALL` would fail as soon as one node is down. |
| **All nodes as contact points** | `CASSANDRA_HOST=cassandra1,cassandra2,cassandra3` | A client (re)connecting while one node is down still finds the cluster. |
| **Nodes join one at a time** | `depends_on: service_healthy` chain | Cassandra doesn't support several nodes bootstrapping at once. |
| **384 MB heap per node** | `MAX_HEAP_SIZE` | Three nodes plus Kafka and Spark fit in the Docker VM; with `local[*]` Spark and larger heaps the VM ran out of memory and restarted the nodes. |

On a single local Cassandra (no Docker), `python -m persistence.schema_tools`
creates the keyspace with as many replicas as there are nodes, since a
quorum of 3 can't be met by one node.

**Honest limits of this setup:** all three nodes share one machine, one disk
and one Docker VM, so this demonstrates the replication and consistency
behaviour, not hardware fault tolerance; in production each node would be a
separate host (ideally separate racks or zones). Kafka runs a single
broker by default (replication factor 1, the least memory); the optional
3-broker stack (section 6) removes that single point of failure. Spark
runs in local mode, so the detector itself is one process: its checkpoint
lets it resume without loss after a crash, but not without a pause.

## Remaining work

- **Recorded demo run**: 10–15 min of producer `--live` → detector →
  Cassandra → dashboard, with screenshots of the dashboard (KPI tiles,
  alerts, live chart) for the report.
- **Price-shock tuning** (with Objective 2): the event-level numbers above
  are the baseline to beat.

## Out of scope (per the proposal)

Not built, by design:

- **Multi-asset correlated ML models**: the detector scores each ticker on
  its own statistics; cross-asset models need far more labeled data than
  the injected set provides.
- **NLP sentiment analysis**: needs a news/social feed aligned with the
  ticks, outside the market-data pipeline's scope.
- **Automated closed-loop order cancellation**: acting on flags without a
  human in the loop is a compliance decision, not a detection one; the
  pipeline stops at alerting.
