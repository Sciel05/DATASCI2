# Stream Processing (Objective 2)

**Status: implemented (`detect_anomalies.py`), unit-tested, calibrated,
and evaluated out-of-sample (see Evaluation below). Held-out test F1 is
0.30 across both detectors at 0.84% anomaly prevalence (24% precision,
40% recall), and the tuning generalizes across time and tickers. Two
honest caveats: on the original single-tick price shocks the rolling
Z-score is barely better than a fixed return threshold, and both
detectors largely miss harder variants (multi-tick ramps, split wash
trades) — see Known limitations.**

Owns: real-time detection logic in Apache Spark Structured Streaming.
Consumes from the `market-ticks` Kafka topic that `data-ingestion/`
produces. Its output feeds `persistence/` (Objective 3).

## What this needs to do

Per the proposal's Specific Objective 2 ("Real-Time Stream Processing &
Detection"):

- Read from Kafka (`market-ticks` topic — see `data-ingestion/README.md`
  for the payload schema: `ticker, event_time_ms, price, bid, ask,
  volume`, plus a `label` field that is **ground truth only** — do not
  read it as a detection input. There is no `side` field in the actual
  data, despite earlier docs).
- Maintain a per-ticker rolling statistical baseline (trailing mean/std
  of price) — use `flatMapGroupsWithState` keyed by `ticker` for a true
  trailing baseline, not just fixed time windows, per the proposal's
  emphasis on evaluating each event against "the asset's own recent
  behavior."
- Compute, per event:
  - **Z-score**: `(price - rolling_mean) / rolling_stddev`
  - **VWAP divergence**: `abs(price - vwap) / vwap`
- Flag anomalies where these exceed a threshold (start around Z > 3,
  tune against the labeled dataset in `data-ingestion/`).
- Implement the wash-trading/spoofing heuristic separately: high volume +
  near-zero net price movement in a short window. This is explicitly
  scoped as a simplified heuristic, not a validated detector — don't
  over-invest here.
- Emit `{ticker, timestamp, price, volume, zscore, vwap_divergence,
  is_anomaly, anomaly_type}` — this is what `persistence/` will sink to
  Cassandra.

## How it works

Both detectors run inside one per-ticker stateful function
(`make_baseline_update_fn`, via `applyInPandasWithState` — PySpark's
equivalent of `flatMapGroupsWithState`), so each keeps its trailing
history across micro-batches instead of restarting at every batch
boundary.

- **price_shock**: rolling Z-score over the last `--baseline-window`
  ticks (default 20, scored once `--min-samples` 10 are available), OR'd
  with VWAP divergence > `--vwap-threshold` (1%). Flags `|z| > 5.0`. A
  tick is scored against the window *before* it, and its contribution to
  the running mean/variance is clipped to `--clip-k` (5) standard
  deviations (see Calibration notes).
- **wash_trade** (adapted from the `Frank-stream-processing` branch):
  flags a tick whose volume exceeds `--wash-volume-ratio` (30%) of the
  trailing `--wash-lookback-seconds` (120s) of volume, while that
  trailing window's price range stays under `--wash-price-range` (0.1%),
  given at least `--wash-min-prior` (4) prior ticks. `price_shock` takes
  precedence if both fire on one tick.

The short window is the main lesson taken from Frank's branch: his
2-minute time window (~5 ticks on this data) beat the original 100-tick
window, and a 15-25 tick window does as well while keeping the window
size predictable in quiet and bursty periods.

## Evaluating against ground truth

`data-ingestion/aapl_msft_googl_tsla_nvda_ticks_labeled.csv` has a
`label` column (`normal` / `price_shock` / `wash_trade`) for exactly
this purpose — join the detector's output back against it (by `ticker` +
`event_time_ms`). Don't feed `label` into the Spark job itself.

Both scripts below replay the dataset offline through the real
`make_baseline_update_fn` (pandas, no Spark/Kafka) — the same code the
Spark job runs.

**Dataset**: 140,751 ticks, 5 tickers, 5 trading days (Sep 15-21, 2026).
Prevalence is **0.84%** (`price_shock` 0.41%, `wash_trade` 0.42%), so
random flagging scores ~0.8% precision — keep that in mind reading any
number below.

### Two ways of scoring

The injector (`data-ingestion/inject_anomalies.py`) labels only the
**first tick** of each event, though a `price_shock` moves prices for 5
ticks and a `wash_trade` burst spans 4-9 ticks.

- **Tick-level** (`calibrate_thresholds.py`, and sections 1-3 of
  `evaluate_generalization.py`): a flag is a true positive only on the
  labeled first tick. Flags on an event's later ticks count as false
  positives, so this **understates precision**.
- **Event-level** (section 4 of `evaluate_generalization.py`): an event
  is detected if the matching detector flags *any* tick inside its span.
  Precision counts flags inside that type's spans as correct, and ignores
  flags inside another type's span (neither right nor wrong). Spans of
  the original events are approximated (5 ticks for `price_shock`; 9, the
  injector's maximum, for `wash_trade`); spans of the harder injected
  variants are exact.

### Calibration (in-sample)

`python calibrate_thresholds.py` — z sweep at `window_size=20,
min_samples=10, clip_k=5.0, vwap_threshold=0.01`, price-shock detector
only, tick-level, **tuned and scored on the full dataset**:

| z_threshold | flagged | precision | recall | F1 |
|---|---|---|---|---|
| 3.0 | 7,024 | 5.30% | 64.03% | 0.098 |
| 3.5 | 3,479 | 9.63% | 57.66% | 0.165 |
| 4.0 | 2,115 | 13.81% | 50.26% | 0.217 |
| 4.5 | 1,543 | 16.40% | 43.55% | 0.238 |
| **5.0 (default)** | 1,227 | 18.66% | 39.41% | 0.253 |
| 5.5 (F1 peak) | 1,044 | 19.73% | 35.46% | **0.254** |
| 6.0 | 891 | 19.64% | 30.12% | 0.238 |
| 7.0 | 714 | 19.47% | 23.92% | 0.215 |

At the defaults: `price_shock` 18.7% / 39.4% (F1 0.253), `wash_trade`
23.5% / 30.8% (F1 0.267), any anomaly 20.9% / 35.5% (F1 0.263).
`z=5.0` rather than the 5.5 peak because F1 is tied within 0.001 and
recall is 4 points higher — a missed manipulation costs more than an
extra alert an analyst dismisses. `vwap_threshold` barely matters
(0.005-0.015 moves F1 by < 0.01).

For comparison, the pre-merge detector (100-tick window, 30-second
tumbling-window wash rule) scored price_shock F1 0.061 and wash_trade F1
0.010 (it flagged 45% of all windows); Frank's branch, 0.080 and 0.254.

Calibration notes that still hold from earlier iterations:

- **A shock tick must stay in the rolling window, not be excluded from
  it.** Excluding flagged ticks caused a runaway feedback loop on real
  data (the window's variance shrinks, so the next tick is more likely
  to be excluded, compounding to ~89% of ticks flagged).
  `test_baseline.py::test_shock_contamination_is_bounded_not_runaway`
  covers it. Instead every tick is included with **bounded influence**:
  its deviation is clipped to `--clip-k` standard deviations before it
  enters the running mean/variance. VWAP's accumulators use the raw
  price.
- **Z-score is the informative signal; VWAP divergence mostly isn't**
  for this injected shock style. It's kept as a secondary OR condition
  per the proposal's spec.

### Out-of-sample evaluation

`python evaluate_generalization.py` (~90s). It tunes on one part of the
data and scores on another. The detector is causal (each tick is scored
only against earlier ticks), so it runs once over the full timeline and
the split is applied when scoring.

**1. Time split.** Tuned on the first 70% of the timeline (to Sep 18
16:58 UTC, 98,526 ticks); tested on the remaining 42,225. A random split
would leak the future into the past. The tuning picked a 15-tick window,
z=7.0, and a 0.2% wash price range. Tick-level, any anomaly:

| | precision | recall | F1 |
|---|---|---|---|
| Tuned detector, train (in-sample) | 23.4% | 36.7% | 0.286 |
| **Tuned detector, test** | **24.0%** | **39.7%** | **0.299** |
| Current defaults, test (chosen on all data, so leaky) | 22.5% | 40.5% | 0.289 |
| Naive fixed threshold, test | 14.1% | 47.6% | 0.217 |
| Random at the detector's flag rate, test | 0.8% | 1.4% | 0.010 |

Test scores are no worse than train scores, so the tuning is not
overfit to the training period.

**2. Baselines, per detector (test split).** The naive baseline flags
`|tick-to-tick return|` above a threshold for price shocks and raw volume
above a threshold for wash trades, both tuned on the training split.

| | tuned detector F1 | naive F1 | random F1 | precision lift over random |
|---|---|---|---|---|
| price_shock | 0.281 | **0.269** | 0.005 | 52x |
| wash_trade | 0.314 | 0.186 | 0.005 | 62x |

The lift over random is large but trivial at 0.4% prevalence. **The
honest comparison is the naive threshold: on the original injected
shocks, the rolling Z-score is barely better than flagging big
tick-to-tick returns.** Those shocks are single-tick jumps of 4-8 local
sigma, which a return threshold catches just as well. The wash-trade
rule clearly beats a plain volume threshold.

**Leave-one-ticker-out** (tune on four tickers, all days; score the
fifth). "Own-best" is the best F1 that ticker could reach if tuned on
itself:

| held out | tuned window / z | shock F1 | own-best shock F1 | wash F1 | own-best wash F1 |
|---|---|---|---|---|---|
| AAPL | 15 / 6.5 | 0.284 | 0.284 | 0.399 | 0.399 |
| GOOGL | 15 / 6.5 | 0.200 | 0.205 | 0.326 | 0.373 |
| MSFT | 15 / 6.5 | 0.366 | 0.378 | 0.242 | 0.319 |
| NVDA | 15 / 6.5 | 0.255 | 0.255 | 0.119 | 0.252 |
| TSLA | 15 / 6.5 | 0.298 | 0.308 | 0.330 | 0.365 |

Every fold picks the same window and z, and price-shock F1 is within
0.013 of each ticker's own best, so the threshold carries over across
stocks. The wash rule carries over less well — NVDA trades about twice
as often as the others, so a fixed 2-minute lookback means something
different for it.

**3. Precision-recall over z** (15-tick window, price_shock,
tick-level):

| z | train P | train R | train F1 | test P | test R | test F1 |
|---|---|---|---|---|---|---|
| 3.0 | 4.9% | 67.4% | 0.092 | 5.8% | 77.7% | 0.108 |
| 4.0 | 11.6% | 55.2% | 0.192 | 14.6% | 64.8% | 0.238 |
| 5.0 | 16.2% | 46.5% | 0.240 | 19.6% | 55.3% | 0.289 |
| 6.0 | 19.1% | 38.3% | 0.255 | 22.5% | 46.4% | 0.303 |
| 7.0 | 21.5% | 34.1% | 0.263 | 22.1% | 38.6% | 0.281 |
| 8.0 | 22.1% | 27.6% | 0.245 | 21.8% | 31.8% | 0.259 |

Test F1 stays within 0.28-0.30 across z = 5.0-7.0, so the default z=5 sits
on a broad plateau rather than a lucky point.

**4. Harder anomaly variants, per event** (current defaults, all days).
The detector and the original injector were written together, so
scoring only the original styles is circular.
`evaluate_generalization.py` injects two harder variants into a copy of
the data, one event per original event of the same family, at least 40
ticks clear of any labeled event, never across a session gap:

| type | events | detected | precision | recall | F1 |
|---|---|---|---|---|---|
| price_shock (original: 1-tick jump, decays over 5) | 581 | 229 | 29.8% | 39.4% | 0.339 |
| **shock_ramp** (same 4-8 sigma, built over 8-20 ticks) | 581 | 42 | 20.4% | **7.2%** | **0.107** |
| wash_trade (original: 6-15x volume, 4-9 ticks) | 595 | 306 | 79.4% | 51.4% | 0.624 |
| **wash_split** (1.5-3x volume, 15-30 ticks) | 595 | 80 | 40.7% | **13.4%** | **0.202** |

Event-level wash-trade precision (79%) is far above tick-level (23%) —
most tick-level "false positives" are the later ticks of real bursts.

### Known limitations

- **Slow or split manipulation goes largely undetected** (table 4).
  A ramp moves slowly enough that the 20-tick baseline follows it up, so
  it never stands out. Split prints never exceed 30% of the trailing
  volume. Directions to try:
  - Compare cumulative multi-tick price moves against the baseline, to
    catch ramps.
  - Sum volume over the trailing window instead of testing single
    ticks, to catch split wash trades.
- **Price-shock Z-score ≈ naive return threshold** on the original
  injected shocks (0.281 vs 0.269 test F1).
- **The wash-trade lookback is fixed in time** and doesn't carry over
  to higher-frequency tickers (NVDA held-out F1 0.119 vs 0.252
  own-best). Scaling it to each ticker's tick rate is the obvious fix.
- **The wash rule is really a volume-spike-in-a-flat-market detector.**
  Real wash trading means the same beneficial owner on both sides, which
  the data can't show. It will flag legitimate block trades in a quiet
  market.
- **Synthetic labels.** All numbers are against injected anomalies on
  real-anchored synthetic ticks. Real market data would need re-checking.
- **One global threshold** for all tickers. Per-ticker thresholds are a
  possible v2.

## Setup

```bash
pip install -r requirements.txt
```

Tested against PySpark 4.2.0 / Java 17.

### Recommended: the Docker stack (known-working Kafka + Spark)

Starting a stateful streaming query (this job uses
`applyInPandasWithState` for the baseline detector) needs a checkpoint
directory, which on native Windows needs `winutils.exe` matching your
**exact** bundled Hadoop version (check with `spark-submit --version` and
`ls $(python -c "import pyspark,os;print(os.path.dirname(pyspark.__file__))")/jars
| grep hadoop` — Spark 4.2.0 bundles Hadoop 3.5.0, new enough that a
prebuilt `winutils.exe` may not exist yet). Rather than fight that,
`docker/` gives you a real Kafka broker + a Spark-ready container in one
command — this is exactly how the calibration numbers above were verified
end-to-end (20k real labeled ticks through a real Kafka broker + Spark,
not just the offline pandas replay):

```bash
cd stream-processing/docker
docker compose up -d --build

# create the topic once
docker exec docker-compose-kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --create --topic market-ticks --partitions 8 --replication-factor 1 \
    --bootstrap-server localhost:9092

# the pyspark container has Java 17, PySpark 4.2.0, pyarrow, pandas,
# confluent-kafka -- the whole repo is mounted at /workspace
docker exec -it docker-compose-pyspark-1 bash

# inside the container: feed it real labeled ticks
cd data-ingestion && python3 producer.py --topic market-ticks \
    --bootstrap-servers kafka:9092 --max-rate

# then run the detector, pointed at the broker by its in-network name
cd ../stream-processing && \
  /usr/local/lib/python3.10/dist-packages/pyspark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
    detect_anomalies.py --bootstrap-servers kafka:9092 \
    --starting-offsets earliest --trigger-once
```

`kafka:9092` is the broker's address *inside* the Docker network (the
service name from `docker/docker-compose.yml`), not `localhost:9092` --
containers reach each other by service name, not by the host's loopback
address. WSL was tried first and rejected for this — its virtual network
couldn't reach the internet at all on this machine (host/sandbox
restriction), so `apt`/pip installs failed; Docker Desktop's network
worked fine.

### Pointing at a different broker

`--bootstrap-servers` defaults to `localhost:9092` and works as-is against
a real (non-Docker) broker reachable at that address -- e.g. one started
per `data-ingestion/README.md`'s own instructions, running natively
alongside this job on Linux/macOS (no `winutils.exe` issue there) or in
WSL with working networking. Point it anywhere else with
`--bootstrap-servers <host>:<port>`; `--input-topic` similarly defaults to
`market-ticks` but can be overridden.

### Native spark-submit (Linux/macOS, or Windows with a working `winutils.exe`)

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
    detect_anomalies.py --starting-offsets earliest
```

`--starting-offsets earliest` replays the full labeled dataset from the
beginning of the topic (needed for the Objective 4 precision/recall
evaluation); the default `latest` only picks up new ticks, for a live
demo alongside `data-ingestion/producer.py`.

Pass `--output-topic market-anomalies` to mirror the anomaly stream to
Kafka for `persistence/` to consume instead of printing to the console.
See `python detect_anomalies.py --help` for all tunable
thresholds/windows (baseline window size, Z-score/VWAP-divergence
thresholds, wash-trade lookback/volume-ratio/price-range settings).

### Unit tests (no Spark/Kafka needed)

```bash
pip install pytest
python -m pytest test_baseline.py -v
```

`test_baseline.py` drives `make_baseline_update_fn` directly with a fake
`GroupState`, bypassing Spark's streaming runtime entirely (pandas only)
— covers warm-up gating, no-leakage scoring, window eviction, state
round-tripping across micro-batches, the runaway-contamination and
bounded-influence regressions above, the wash-trade rule (flat vs moving
market, lookback expiry, precedence, history across batches), and
loading state checkpointed before the wash-trade detector moved in. Fast (~2s), good for iterating on
the detection logic itself.

### Threshold calibration

```bash
python calibrate_thresholds.py
```

Sweeps the Z-score and VWAP-divergence thresholds (values hardcoded in
the script, not CLI flags) against the full labeled dataset offline —
same `make_baseline_update_fn` the Spark job runs — and reports
precision/recall/F1 per combination, plus every detector at the
defaults. This produced the Calibration numbers above exactly; doesn't
change any defaults in `detect_anomalies.py`, it only reports.

### Out-of-sample evaluation

```bash
python evaluate_generalization.py
```

Time split, leave-one-ticker-out, baselines, precision-recall over z,
and the harder injected variants — produces the Out-of-sample
evaluation tables above (~90s, pandas only).
