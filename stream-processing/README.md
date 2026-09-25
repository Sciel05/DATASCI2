# Stream Processing (Objective 2)

**Status: implemented (`detect_anomalies.py`), unit-tested, calibrated,
evaluated out-of-sample, and verified tick-for-tick against the real
Kafka + Spark pipeline (see Parity check). Held-out test F1 is 0.30
across both detectors at 0.84% anomaly prevalence (24% precision, 39%
recall), and the tuning generalizes across time and tickers. Two
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
  read it as a detection input. The CSV has no `side` column;
  `producer.py` synthesizes one from price direction, and the detector
  doesn't use it).
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

Stream handling, per ticker:

- **Keying**: `producer.py` keys every message by ticker, so each
  ticker's ticks land on one partition in order. Verified on the real
  `market-ticks` topic (all 140,751 messages consumed with keys printed):
  every key equals its payload's ticker, each ticker maps to exactly one
  partition, and no ticker's ticks are out of time order.
- **Overnight gap reset**: if a tick arrives more than
  `--gap-reset-minutes` (30) after the ticker's previous one, both
  detectors start cold, so the morning open re-warms instead of being
  scored against yesterday's close. The labeled data has exactly 20 such
  gaps (4 per ticker, all 17.5h+ overnight/weekend); there are no
  intraday gaps over 5 minutes. This removed ~225 in-sample false
  price_shock alarms at the opens.
- **Late ticks**: within a micro-batch, ticks are sorted by event time
  before scoring, so reordering inside a batch is harmless. A tick older
  than one already processed for its ticker (it arrived in a *later*
  batch) is dropped and counted rather than scored against its own
  future: the running count lives in state, and each batch with drops
  logs `[detect_anomalies] <TICKER>: dropped N late tick(s) this batch,
  M total` to the executor log. Ticks later than `--late-watermark`
  (1 minute) are dropped by Spark itself before the state function;
  `--trigger-once` runs print that total (`numRowsDroppedByWatermark`).
  Both were 0 in the parity runs, as expected from in-order keyed
  partitions.

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
| 3.0 | 6,785 | 5.45% | 63.68% | 0.101 |
| 3.5 | 3,243 | 10.27% | 57.31% | 0.174 |
| 4.0 | 1,879 | 15.43% | 49.91% | 0.236 |
| 4.5 | 1,311 | 19.15% | 43.20% | 0.265 |
| **5.0 (default)** | 1,000 | 22.70% | 39.07% | 0.287 |
| 5.5 (F1 peak) | 820 | 24.88% | 35.11% | **0.291** |
| 6.0 | 674 | 25.67% | 29.78% | 0.276 |
| 7.0 | 503 | 27.44% | 23.75% | 0.255 |

At the defaults: `price_shock` 22.7% / 39.1% (F1 0.287), `wash_trade`
23.3% / 30.8% (F1 0.265), any anomaly 23.3% / 35.4% (F1 0.281).
`z=5.0` rather than the 5.5 peak because F1 is within 0.004 and recall
is 4 points higher — a missed manipulation costs more than an extra
alert an analyst dismisses. The out-of-sample PR table below shows z=5
sits on the plateau either way. `vwap_threshold` barely matters
(0.005-0.015 moves F1 by < 0.01).

For comparison, the pre-merge detector (100-tick window, 30-second
tumbling-window wash rule) scored price_shock F1 0.061 and wash_trade F1
0.010 (it flagged 45% of all windows); Frank's branch, 0.080 and 0.254.
The overnight gap reset lifted price_shock precision from 18.7% to 22.7%
at the same recall.

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
| Tuned detector, train (in-sample) | 25.6% | 36.7% | 0.302 |
| **Tuned detector, test** | **24.0%** | **39.1%** | **0.297** |
| Current defaults, test (chosen on all data, so leaky) | 23.7% | 39.9% | 0.297 |
| Naive fixed threshold, test | 14.1% | 47.6% | 0.217 |
| Random at the detector's flag rate, test | 0.8% | 1.4% | 0.010 |

Test F1 is within 0.005 of train F1, so the tuning is not overfit to the
training period.

**2. Baselines, per detector (test split).** The naive baseline flags
`|tick-to-tick return|` above a threshold for price shocks and raw volume
above a threshold for wash trades, both tuned on the training split.

| | tuned detector F1 | naive F1 | random F1 | precision lift over random |
|---|---|---|---|---|
| price_shock | 0.279 | **0.269** | 0.005 | 52x |
| wash_trade | 0.313 | 0.186 | 0.005 | 62x |

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
| AAPL | 15 / 6.5 | 0.302 | 0.302 | 0.397 | 0.397 |
| GOOGL | 15 / 6.5 | 0.238 | 0.244 | 0.321 | 0.373 |
| MSFT | 15 / 6.5 | 0.413 | 0.419 | 0.241 | 0.319 |
| NVDA | 15 / 6.5 | 0.266 | 0.268 | 0.118 | 0.250 |
| TSLA | 15 / 6.5 | 0.317 | 0.326 | 0.330 | 0.365 |

Every fold picks the same window and z, and price-shock F1 is within
0.009 of each ticker's own best, so the threshold carries over across
stocks. The wash rule carries over less well — NVDA trades about twice
as often as the others, so a fixed 2-minute lookback means something
different for it.

**3. Precision-recall over z** (15-tick window, price_shock,
tick-level):

| z | train P | train R | train F1 | test P | test R | test F1 |
|---|---|---|---|---|---|---|
| 3.0 | 5.0% | 67.4% | 0.094 | 5.7% | 76.5% | 0.107 |
| 4.0 | 12.5% | 55.2% | 0.204 | 14.6% | 63.7% | 0.237 |
| 5.0 | 18.1% | 46.5% | 0.261 | 19.6% | 54.2% | 0.288 |
| 6.0 | 22.4% | 38.3% | 0.283 | 22.5% | 45.3% | 0.301 |
| 7.0 | 26.3% | 34.1% | 0.297 | 22.2% | 37.4% | 0.279 |
| 8.0 | 28.2% | 27.6% | 0.279 | 21.9% | 30.7% | 0.256 |

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
| price_shock (original: 1-tick jump, decays over 5) | 581 | 227 | 36.7% | 39.1% | 0.378 |
| **shock_ramp** (same 4-8 sigma, built over 8-20 ticks) | 581 | 42 | 25.6% | **7.2%** | **0.113** |
| wash_trade (original: 6-15x volume, 4-9 ticks) | 595 | 306 | 78.8% | 51.4% | 0.622 |
| **wash_split** (1.5-3x volume, 15-30 ticks) | 595 | 81 | 40.6% | **13.6%** | **0.204** |

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
  injected shocks (0.279 vs 0.269 test F1).
- **The wash-trade lookback is fixed in time** and doesn't carry over
  to higher-frequency tickers (NVDA held-out F1 0.118 vs 0.250
  own-best). Scaling it to each ticker's tick rate is the obvious fix.
- **The wash rule is really a volume-spike-in-a-flat-market detector.**
  Real wash trading means the same beneficial owner on both sides, which
  the data can't show. It will flag legitimate block trades in a quiet
  market.
- **Synthetic labels.** All numbers are against injected anomalies on
  real-anchored synthetic ticks. Real market data would need re-checking.
- **One global threshold** for all tickers. Per-ticker thresholds are a
  possible v2.
- **Running-sum variance is numerically fragile.** The rolling window's
  variance is `sum_sq/n - mean^2` from running sums, which cancels
  badly at price levels of 100-500 with tick-level spreads: a one-ulp
  change in an input price moves later z-scores by ~1e-6. No flag has
  changed because of it, but recomputing mean/variance directly from the
  (at most 20-tick) window would remove it.

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
command — this is where the Parity check below runs:

```bash
cd stream-processing/docker
docker compose up -d --build

# create the topic once
docker exec docker-kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --create --topic market-ticks --partitions 8 --replication-factor 1 \
    --bootstrap-server localhost:9092

# the pyspark container has Java 17, PySpark 4.2.0, pyarrow, pandas,
# confluent-kafka -- the whole repo is mounted at /workspace
docker exec -it docker-pyspark-1 bash

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

Container names come from the compose project (the `docker/`
directory name): `docker-kafka-1`, `docker-pyspark-1`.

### Parity check (streaming vs offline)

The tables above come from the offline replay. `parity_check.py` proves
the streaming path computes the same thing: it replays a slice through
the real producer → Kafka → Spark job, then scores the same slice with
the offline function, and compares every tick. `--max-offsets-per-trigger`
forces many small micro-batches, so per-ticker state has to carry across
batch boundaries.

```bash
docker exec -w /workspace/stream-processing docker-pyspark-1 \
    python3 parity_check.py run --day 2026-09-16
docker exec -w /workspace/stream-processing docker-pyspark-1 \
    python3 parity_check.py run --day 2026-09-16 2026-09-17   # crosses an overnight gap
```

`run` creates two uniquely named topics (`parity-ticks-<id>`,
`parity-anomalies-<id>`, so `market-ticks` is untouched and no run
reuses another's records), produces the slice with
`data-ingestion/producer.py`, runs `detect_anomalies.py` with
`--max-offsets-per-trigger 2000` and a temporary checkpoint directory,
reads the output topic back, compares, and **deletes both topics and the
checkpoint when it finishes, even on failure**. It exits non-zero on any
mismatch. Detector flags after `--` go to both the Spark job and the
offline run. `slice` and `compare` do the first and last step alone, for
a pipeline run made by hand.

The offline side reads the same slice file the producer reads. That
matters: pandas' default CSV float parser isn't correctly rounded, so
writing a slice and reading it back shifts some long decimals by one ulp
(61 of 27,923 prices on Sep 16, all NVDA). Scored from the original
parse instead, 5,498 z-scores differed by ~1e-6 while every flag still
matched — see Known limitations on running-sum variance.

**Results** (current defaults, 2000 records per micro-batch):

| slice | ticks | micro-batches | rows out (Spark / offline) | zscore / vwap / flag / type mismatches | flags (shock / wash) |
|---|---|---|---|---|---|
| Sep 16 | 27,923 | 14 | 27,923 / 27,923 | 0 / 0 / 0 / 0 | 166 / 181 both |
| Sep 16-17 (crosses an overnight gap) | 55,171 | 28 | 55,171 / 55,171 | 0 / 0 / 0 / 0 | 372 / 317 both |

z-score and VWAP divergence match to 1e-9 relative. Late drops and
watermark drops were 0. In the two-day run, Spark's own output shows
each ticker's first 10 ticks on Sep 17 unscored (baseline re-warming
after the gap), then scored from the 11th. Negative control: comparing
against `--baseline-window 21` fails with 27,773 z-score and 25 flag
mismatches, so the check does catch a one-tick difference in
configuration.

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
python -m pytest test_baseline.py test_parity_check.py -v
```

`test_baseline.py` drives `make_baseline_update_fn` directly with a fake
`GroupState`, bypassing Spark's streaming runtime entirely (pandas only)
— covers warm-up gating, no-leakage scoring, window eviction, state
round-tripping across micro-batches, the runaway-contamination and
bounded-influence regressions above, the wash-trade rule (flat vs moving
market, lookback expiry, precedence, history across batches), the
overnight gap reset (a day's close then the next morning's open), late
ticks (reordered within a batch; dropped and counted across batches,
including a replay with stragglers held back a batch), and loading both
earlier checkpoint state layouts. Fast (~2s), good for iterating on
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
