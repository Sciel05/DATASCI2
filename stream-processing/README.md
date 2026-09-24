# Stream Processing (Objective 2)

**Status: implemented (`detect_anomalies.py`), unit-tested, and verified
end-to-end against a real Kafka broker + Spark. Thresholds calibrated
against the full labeled dataset (see Calibration below): precision tops
out ~4-5% and recall ~8-11% at any reasonable operating point across a
full 1.5-6.0 Z-threshold sweep — a real ceiling for this plain
Z-score/VWAP baseline on this data, not an undertuned threshold. Chosen
defaults (`z=3.5`, `vwap=0.01`) favor recall over the pure F1-optimal
point deliberately. See the Calibration section for v2 ideas (adaptive
window, joint Z-score/wash-trade signal, per-ticker thresholds) for
whoever picks this up in Objective 4.**

Owns: real-time detection logic in Apache Spark Structured Streaming.
Consumes from the `market-ticks` Kafka topic that `data-ingestion/`
produces. Its output feeds `persistence/` (Objective 3).

## What this needs to do

Per the proposal's Specific Objective 2 ("Real-Time Stream Processing &
Detection"):

- Read from Kafka (`market-ticks` topic — see `data-ingestion/README.md`
  for the payload schema: `ticker, event_time_ms, price, bid, ask,
  volume, side`, plus a `label` field that is **ground truth only** — do
  not read it as a detection input).
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
- Implement the wash-trading/spoofing heuristic separately (simpler
  windowed aggregation): high volume + near-zero net price movement in a
  short window. This is explicitly scoped as a simplified heuristic, not
  a validated detector — don't over-invest here.
- Emit `{ticker, timestamp, price, volume, zscore, vwap_divergence,
  is_anomaly, anomaly_type}` — this is what `persistence/` will sink to
  Cassandra.

## Evaluating against ground truth

`data-ingestion/aapl_msft_googl_tsla_nvda_ticks_labeled.csv` has a
`label` column (`normal` / `price_shock` / `wash_trade`) for exactly
this purpose — join your detector's output back against it (by
`ticker` + `event_time_ms`) to compute precision/recall for the
Objective 4 evaluation. Don't feed `label` into the Spark job itself.

### Calibration (baseline Z-score/VWAP detector)

The full labeled dataset was replayed offline through the real
`make_baseline_update_fn` (pandas-driven, same code the Spark job runs —
see `test_baseline.py` and `calibrate_thresholds.py`) to pick defaults,
then re-verified against an actual Kafka + Spark run (see Setup below).
`window_size=100, min_samples=30, clip_k=5.0` throughout.

**Full z_threshold sweep** (`vwap_threshold=0.01` fixed) — run via
`python calibrate_thresholds.py`, extended range added afterward per the
same method:

| z_threshold | flagged | flag rate | precision | recall | F1 |
|---|---|---|---|---|---|
| 1.5 | 56,145 | 39.89% | 0.55% | 52.84% | 0.0108 |
| 2.0 | 27,066 | 19.23% | 0.83% | 38.55% | 0.0162 |
| 2.5 | 10,668 | 7.58% | 1.48% | 27.19% | 0.0281 |
| 3.0 | 3,800 | 2.70% | 2.63% | 17.21% | 0.0457 |
| **3.5 (default)** | 1,564 | 1.11% | 4.16% | 11.19% | 0.0606 |
| 4.0 (F1-optimal) | 913 | 0.65% | 5.48% | 8.61% | **0.0669** |
| 4.5 | 668 | 0.47% | 4.79% | 5.51% | 0.0512 |
| 5.0 | 567 | 0.40% | 3.70% | 3.61% | 0.0366 |
| 5.5 | 529 | 0.38% | 2.84% | 2.58% | 0.0270 |
| 6.0 | 512 | 0.36% | 2.34% | 2.07% | 0.0220 |

F1 rises monotonically through 3.5, peaks at **z=4.0**, then falls —
a real interior peak, not a sweep-boundary artifact.

**vwap_threshold sweep** (`z_threshold=3.5` fixed, the stage before the
extended z-sweep found the 4.0 peak):

| vwap_threshold | flagged | flag rate | precision | recall | F1 |
|---|---|---|---|---|---|
| 0.005 | 2,245 | 1.60% | 2.98% | 11.53% | 0.0474 |
| 0.01 (**default**) | 1,564 | 1.11% | 4.16% | 11.19% | 0.0606 |
| 0.015 | 1,457 | 1.04% | 4.39% | 11.02% | 0.0628 |

vwap_threshold matters far less than z_threshold in this range — F1
moves by ~0.015 across the swept values vs. ~0.06 across the z-sweep.

**Chosen defaults: `z_threshold=3.5`, `vwap_threshold=0.01`** — deliberately
*not* the pure F1-optimal z=4.0. At 3.5 vs. 4.0: recall is meaningfully
higher (11.2% vs. 8.6%) for a real but smaller precision cost (4.2% vs.
5.5%). For a surveillance system, a missed price shock (false negative) is
worse than an extra alert an analyst has to dismiss (false positive), so
the recall-favoring point near the F1 peak was chosen over the raw
optimum.

**The honest ceiling**: across the entire 1.5-6.0 z_threshold range,
precision never exceeds ~5.5% and recall never exceeds ~53% (and the two
trade off directly — the high-recall end has near-zero precision). At
every reasonable operating point (F1 > 0.04), precision tops out around
4-5% and recall around 8-11%. This is not an undertuned threshold — it's
the real detection ceiling of a plain per-ticker rolling Z-score/VWAP
baseline against this injected `price_shock` style. Three things worth
knowing if you tune further anyway:

- **Z-score is the informative signal; VWAP divergence mostly isn't**, at
  least for this injected `price_shock` style. VWAP-divergence-only
  detection gets ~0% recall. It's kept as a secondary OR condition (per
  the proposal's spec) rather than dropped, but don't expect it to carry
  much weight — real per-tick VWAP divergence for *normal* ticks is
  already ~0.4% at the 99th percentile, so anything below ~1% just adds
  noise.
- **A shock tick must stay in the rolling window, not be excluded from
  it.** An earlier version excluded flagged ticks from the baseline
  window (reasoning: don't let a shock's own price pollute the baseline
  it gets judged against). On real data that caused an unbounded feedback
  loop instead: excluding a tick shrinks the window's variance estimate,
  which makes the next tick more likely to be excluded too, compounding
  until ~89% of all ticks were flagged. `test_baseline.py::test_shock_contamination_is_bounded_not_runaway`
  is a regression test for this. The current behavior includes every
  tick, but with **bounded influence**: a tick's deviation from the
  current mean is clipped to at most `--clip-k` (default 5) standard
  deviations before being folded into the running mean/variance, so one
  large shock can't stretch the window's tolerance wide open for the
  following `window_size` ticks the way folding in its raw price would.
  VWAP's accumulators are intentionally untouched (still use the raw
  price) — this only bounds the mean/variance path.
- **Bounded influence alone barely moves precision/recall at these
  settings** — measured on both the full dataset and the 20k-tick slice
  used for the Docker smoke test, recall moved by ~0.2–1.2pp and
  precision was flat within noise (unbounded: 2.72%/17.04% full,
  2.96%/18.60% slice → bounded: 2.63%/17.21% full, 2.92%/19.77% slice).
  Makes sense: `price_shock` injections are isolated single-tick spikes,
  and at `window_size=100` one tick's contribution is already diluted
  1/100 whether raw or clipped. The earlier exclusion bug (89% flag rate)
  was the real problem; clipping is a smaller, complementary safety
  margin against one shock temporarily widening tolerance, not a lever
  for precision/recall by itself.

**Where a v2 could look, for whoever picks this up in Objective 4** — not
implemented here, just pointers: a wider or adaptive baseline window
(volatility regimes shift intraday; one fixed `window_size` can't track
both a calm open and a volatile close well), combining the Z-score and
wash-trade signals jointly instead of as fully separate detectors (a
shock riding on unusual volume is more informative than either signal
alone), or a per-ticker learned threshold instead of one global cutoff
(NVDA's baseline volatility isn't AAPL's). Raising the threshold further
within this same design won't move the ceiling — the sweep above already
covers that.

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
thresholds, wash-trade volume/stability/window settings).

### Unit tests (no Spark/Kafka needed)

```bash
pip install pytest
python -m pytest test_baseline.py -v
```

`test_baseline.py` drives `make_baseline_update_fn` directly with a fake
`GroupState`, bypassing Spark's streaming runtime entirely (pandas only)
— covers warm-up gating, no-leakage scoring, window eviction, state
round-tripping across micro-batches, and the runaway-contamination and
bounded-influence regressions above. Fast (~2s), good for iterating on
the detection logic itself.

### Threshold calibration

```bash
python calibrate_thresholds.py
```

Sweeps the Z-score and VWAP-divergence thresholds (values hardcoded in
the script, not CLI flags) against the full labeled dataset offline —
same `make_baseline_update_fn` the Spark job runs — and reports
precision/recall/F1 per combination. This produced the Calibration
numbers above exactly; doesn't change any defaults in
`detect_anomalies.py`, it only reports.
