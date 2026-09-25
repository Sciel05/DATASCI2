# Stream Processing (Objective 2)

**Status: implemented (`detect_anomalies.py`), unit-tested, evaluated
out-of-sample and on a held-out set of harder anomalies never used for
tuning, and verified tick-for-tick against the real Kafka + Spark
pipeline (see Parity check). On the time-split test period it beats the
naive threshold baseline on every anomaly type (per-event F1: price
shocks 0.404 vs 0.286, wash trades 0.966 vs 0.871, held-out slow ramps
0.334 vs 0.057, held-out fine splits 0.412 vs 0.087), with ~72% of
incidents overlapping a real anomaly at ~7 alerts per ticker-hour.
Remaining regressions and limits are listed under "Final results" and
"Known limitations" — slow price ramps in particular stay hard.**

Owns: real-time detection logic in Apache Spark Structured Streaming.
Consumes from the `market-ticks` Kafka topic that `data-ingestion/`
produces. Its output feeds `persistence/` (Objective 3); the field list
for the Cassandra table is in `docs/cassandra-schema-notes.md`.

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
  Cassandra. These eight fields are unchanged; the fields added since are
  appended after them (see Output).

## How it works

Every signal runs inside one per-ticker stateful function
(`make_baseline_update_fn`, via `applyInPandasWithState` — PySpark's
equivalent of `flatMapGroupsWithState`), so each keeps its trailing
history across micro-batches instead of restarting at every batch
boundary.

Price signals (flag `anomaly_type = "price_shock"`):

- **Z-score / VWAP**: rolling Z-score over the last `--baseline-window`
  ticks (default 20, scored once `--min-samples` 10 are available), and
  VWAP divergence. A tick is scored against the window *before* it, and
  its contribution to the window is clipped to `--clip-k` (5) standard
  deviations (see Calibration notes). Mean and variance are computed
  directly from the (at most 20) clipped prices in the window.
- **EWMA ramp signal**: a fast price EWMA (span 3) against a slow one
  (span 20), divided by the RMS of their recent divergence. A price
  walked up or down over many ticks pulls the two apart before any single
  tick looks extreme. Scored after 50 ticks.
- **Price CUSUM**: two-sided CUSUM of tick-to-tick price changes,
  standardized by a slow RMS of (clipped) changes; allowance k=2, alarm
  h=6. Scored after 50 ticks. The side that fires restarts at zero.

Volume signals (flag `anomaly_type = "wash_trade"`):

- **Wash rule** (adapted from the `Frank-stream-processing` branch): a
  tick whose volume exceeds `--wash-volume-ratio` (70%) of the trailing
  lookback's volume while that lookback's price range stays under
  `--wash-price-range` (0.15%), given at least `--wash-min-prior` (4)
  prior ticks. At these re-tuned defaults it rarely fires on its own —
  the volume spike and volume CUSUM now cover what it caught (see Final
  results) — but the price range also gates the volume CUSUM. The lookback is `--wash-lookback-ticks` (16) ticks' worth
  of the ticker's typical time between ticks (a slow average of
  inter-tick gaps, each capped at 60s), so busy and quiet tickers look
  back over a comparable number of trades; the fixed
  `--wash-lookback-seconds` (120) applies until that average has 20 ticks.
- **Volume CUSUM**: one-sided CUSUM of standardized log-volume above its
  slow average, accumulated only while the lookback's price range is
  flat and reset as soon as price moves; k=0.75, h=3; restarts at zero
  after it fires. Catches many modestly-large prints into a flat market
  (split wash trades).
- **Volume spike**: the naive volume-threshold rule, scaled per ticker —
  a tick whose volume exceeds `--volume-spike-multiple` (6) times the
  ticker's typical (slow-average) volume, with no flat-market condition.
  Catches the obvious large prints.

Each CUSUM step is clipped to `--clip-k`, and the slow statistics update
after scoring, so one shock can't carry a CUSUM alone. A CUSUM that
fires restarts from zero on the next tick, so a burst raises one alarm
per accumulation rather than one per tick.

**Risk score and incidents.** Every signal is expressed as a multiple of
its own alarm level (|z|/5, vwap_div/1%, |ewma_div|/2.75, CUSUM/h, tick
volume / the wash rule's volume limit, tick volume / 6x typical volume);
`risk_score` is the largest. A
tick is flagged when `risk_score > --risk-threshold` (1.0, i.e. any
signal past its own alarm level); `signals` lists which. `anomaly_type`
is `price_shock` if any price signal fired, else `wash_trade`.
Consecutive flags on a ticker at most `--incident-gap-ticks` (10)
unflagged ticks apart share an `incident_id`.

Stream handling, per ticker:

- **Keying**: `producer.py` keys every message by ticker, so each
  ticker's ticks land on one partition in order. Verified on the real
  `market-ticks` topic (all 140,751 messages consumed with keys printed):
  every key equals its payload's ticker, each ticker maps to exactly one
  partition, and no ticker's ticks are out of time order.
- **Overnight gap reset**: if a tick arrives more than
  `--gap-reset-minutes` (30) after the ticker's previous one, every
  signal starts cold — rolling window, EWMAs, CUSUMs, trading rate, open
  incident — so the morning open re-warms instead of being scored
  against yesterday's close. The labeled data has exactly 20 such gaps
  (4 per ticker, all 17.5h+ overnight/weekend); there are no intraday
  gaps over 5 minutes. This removed ~225 in-sample false price_shock
  alarms at the opens.
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
- **tick_id**: `<ticker>-<event_time_ms>-<seq>`, `seq` counting earlier
  ticks of the ticker in the same millisecond (normally 0). A retried
  micro-batch restarts from the committed state and yields the same ids,
  so a sink keyed on it overwrites instead of duplicating.

### Output

The eight original fields, unchanged, then the appended ones (full types
in `docs/cassandra-schema-notes.md`): `ewma_divergence`, `cusum_price`,
`cusum_volume`, `risk_score`, `signals`, `incident_id`, `tick_id`.
`anomaly_type` still takes only `price_shock` / `wash_trade` / null —
which signal fired is in `signals` (`zscore`, `vwap`, `ewma`,
`cusum_price`, `wash_rule`, `cusum_volume`, `volume_spike`).

## Evaluating against ground truth

`data-ingestion/aapl_msft_googl_tsla_nvda_ticks_labeled.csv` has a
`label` column (`normal` / `price_shock` / `wash_trade`) for exactly
this purpose — join the detector's output back against it (by `ticker` +
`event_time_ms`). Don't feed `label` into the Spark job itself.

Both scripts below replay the dataset offline through the real
`make_baseline_update_fn` (pandas, no Spark/Kafka) with the Spark job's
own defaults (`detector_kwargs(parse_args([]))`) — the same code and
settings the Spark job runs.

**Dataset**: 140,751 ticks, 5 tickers, 5 trading days (Sep 15-21, 2026).
Prevalence is **0.84%** (`price_shock` 0.41%, `wash_trade` 0.42%), so
random flagging scores ~0.8% precision — keep that in mind reading any
number below.

### Three ways of scoring

The injector (`data-ingestion/inject_anomalies.py`) labels only the
**first tick** of each event, though a `price_shock` moves prices for 5
ticks and a `wash_trade` burst spans 4-9 ticks.

- **Tick-level** (`calibrate_thresholds.py`, and sections 1-3 of
  `evaluate_generalization.py`): a flag is a true positive only on the
  labeled first tick. Flags on an event's later ticks count as false
  positives, so this **understates precision** — especially for signals
  that fire more than once per event.
- **Event-level** (section 4 of `evaluate_generalization.py`): an event
  is detected if the matching detector (by `anomaly_type`) flags *any*
  tick inside its span. Precision counts flags inside that type's spans
  as correct, and ignores flags inside another type's span (neither right
  nor wrong). Spans of the original events are approximated (5 ticks for
  `price_shock`; 9, the injector's maximum, for `wash_trade`); spans of
  the injected variants are exact.
- **Incident-level** (section 4): an incident (flags sharing an
  `incident_id`) is correct if any of its ticks falls in any anomaly
  span. Also reported: share of events caught, and alerts (incidents)
  per trading hour per ticker.

### Harder variants: existing (for tuning) and held-out (for final scoring only)

The detector and the original injector were written together, so
scoring only the original styles is circular. `evaluate_generalization.py`
injects harder variants into in-memory copies of the data
(`data-ingestion/` is untouched), one event per original event of the
same family per ticker, at least 40 ticks clear of any labeled event,
never across a session gap, scaled to each session's tick sigma:

| set | variant | parameters | seed | used for |
|---|---|---|---|---|
| existing | `shock_ramp` | 4-8 sigma move built over 8-20 ticks, 5-tick decay | 7 | tuning 3a-3d |
| existing | `wash_split` | flat price, 1.5-3x volume per tick over 15-30 ticks | 7 | tuning 3a-3d |
| **held-out** | `shock_ramp_long` | same move built over **25-40** ticks | **20260925** | final scoring only |
| **held-out** | `wash_split_fine` | flat price, **1.2-2x** volume over 15-30 ticks | **20260925** | final scoring only |

The held-out set is generated only with `--heldout`; no threshold was
chosen while looking at it.

### Final results (per event)

`python evaluate_generalization.py --heldout --naive [--period test]`.

**How the settings were chosen.** The latest round (CUSUM restart,
volume spike, wash-rule ratio/range) was tuned on the **training split
only** — the first 70% of the timeline, on the original anomalies plus
the existing variants. Earlier defaults (z, window, EWMA, CUSUM k/h,
lookback, risk threshold) were chosen on all five days of the original
data and existing variants. So the **test period** below is clean with
respect to the latest round but not fully clean overall, and the
**held-out variants** were never used for any choice.

**Test period** (Sep 18 16:58 UTC onward, 30% of the timeline):

| type | final P / R / F1 | naive P / R / F1 |
|---|---|---|
| price_shock (original) | 35.2% / 47.5% / **0.404** | 21.3% / 43.6% / 0.286 |
| wash_trade (original) | 93.5% / 100% / **0.966** | 78.9% / 97.1% / 0.871 |
| shock_ramp (existing) | 36.5% / 20.2% / **0.260** | 10.1% / 3.0% / 0.046 |
| wash_split (existing) | 89.4% / 77.5% / **0.830** | 40.4% / 30.2% / 0.346 |
| **shock_ramp_long (held-out)** | 38.6% / 29.5% / **0.334** | 16.4% / 3.5% / 0.057 |
| **wash_split_fine (held-out)** | 67.5% / 29.7% / **0.412** | 25.4% / 5.2% / 0.087 |

**All days**, against the detector before this work (commit `cd3536c`:
Z-score + VWAP + fixed-lookback wash rule) and the previous round
(`d1f3b02`):

| type | pre-change F1 | previous round F1 | **final P / R / F1** | naive F1 |
|---|---|---|---|---|
| price_shock (original) | 0.378 | 0.374 | 36.1% / 42.9% / **0.392** | 0.320 |
| wash_trade (original) | 0.622 | 0.698 | 93.7% / 99.2% / **0.963** | 0.872 |
| shock_ramp (existing) | 0.113 | 0.225 | 32.2% / 16.5% / **0.218** | 0.069 |
| wash_split (existing) | 0.204 | 0.591 | 89.3% / 73.3% / **0.805** | 0.341 |
| **shock_ramp_long (held-out)** | 0.153 | 0.300 | 36.3% / 24.1% / **0.290** | 0.084 |
| **wash_split_fine (held-out)** | 0.089 | 0.241 | 66.6% / 29.9% / **0.413** | 0.102 |

The naive baseline flags |tick return| and raw volume above thresholds
tuned tick-level on the training split.

**Latest round, step by step** (training split only; flagged ticks,
incidents, alerts per ticker-hour and incident precision on the original
data; per-event F1):

| step | flagged ticks | incidents | alerts/h | incident precision | price_shock | wash_trade | shock_ramp | wash_split |
|---|---|---|---|---|---|---|---|---|
| before | 5.94% | 1,003 | 8.7 | 51.0% | 0.367 | 0.694 | 0.205 | 0.587 |
| 1. CUSUMs restart after firing | 2.62% | 1,035 | 9.0 | 50.9% | 0.380 | 0.786 | 0.201 | 0.615 |
| 2. volume spike signal (6x typical) | 3.42% | 1,084 | 9.4 | 51.7% | 0.380 | 0.871 | 0.201 | 0.690 |
| 3. scalar state in typed fields | 3.42% | 1,084 | 9.4 | 51.7% | 0.380 | 0.871 | 0.201 | 0.690 |
| 4. variance straight from the window | 3.40% | 1,085 | 9.4 | 51.6% | 0.386 | 0.871 | 0.200 | 0.690 |
| 5. wash rule re-tuned (0.7 / 0.15%) | 2.92% | 765 | 6.7 | 71.9% | 0.386 | 0.962 | 0.200 | 0.794 |

Findings from the round:

- **The CUSUM restart halved the flag volume** (5.94% -> 2.62% of
  ticks) — the CUSUMs had kept firing through every burst — and raised
  wash_trade F1 0.694 -> 0.786.
- **The volume spike had to be relative to each ticker.** A fixed share
  count (the naive baseline's own form; 30k-70k shares) barely moved
  wash_trade F1 (0.777-0.790) because it over-flags busy tickers; 6x the
  ticker's typical volume gave 0.871.
- **The running-sum variance error was deciding some flags**, not just
  moving z-scores by ~1e-6: computed directly, the Z-score flags ~5%
  fewer ticks at the same recall (in-sample, z=5: 1,000 -> 951).
- **The wash rule is now nearly redundant.** At ratio >= ~0.7 results
  equal switching it off; lower ratios mostly add false alarms (0.2:
  wash_trade F1 0.58-0.66). The time split in section 1 independently
  picks the same 0.7 / 0.15%.

**Regressions.** No original anomaly type regresses against the
previous round or the pre-change detector (price_shock 0.374 -> 0.392,
wash_trade 0.698 -> 0.963, all days). Two harder price variants dip
slightly against the previous round: **shock_ramp 0.225 -> 0.218** and
**held-out shock_ramp_long 0.300 -> 0.290** (all days), mostly from the
CUSUM restart (step 1: shock_ramp 0.205 -> 0.201 on the training split).
Tick-level, the full detector now scores price_shock 0.275, wash_trade
0.244, any 0.256 in-sample (previous round 0.256 / 0.101 / 0.135), on
2.95% of ticks flagged (was 6.07%).

**Incidents** (all days, default risk threshold 1.0):

| dataset | flagged ticks | incidents | incidents overlapping a real anomaly | events caught | alerts / ticker / hour | AAPL | GOOGL | MSFT | NVDA | TSLA |
|---|---|---|---|---|---|---|---|---|---|---|
| original | 2.95% | 1,103 | 72.0% | 71.8% | 6.8 | 5.6 | 4.3 | 4.7 | 12.2 | 7.1 |
| + shock_ramp | 3.06% | 1,149 | 78.9% | 54.2% | 7.1 | 5.8 | 4.5 | 4.8 | 13.1 | 7.3 |
| + wash_split | 5.70% | 1,823 | 70.1% | 72.3% | 11.2 | 9.9 | 7.0 | 7.2 | 20.3 | 11.7 |
| + shock_ramp_long (held-out) | 3.05% | 1,189 | 81.2% | 56.9% | 7.3 | 5.8 | 4.6 | 5.0 | 13.4 | 7.8 |
| + wash_split_fine (held-out) | 5.26% | 1,685 | 58.3% | 58.0% | 10.4 | 9.1 | 6.5 | 6.6 | 18.8 | 10.8 |

On the test period alone (original data): 3.03% of ticks flagged, 338
incidents, 72.2% overlapping a real anomaly, 73.7% of events caught, 7.1
alerts per ticker-hour. Each dataset above is the original data plus that
variant, so every row also contains the original anomalies. Alert rates
scale with each ticker's trading activity (NVDA trades ~2-3x as often as
the others).

**Risk threshold** (training split; per-event F1 / incident precision
and alerts per ticker-hour on the original data):

| risk threshold | price_shock | wash_trade | shock_ramp | wash_split | incident precision | alerts/h |
|---|---|---|---|---|---|---|
| 0.9 | 0.339 | 0.941 | 0.267 | 0.842 | 58% | 8.5 |
| **1.0 (default)** | 0.386 | 0.962 | 0.200 | 0.794 | 72% | 6.7 |
| 1.1 | 0.381 | 0.973 | 0.165 | 0.732 | 79% | 5.9 |
| 1.5 | 0.273 | 0.983 | 0.064 | 0.489 | 90% | 4.5 |
| 2.0 | 0.172 | 0.969 | 0.022 | 0.244 | 94% | 3.9 |

0.9 has the best mean F1 of the four rows but drops original
price_shock F1 to 0.339; 1.0 is the best setting that doesn't regress an
original row. Raising the threshold trades ramp and split recall for
fewer, more precise alerts, so `risk_score` is best used to rank them.

### Calibration (in-sample, Z-score component)

`python calibrate_thresholds.py` — z sweep at `window_size=20,
min_samples=10, clip_k=5.0, vwap_threshold=0.01` with the EWMA, CUSUM and
volume-spike signals switched off, price-shock detector only, tick-level,
**tuned and scored on the full dataset**:

| z_threshold | flagged | precision | recall | F1 |
|---|---|---|---|---|
| 3.0 | 6,739 | 5.49% | 63.68% | 0.101 |
| 3.5 | 3,196 | 10.42% | 57.31% | 0.176 |
| 4.0 | 1,832 | 15.83% | 49.91% | 0.240 |
| 4.5 | 1,264 | 19.86% | 43.20% | 0.272 |
| **5.0 (default)** | 951 | 23.87% | 39.07% | 0.296 |
| 5.5 (F1 peak) | 771 | 26.46% | 35.11% | **0.302** |
| 6.0 | 625 | 27.68% | 29.78% | 0.287 |
| 7.0 | 455 | 30.33% | 23.75% | 0.266 |

`z=5.0` rather than the 5.5 peak because F1 is within 0.006 and recall
is 4 points higher — a missed manipulation costs more than an extra
alert an analyst dismisses. `vwap_threshold` barely matters (0.005-0.015
moves F1 by < 0.01). The script also prints every signal at the job's
defaults, tick-level (price_shock 0.275, wash_trade 0.244, any 0.256).

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
  enters the window. VWAP's accumulators use the raw price.
- **Z-score is the informative signal; VWAP divergence mostly isn't**
  for this injected shock style. It's kept as a secondary OR condition
  per the proposal's spec.

### Out-of-sample evaluation (Z-score and wash-rule components)

`python evaluate_generalization.py --sections split` (~90s). Sections
1-3 study the Z-score and the wash rule (with the scaled lookback) on
their own — the EWMA, CUSUM and volume-spike signals switched off —
tick-level. The detector is causal (each tick is scored only against
earlier ticks), so it runs once over the full timeline and the split is
applied when scoring.

**1. Time split.** Tuned on the first 70% of the timeline (to Sep 18
16:58 UTC, 98,526 ticks); tested on the remaining 42,225. A random split
would leak the future into the past. The tuning picked a 15-tick window,
z=7.0, and wash ratio/range 0.7 / 0.15% (the defaults). Tick-level, any
anomaly:

| | precision | recall | F1 |
|---|---|---|---|
| Tuned components, train (in-sample) | 36.4% | 35.4% | 0.359 |
| **Tuned components, test** | **32.1%** | **38.0%** | **0.348** |
| Current defaults, test | 32.0% | 41.4% | 0.360 |
| Naive fixed threshold, test | 14.1% | 47.6% | 0.217 |
| Random at the components' flag rate, test | 0.8% | 1.0% | 0.009 |

Test F1 is within 0.02 of train F1, so the tuning is not overfit to the
training period.

**2. Baselines, per component (test split, tick-level).**

| | tuned F1 | naive F1 | random F1 | precision lift over random |
|---|---|---|---|---|
| price_shock (Z-score/VWAP) | 0.289 | **0.269** | 0.005 | 56x |
| wash_trade (wash rule) | 0.430 | 0.186 | 0.004 | 120x |

**On the original single-tick shocks, the rolling Z-score alone is
barely better than flagging big tick-to-tick returns** — those shocks
are single-tick jumps of 4-8 local sigma, which a return threshold
catches just as well. With every signal, per event, the gap is wider
(test period 0.404 vs 0.286).

**Leave-one-ticker-out** (tune on four tickers, all days; score the
fifth). "Own-best" is the best F1 that ticker could reach if tuned on
itself:

| held out | tuned window / z | shock F1 | own-best shock F1 | wash F1 | own-best wash F1 |
|---|---|---|---|---|---|
| AAPL | 15 / 6.5 | 0.302 | 0.302 | 0.440 | 0.498 |
| GOOGL | 15 / 6.5 | 0.238 | 0.244 | 0.428 | 0.432 |
| MSFT | 15 / 6.5 | 0.413 | 0.466 | 0.288 | 0.327 |
| NVDA | 20 / 5.5 | 0.259 | 0.272 | 0.394 | 0.397 |
| TSLA | 15 / 6.5 | 0.358 | 0.369 | 0.436 | 0.491 |

The threshold carries over across stocks: shock F1 is within 0.05 and
wash F1 within 0.06 of each ticker's own best. Before the lookback was
scaled to the trading rate, NVDA's held-out wash F1 was 0.118 against an
own-best of 0.250 — a fixed 2-minute lookback spans ~11 NVDA ticks but
~4-6 for the others.

**3. Precision-recall over z** (15-tick window, price_shock,
tick-level):

| z | train P | train R | train F1 | test P | test R | test F1 |
|---|---|---|---|---|---|---|
| 3.0 | 5.1% | 67.4% | 0.094 | 5.8% | 76.5% | 0.108 |
| 4.0 | 12.7% | 55.2% | 0.206 | 15.0% | 63.7% | 0.242 |
| 5.0 | 18.6% | 46.5% | 0.266 | 20.3% | 54.2% | 0.296 |
| 6.0 | 23.4% | 38.3% | 0.291 | 23.7% | 45.3% | 0.311 |
| 7.0 | 27.5% | 34.1% | 0.304 | 23.5% | 37.4% | 0.289 |
| 8.0 | 30.3% | 27.6% | 0.289 | 23.4% | 30.7% | 0.266 |

Test F1 stays within 0.29-0.31 across z = 5.0-7.0, so the default z=5
sits on a broad plateau rather than a lucky point.

### Known limitations

- **Slow ramps stay hard; injected ramps are about a one-standard-
  deviation move, which caps any price-only detector.** Normal prices in
  this data trend: tick-to-tick price changes have lag-1 autocorrelation
  +0.12 to +0.28, and over 15 ticks normal price changes have a standard
  deviation of 5-7x the tick sigma (a random walk would give 3.9x). The
  injected ramps move a total of 4-8 tick sigma, so a ramp looks like an
  ordinary one-sd trend. Every price-only ramp signal tried here
  (fast-vs-slow EWMA, price CUSUM, the rolling window against a slow
  EWMA) trades ramp recall directly against false alarms on normal
  trending periods; ramp recall stays around 17-30%.
- **The price CUSUM (k=2, h=6) was a small regression on the original
  shocks when added** (price_shock F1 0.378 -> 0.374, per event, existing
  variants), traded for ramp F1 0.183 -> 0.225. At lower allowances
  (k <= 1) it floods normal trending periods with flags (original
  price_shock precision 2-16%). Later changes more than recovered the
  original-shock loss (now 0.392).
- **Restarting the CUSUMs costs a little ramp recall** (shock_ramp F1
  0.225 -> 0.218, held-out shock_ramp_long 0.300 -> 0.290, all days) in
  exchange for half the flag volume.
- **Still a noisy output.** ~3% of ticks are flagged (~5-6% when split
  wash trades are present); use `incident_id` and `risk_score` to group
  and rank them. ~7 alerts per ticker-hour on the original data, ~12 for
  NVDA; about 72% of incidents overlap a real anomaly.
- **Finely split wash trades are only partly caught** (held-out 1.2-2x
  prints: recall ~30%): prints that close to normal volume look like an
  ordinary busy, flat stretch.
- **One global risk threshold** moves the families in opposite
  directions (see the risk-threshold table); per-family or per-ticker
  thresholds are a possible v2.
- **The volume signals are really volume-spike-in-a-flat-market
  detectors.** Real wash trading means the same beneficial owner on both
  sides, which the data can't show. They will flag legitimate block
  trades and busy flat stretches.
- **Synthetic labels.** All numbers are against injected anomalies on
  real-anchored synthetic ticks, and the held-out variants come from the
  same injector design with different parameters. Real market data would
  need re-checking.

## Setup

```bash
pip install -r requirements.txt
```

Tested against PySpark 4.2.0 / Java 17.

### Checkpoints: any change to detector state needs a fresh directory

Spark writes each stateful query's state schema into its checkpoint and
refuses to restart a query whose state schema has changed (and state
that does load under a changed detector means something different from
what the new code expects). So the job puts a state layout version in
the checkpoint path: `--checkpoint-dir` (default
`./checkpoints/detect_anomalies`) is used as
`<dir>/state-v<STATE_LAYOUT_VERSION>/baseline`. **Bump
`STATE_LAYOUT_VERSION` in `detect_anomalies.py` whenever
`BASELINE_STATE_SCHEMA` or what the detector keeps in state changes**;
the job then starts from a fresh directory instead of failing on the old
one. A fresh checkpoint also means the query starts from
`--starting-offsets` again and every ticker's baseline re-warms.
Current version: 6. v1: pickled blob only. v2: + typed CUSUM fields.
v3: + trading rate (blob). v4: + open incident (blob). v5: + tick_id
sequence (blob). v6: trading rate, open incident and tick_id sequence
move from the blob to typed fields (14 typed fields in all; the blob
keeps the window lists and the baseline/EWMA scalars). The typed fields
are Spark schema changes; the blob additions are read with defaults by
`load_state`, but the version is bumped for those too, because resumed
state from an older detector isn't what the new one expects. (Dropping
the old running variance sums didn't bump it: a v6 state stays exactly
valid.)

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
reads the output topic back, compares, and **deletes both topics, its
consumer group and the checkpoint when it finishes, even on failure**. It exits non-zero on any
mismatch. Detector flags after `--` go to both the Spark job and the
offline run. `slice` and `compare` do the first and last step alone, for
a pipeline run made by hand.

The offline side reads the same slice file the producer reads. That
matters: pandas' default CSV float parser isn't correctly rounded, so
writing a slice and reading it back shifts some long decimals by one ulp
(61 of 27,923 prices on Sep 16, all NVDA). Scored from the original
parse instead, 5,498 z-scores differed by ~1e-6 while every flag still
matched. That sensitivity came from the old running-sum variance, which
has since been replaced by a direct computation from the window.

**Results** (final detector, current defaults, 2000 records per
micro-batch). Every output field is compared: `price`, `volume`,
`zscore`, `vwap_divergence`, `ewma_divergence`, `cusum_price`,
`cusum_volume`, `risk_score` (floats, within 1e-9 relative), and
`is_anomaly`, `anomaly_type`, `signals`, `incident_id`, `tick_id`
(exact):

| slice | ticks | micro-batches | rows out (Spark / offline) | mismatches, all 13 fields | flags (shock / wash) |
|---|---|---|---|---|---|
| Sep 16 | 27,923 | 14 | 27,923 / 27,923 | 0 | 202 / 661 both |
| Sep 16-17 (crosses an overnight gap) | 55,171 | 28 | 55,171 / 55,171 | 0 | 427 / 1,196 both |

Late drops and watermark drops were 0. After each run only `market-ticks`
and the pre-existing `verify-ordering-checker` group remain on the
broker. Checks from the earlier Z-score-only version of the parity run:
in the two-day run, Spark's own output showed each ticker's first 10
ticks on Sep 17 unscored (baseline re-warming after the gap), then
scored from the 11th; and as a negative control, comparing
against `--baseline-window 21` failed with 27,773 z-score and 25 flag
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
thresholds, EWMA and CUSUM settings, wash-trade lookback/volume-ratio/
price-range settings, risk threshold and incident gap).

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
including a replay with stragglers held back a batch), loading both
earlier checkpoint state layouts, the EWMA and CUSUM signals (typed
state, ramps, split prints in a flat market, reset when price moves,
restart after firing, gap reset), the volume spike, the trading-rate
lookback, the risk score and incident grouping, `tick_id`, one-ulp
input insensitivity of the Z-score, and the versioned checkpoint
path.
`test_parity_check.py` covers the parity comparison itself. Fast (~2s),
good for iterating on the detection logic itself.

### Threshold calibration

```bash
python calibrate_thresholds.py
```

Sweeps the Z-score and VWAP-divergence thresholds (values hardcoded in
the script, not CLI flags) against the full labeled dataset offline —
same `make_baseline_update_fn` the Spark job runs — and reports
precision/recall/F1 per combination (EWMA, CUSUM and volume-spike
signals off, so the sweep isolates the Z-score), plus every signal at
the job's defaults. This produced the Calibration numbers above exactly; doesn't
change any defaults in `detect_anomalies.py`, it only reports.

### Out-of-sample evaluation

```bash
python evaluate_generalization.py                          # sections 1-4
python evaluate_generalization.py --sections variants      # per-event + incidents only
python evaluate_generalization.py --sections variants --period train  # tuning
python evaluate_generalization.py --heldout --naive --period test      # final scoring
```

Time split, leave-one-ticker-out, baselines, precision-recall over z,
per-event and per-incident scores on the harder variants — produces the
evaluation tables above (~2-3 min, pandas only). Tune with
`--period train` only; `--period test` and `--heldout` are for final
scoring.
