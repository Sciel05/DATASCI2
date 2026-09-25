# Cassandra schema notes for the stream-processing output

For whoever builds the Cassandra table in `persistence/`. This note only
describes what `stream-processing/detect_anomalies.py` writes; it doesn't
change any Cassandra code.

## What arrives on the output topic

With `--output-topic <topic>`, the job writes **one message per tick**
(not only anomalies) to Kafka:

- **key**: the ticker, UTF-8 (e.g. `NVDA`)
- **value**: one JSON object with the fields below

Spark's `to_json` **omits null fields**, so a missing key means null.
For example, `zscore` is absent while a ticker's baseline is still
warming up. `timestamp` arrives as an ISO-8601 UTC string with
milliseconds, e.g. `"2026-09-16T13:29:59.970Z"`.

Example row (warming up, not flagged):

```json
{"ticker":"NVDA","timestamp":"2026-09-16T13:29:59.970Z","price":214.14,"volume":18564.0,"is_anomaly":false,"risk_score":0.0,"tick_id":"NVDA-1789565399970-0"}
```

## Fields

Order as emitted (`OUTPUT_SCHEMA` in `detect_anomalies.py`). All are
nullable in the Spark schema; the notes say when each is actually null.

| field | Spark type | suggested CQL type | null when | meaning |
|---|---|---|---|---|
| `ticker` | string | `text` | never | Symbol, e.g. `AAPL` |
| `timestamp` | timestamp | `timestamp` | never | Event time of the tick (ms precision, UTC) |
| `price` | double | `double` | never | Trade price |
| `volume` | double | `double` | never | Trade volume |
| `zscore` | double | `double` | baseline warming up (first 10 ticks after start/overnight gap) | `(price - rolling mean) / rolling std` over the last 20 ticks |
| `vwap_divergence` | double | `double` | same as `zscore` | `abs(price - vwap) / vwap` over the rolling window |
| `is_anomaly` | boolean | `boolean` | never | `risk_score > --risk-threshold` (default 1.0) |
| `anomaly_type` | string | `text` | not flagged | `price_shock` or `wash_trade` only |
| `ewma_divergence` | double | `double` | first 50 ticks after start/gap | Fast-minus-slow price EWMA, in units of its recent RMS (ramp signal) |
| `cusum_price` | double | `double` | first 50 ticks after start/gap | Price CUSUM, `max(up, down)`, in tick-sigma units |
| `cusum_volume` | double | `double` | first 50 ticks after start/gap | Volume CUSUM (flat-market gated), in log-volume sd units |
| `risk_score` | double | `double` | never (0.0 while nothing is scored) | Largest signal as a multiple of its own alarm level |
| `signals` | string | `text` (or `set<text>` after splitting on `,`) | not flagged | Comma-separated signals above the threshold: any of `zscore`, `vwap`, `ewma`, `cusum_price`, `wash_rule`, `cusum_volume` |
| `incident_id` | string | `text` | not flagged | `<ticker>-<first flag's event_time_ms>`; consecutive flags on a ticker share it |
| `tick_id` | string | `text` | never | `<ticker>-<event_time_ms>-<seq>`, unique per tick, stable across retries |

The first eight fields are the original contract and are unchanged; the
last seven were appended.

## Suggested key

```sql
CREATE TABLE market_surveillance.tick_scores (
    ticker           text,
    date             date,        -- UTC date of `timestamp`, derived by the sink
    event_time       timestamp,   -- the `timestamp` field
    tick_id          text,
    price            double,
    volume           double,
    zscore           double,
    vwap_divergence  double,
    is_anomaly       boolean,
    anomaly_type     text,
    ewma_divergence  double,
    cusum_price      double,
    cusum_volume     double,
    risk_score       double,
    signals          text,
    incident_id      text,
    PRIMARY KEY ((ticker, date), event_time, tick_id)
) WITH CLUSTERING ORDER BY (event_time ASC, tick_id ASC);
```

- **Partition key `(ticker, date)`**: one partition per ticker per
  trading day, 3,081-11,765 rows on the labeled data (MSFT smallest,
  NVDA largest). This keeps partitions bounded and matches the natural query,
  "one ticker's day".
- **Clustering `event_time, tick_id`**: rows sort by time within the
  day, and `tick_id` breaks ties between ticks in the same millisecond.
- **Retries overwrite, not duplicate.** Cassandra writes are upserts on
  the primary key. `tick_id` is deterministic for a given input: a
  micro-batch Spark retries after a failure restarts from the committed
  state and produces the same `tick_id`s, so re-writing it replaces the
  same rows. (A message the producer itself sends twice is a different
  tick to the detector and gets the next `seq`.)
- `date` isn't in the payload: derive it from `timestamp` in UTC.

If analysts mainly want alerts, a second table keyed by
`((ticker, date), incident_id, event_time, tick_id)` holding only
`is_anomaly = true` rows would serve "show me today's incidents" without
scanning every tick.
