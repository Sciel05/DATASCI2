# Data Ingestion (Objective 1)

Kafka producer + data source for the market surveillance pipeline. Owns
Objective 1: Event Ingestion & Partitioning. Feeds the `market-ticks`
Kafka topic that Objective 2 (Spark) consumes from.

## Dataset in use

`aapl_msft_googl_tsla_nvda_ticks_labeled.csv` — hybrid real+synthetic,
labeled:

- **Base**: real 1-minute OHLCV bars for AAPL, MSFT, GOOGL, TSLA, NVDA
  (5 trading days, Sep 15–21 2026) via `yfinance`.
- **Tick upsampling**: each 1-min bar expanded into 5–80 synthetic trades
  (volume-proportional), price constrained to stay within that bar's
  high/low via a Brownian-bridge-style random path from open to close.
  Real market dynamics at the bar level; synthetic path *within* each bar
  (true tick-by-tick data isn't available for free — 1-min bars are the
  finest real granularity yfinance offers).
- **Anomaly injection**: `inject_anomalies.py` adds labeled
  `price_shock` and `wash_trade` events on top, session-aware (never
  injects across overnight/weekend gaps, scales shock size to each
  session's own local volatility).

140,751 total ticks, 1,176 labeled anomalies (0.84%):

| Ticker | Ticks  | price_shock | wash_trade |
|--------|--------|--------------|------------|
| AAPL   | 24,155 | 102          | 116        |
| GOOGL  | 19,451 | 68           | 77         |
| MSFT   | 16,782 | 78           | 67         |
| NVDA   | 51,593 | 210          | 212        |
| TSLA   | 28,770 | 123          | 123        |

Columns: `ticker, event_time_ms, price, bid, ask, volume, label`.
`label` is ground truth for **your own evaluation only** — Spark must not
read it as a detection input, that would be cheating the results.

## Setup

```bash
pip install -r requirements.txt
```

Kafka broker expected at `localhost:9092` (override with
`--bootstrap-servers`). Create the topic first:

```bash
kafka-topics.sh --create --topic market-ticks \
  --partitions 8 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

## Run it

```bash
python producer.py --csv aapl_msft_googl_tsla_nvda_ticks_labeled.csv \
    --topic market-ticks --speed 200 --max-gap-sec 5
```

`--speed 200 --max-gap-sec 5` replays the full 6-day span in ~10 minutes
(caps how long the producer sleeps through any single gap, e.g. the
weekend). Use `--max-rate` instead for throughput testing (Objective 4).

Verify partitioning/ordering (Objective 1 checkpoint):

```bash
python verify_consumer.py --topic market-ticks --bootstrap-servers localhost:9092
```

Confirms each ticker consistently maps to one partition and
`event_time_ms` never goes backwards within a partition. Zero violations
= Objective 1 is done.

## Files

| File | Purpose |
|---|---|
| `producer.py` | Kafka producer — replays the CSV in order, keyed by ticker |
| `inject_anomalies.py` | Adds labeled anomalies to real-anchored tick data |
| `generate_synthetic_data.py` | Fully synthetic fallback generator (no real-data dependency) |
| `verify_consumer.py` | Confirms partition consistency + ordering |
| `aapl_msft_googl_tsla_nvda_ticks_labeled.csv` | The dataset described above |

## Regenerating / extending the dataset

The real-bar → tick upsampling step ran in Colab (not included here as a
script yet — ask in the group chat if you need the notebook cell). To
add more tickers or a longer window, re-run that upsampling with a wider
`tickers` list or longer `period`, then:

```bash
python inject_anomalies.py --in <raw_ticks>.csv \
    --out <labeled>.csv --anomaly-rate 0.008
```

`inject_anomalies.py` already groups by `symbol` and is session-aware, so
multi-ticker input works without any code changes.
