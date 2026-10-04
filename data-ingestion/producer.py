"""
producer.py

Replays a tick CSV (from generate_synthetic_data.py OR inject_anomalies.py)
into a Kafka topic, in strict chronological order, keyed by ticker symbol
so that Kafka's partitioning preserves per-asset ordering.

Expects columns: ticker, event_time_ms, price, volume, side
  - inject_anomalies.py's output also has bid/ask/label columns; those are
    passed through into the Kafka payload when present (label is included
    only so your OWN evaluation script can check it - strip it before
    treating the payload as ground truth in Spark, that would be cheating).
  - if your CSV doesn't have a 'side' column (real-data-derived files won't),
    it's synthesized as buy/sell based on whether price ticked up or down.

Pacing: sleeps between sends scaled to the real inter-event time gaps in
the data, divided by --speed. speed=1 replays in real time; speed=50
compresses a 5-minute session into 6 seconds - good for demos. Multi-day
data (real market hours) will include long overnight/weekend gaps - use
--max-gap-sec to cap how long the producer will actually sleep through one
of those, so your demo doesn't stall for a simulated weekend.
Use --max-rate to ignore pacing entirely and fire as fast as the broker
will take it (useful for throughput testing, Objective 4).

Usage:
    python producer.py --csv ticks.csv --topic market-ticks \
        --bootstrap-servers localhost:9092 --speed 50

    python producer.py --csv AAPL_ticks_labeled.csv --topic market-ticks \
        --speed 200 --max-gap-sec 5

    python producer.py --csv ticks.csv --topic market-ticks --max-rate

--live (alias --restamp-now) replaces each tick's event_time_ms with the
wall-clock time it is scheduled to be sent (replay start + its paced offset),
so a replay looks like live trading to anything that filters on "the last N
minutes" (e.g. the live dashboard). Spacing between ticks is the original
spacing divided by --speed, with gaps capped by --max-gap-sec. With
--max-rate there is no schedule, so ticks get the actual send time.
--loop replays the dataset forever; combine it with --live for a demo that
keeps running.

    python producer.py --csv ticks.csv --topic market-ticks-live \
        --speed 5 --max-gap-sec 5 --live --loop
"""

import argparse
import json
import time
import sys

import pandas as pd
from confluent_kafka import Producer


def delivery_report(err, msg):
    if err is not None:
        print(f"  [DELIVERY FAILED] {msg.key()}: {err}", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="ticks.csv")
    ap.add_argument("--topic", default="market-ticks")
    ap.add_argument("--bootstrap-servers", default="localhost:9092")
    ap.add_argument("--speed", type=float, default=50.0,
                    help="Replay speed multiplier (ignored with --max-rate)")
    ap.add_argument("--max-rate", action="store_true",
                    help="Ignore timestamp pacing; send as fast as possible")
    ap.add_argument("--max-gap-sec", type=float, default=2.0,
                    help="Cap simulated pause for any single gap "
                         "(e.g. overnight/weekend) to this many real seconds (default: 2.0)")
    ap.add_argument("--report-every", type=int, default=500,
                    help="Print a progress line every N messages")
    ap.add_argument("--loop", action="store_true",
                    help="Continuously loop replay over the dataset")
    ap.add_argument("--live", "--restamp-now", dest="live", action="store_true",
                    help="Stamp each tick with the wall-clock time it is sent "
                         "instead of its original event time (live pipeline/dashboard)")
    args = ap.parse_args()

    df = pd.read_csv(args.csv).sort_values("event_time_ms").reset_index(drop=True)

    if "side" not in df.columns:
        # real-data-derived files won't have a buy/sell flag - synthesize
        # one from price direction (uptick = buy-pressure, downtick = sell)
        price_diff = df["price"].diff().fillna(0)
        df["side"] = price_diff.apply(lambda d: "buy" if d >= 0 else "sell")

    passthrough_cols = [c for c in ("bid", "ask", "label") if c in df.columns]
    print(f"Loaded {len(df)} events from {args.csv}", flush=True)
    if passthrough_cols:
        print(f"  Passing through extra columns: {passthrough_cols}", flush=True)

    conf = {
        "bootstrap.servers": args.bootstrap_servers,
        "linger.ms": 5,        # small batching window, keeps latency low
        "compression.type": "lz4",
    }
    producer = Producer(conf)

    sent = 0
    iteration = 0

    while True:
        iteration += 1
        if args.loop and iteration > 1:
            print(f"--- Restarting dataset loop (iteration {iteration}) ---", flush=True)

        start_wall = time.time()
        first_event_ms = df["event_time_ms"].iloc[0]
        prev_event_ms = first_event_ms
        time_debt_ms = 0.0  # accumulated real-time gap removed by --max-gap-sec

        for _, row in df.iterrows():
            event_ts_ms = int(row["event_time_ms"])
            if not args.max_rate:
                if args.max_gap_sec is not None:
                    gap_ms = row["event_time_ms"] - prev_event_ms
                    allowed_ms = args.max_gap_sec * 1000 * args.speed
                    if gap_ms > allowed_ms:
                        time_debt_ms += gap_ms - allowed_ms
                    prev_event_ms = row["event_time_ms"]

                target_elapsed = (row["event_time_ms"] - first_event_ms - time_debt_ms) / 1000.0 / args.speed
                if args.live:
                    # the scheduled send time, not time.time(): stays in order and
                    # keeps the (sped-up) spacing even when a send runs late
                    event_ts_ms = int(start_wall * 1000 + target_elapsed * 1000)
                actual_elapsed = time.time() - start_wall
                sleep_for = target_elapsed - actual_elapsed
                if sleep_for > 0:
                    time.sleep(sleep_for)
            elif args.live:
                # no schedule without pacing, so the actual send time
                event_ts_ms = int(time.time() * 1000)

            payload = {
                "ticker": row["ticker"],
                "event_time_ms": event_ts_ms,
                "price": float(row["price"]),
                "volume": float(row["volume"]),
                "side": row["side"],
            }
            for col in passthrough_cols:
                payload[col] = row[col] if not pd.isna(row[col]) else None
            # Key by ticker -> Kafka hashes to the same partition every time,
            # guaranteeing per-asset ordering. This is Objective 1's core requirement.
            producer.produce(
                args.topic,
                key=str(row["ticker"]).encode("utf-8"),
                value=json.dumps(payload).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)  # trigger delivery callbacks without blocking
            sent += 1

            if sent % args.report_every == 0:
                elapsed = time.time() - start_wall
                rate = sent / elapsed if elapsed > 0 else 0
                loop_info = f" [loop {iteration}]" if args.loop else ""
                print(f"  sent {sent}/{len(df)}{loop_info}  ({rate:.1f} msgs/sec avg)", flush=True)

        producer.flush(timeout=10)
        if not args.loop:
            break

    total_elapsed = time.time() - start_wall
    print(f"Done. Sent {sent} messages in {total_elapsed:.1f}s "
          f"({sent / total_elapsed:.1f} msgs/sec avg).", flush=True)


if __name__ == "__main__":
    main()
