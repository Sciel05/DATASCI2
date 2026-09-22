"""
verify_consumer.py

Sanity-check tool for Objective 1: confirms that (a) every message for a
given ticker always lands in the same partition, and (b) within a
partition, event_time_ms is non-decreasing (i.e. Kafka preserved the
per-asset chronological order your producer sent).

Run this WHILE producer.py is running, or after it finishes.

Usage:
    python verify_consumer.py --topic market-ticks --bootstrap-servers localhost:9092
"""

import argparse
import json
import signal
import sys
from collections import defaultdict

from confluent_kafka import Consumer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="market-ticks")
    ap.add_argument("--bootstrap-servers", default="localhost:9092")
    ap.add_argument("--max-messages", type=int, default=5000,
                     help="Stop after this many messages (0 = run forever)")
    args = ap.parse_args()

    conf = {
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": "verify-ordering-checker",
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(conf)
    consumer.subscribe([args.topic])

    ticker_to_partition = {}
    last_ts_per_partition = defaultdict(lambda: -1)
    violations = []
    count = 0

    def shutdown(*_):
        print_summary()
        consumer.close()
        sys.exit(0)

    def print_summary():
        print(f"\nChecked {count} messages across {len(ticker_to_partition)} tickers.")
        print(f"Ticker -> partition mapping: {ticker_to_partition}")
        if violations:
            print(f"ORDERING VIOLATIONS: {len(violations)}")
            for v in violations[:10]:
                print(f"  {v}")
        else:
            print("No ordering violations detected. Per-asset chronological order holds.")

    signal.signal(signal.SIGINT, shutdown)

    print(f"Consuming from '{args.topic}'... Ctrl+C to stop early.")
    while args.max_messages == 0 or count < args.max_messages:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}", file=sys.stderr)
            continue

        key = msg.key().decode("utf-8") if msg.key() else None
        partition = msg.partition()
        value = json.loads(msg.value())
        ts = value["event_time_ms"]
        count += 1

        if key in ticker_to_partition and ticker_to_partition[key] != partition:
            violations.append(
                f"{key} seen on partition {ticker_to_partition[key]} and {partition}"
            )
        ticker_to_partition[key] = partition

        if ts < last_ts_per_partition[partition]:
            violations.append(
                f"partition {partition}: out-of-order event_time_ms "
                f"{ts} after {last_ts_per_partition[partition]} (ticker={key})"
            )
        last_ts_per_partition[partition] = ts

    print_summary()
    consumer.close()


if __name__ == "__main__":
    main()
