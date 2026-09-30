"""
verify_consumer.py

Sanity-check tool for Objective 1: confirms that (a) every message for a
given ticker always lands in the same partition, and (b) within a
partition, event_time_ms is non-decreasing (i.e. Kafka preserved the
per-asset chronological order your producer sent).

Every run reads every partition from the beginning and commits no
offsets, so repeated runs always check the whole topic. By default it
stops at the end of the topic as it was when the run started; pass
--follow to keep checking new messages WHILE producer.py is running.

Exits 1 if any violation is found, 0 otherwise.

Usage:
    python verify_consumer.py --topic market-ticks --bootstrap-servers localhost:9092
    python verify_consumer.py --follow   # alongside a running producer
"""

import argparse
import json
import signal
import sys
from collections import defaultdict

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, TopicPartition


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="market-ticks")
    ap.add_argument("--bootstrap-servers", default="localhost:9092")
    ap.add_argument("--max-messages", type=int, default=0,
                     help="Stop after this many messages (0 = no limit)")
    ap.add_argument("--follow", action="store_true",
                     help="Keep consuming past the end of the topic until Ctrl+C")
    args = ap.parse_args()

    # Partitions are assigned directly rather than via subscribe(), and
    # nothing is committed, so the group id never stores a position that a
    # later run would resume from.
    conf = {
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": "verify-ordering-checker",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
    }
    consumer = Consumer(conf)

    topic_md = consumer.list_topics(args.topic, timeout=10).topics[args.topic]
    if topic_md.error is not None:
        print(f"Topic '{args.topic}' unavailable: {topic_md.error}", file=sys.stderr)
        consumer.close()
        sys.exit(2)
    partitions = sorted(topic_md.partitions)

    # Partitions with data still to read; empty ones are done already.
    pending = set()
    for p in partitions:
        low, high = consumer.get_watermark_offsets(
            TopicPartition(args.topic, p), timeout=10
        )
        if high > low:
            pending.add(p)
    consumer.assign([TopicPartition(args.topic, p, OFFSET_BEGINNING) for p in partitions])

    ticker_to_partition = {}
    last_ts_per_partition = defaultdict(lambda: -1)
    violations = []
    count = 0

    def shutdown(*_):
        print_summary()
        consumer.close()
        sys.exit(1 if violations else 0)

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

    print(f"Consuming from '{args.topic}' ({len(partitions)} partitions)... Ctrl+C to stop early.")
    while args.max_messages == 0 or count < args.max_messages:
        if not args.follow and not pending:
            break
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                pending.discard(msg.partition())
            else:
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

    shutdown()


if __name__ == "__main__":
    main()
