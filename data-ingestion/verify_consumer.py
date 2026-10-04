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

--watch is --follow with a live screen for demos: one row per partition
(its tickers, leader broker, messages checked, newest event time,
violations), redrawn every --refresh seconds, under a status line that
reads ORDER HOLDS or ORDER VIOLATED. With the 3-broker stack, stopping a
broker mid-demo shows the partition leaders move while the order holds.

Exits 1 if any violation is found, 0 otherwise.

Usage:
    python verify_consumer.py --topic market-ticks --bootstrap-servers localhost:9092
    python verify_consumer.py --follow   # alongside a running producer
    python verify_consumer.py --watch    # same, as a live screen (demo)
"""

import argparse
import json
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, TopicPartition

GREEN, RED, DIM, BOLD, RESET = "\033[92m", "\033[91m", "\033[2m", "\033[1m", "\033[0m"


def fmt_ms(ms):
    if ms < 0:
        return "-"
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="market-ticks")
    ap.add_argument("--bootstrap-servers", default="localhost:9092")
    ap.add_argument("--max-messages", type=int, default=0,
                     help="Stop after this many messages (0 = no limit)")
    ap.add_argument("--follow", action="store_true",
                     help="Keep consuming past the end of the topic until Ctrl+C")
    ap.add_argument("--watch", action="store_true",
                     help="--follow with a live per-partition screen (for demos)")
    ap.add_argument("--refresh", type=float, default=1.0,
                     help="Seconds between screen redraws with --watch")
    args = ap.parse_args()
    if args.watch:
        args.follow = True

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
    count_per_partition = defaultdict(int)
    violations_per_partition = defaultdict(int)
    tickers_per_partition = defaultdict(set)
    violations = []
    count = 0
    started = time.time()
    stop = []

    def request_stop(*_):
        # Only set a flag: calling the Kafka client from a signal handler,
        # while poll() is running, can deadlock it.
        stop.append(True)

    def print_summary():
        print(f"\nChecked {count} messages across {len(ticker_to_partition)} tickers.")
        print(f"Ticker -> partition mapping: {ticker_to_partition}")
        if violations:
            print(f"ORDERING VIOLATIONS: {len(violations)}")
            for v in violations[:10]:
                print(f"  {v}")
        else:
            print("No ordering violations detected. Per-asset chronological order holds.")

    def draw():
        """The live screen: status line, then one row per partition."""
        leaders = {p: md.leader for p, md in
                   consumer.list_topics(args.topic, timeout=5).topics[args.topic].partitions.items()}
        rate = count / max(time.time() - started, 1e-9)
        if violations:
            status = f"{RED}{BOLD}ORDER VIOLATED{RESET}{RED}: {len(violations)} violation(s){RESET}"
        else:
            status = f"{GREEN}{BOLD}ORDER HOLDS{RESET}{GREEN}: 0 violations in {count:,} messages{RESET}"
        lines = [
            "\033[H\033[2J",  # cursor home + clear screen
            f"{BOLD}Kafka per-partition order proof{RESET}  topic {args.topic} @ {args.bootstrap_servers}",
            f"Every ticker must stay on one partition, and event_time_ms must never go back "
            f"within a partition.  {DIM}{datetime.now():%H:%M:%S}  {rate:,.0f} msg/s checked{RESET}",
            "",
            f"  {status}",
            "",
            f"  {'partition':>9}  {'leader':>6}  {'tickers':<14} {'messages':>10}  {'newest event_time (UTC)':<23}  {'violations':>10}",
            f"  {'-' * 9}  {'-' * 6}  {'-' * 14} {'-' * 10}  {'-' * 23}  {'-' * 10}",
        ]
        for p in partitions:
            n = count_per_partition[p]
            v = violations_per_partition[p]
            tickers = ",".join(sorted(tickers_per_partition[p])) or "-"
            colour = RED if v else (GREEN if n else DIM)
            lines.append(
                f"  {colour}{p:>9}  {'b' + str(leaders.get(p, '?')):>6}  {tickers:<14} {n:>10,}  "
                f"{fmt_ms(last_ts_per_partition[p]):<23}  {v:>10}{RESET}"
            )
        lines += ["", f"  {DIM}ticker -> partition: {dict(sorted(ticker_to_partition.items()))}{RESET}"]
        if violations:
            lines += ["", f"  {RED}latest violations:{RESET}"] + [f"    {x}" for x in violations[-5:]]
        lines += ["", f"  {DIM}Ctrl+C to stop{RESET}"]
        sys.stdout.write("\n".join(lines) + "\n")
        sys.stdout.flush()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    if not args.watch:
        print(f"Consuming from '{args.topic}' ({len(partitions)} partitions)... Ctrl+C to stop early.")
    next_draw = 0.0
    while not stop and (args.max_messages == 0 or count < args.max_messages):
        if args.watch and time.time() >= next_draw:
            draw()
            next_draw = time.time() + args.refresh
        if not args.follow and not pending:
            break
        msg = consumer.poll(0.2 if args.watch else 1.0)
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
        count_per_partition[partition] += 1
        tickers_per_partition[partition].add(key)

        if key in ticker_to_partition and ticker_to_partition[key] != partition:
            violations.append(
                f"{key} seen on partition {ticker_to_partition[key]} and {partition}"
            )
            violations_per_partition[partition] += 1
        ticker_to_partition[key] = partition

        if ts < last_ts_per_partition[partition]:
            violations.append(
                f"partition {partition}: out-of-order event_time_ms "
                f"{ts} after {last_ts_per_partition[partition]} (ticker={key})"
            )
            violations_per_partition[partition] += 1
        last_ts_per_partition[partition] = ts

    if args.watch:
        draw()
    print_summary()
    consumer.close()
    sys.exit(1 if violations else 0)


if __name__ == "__main__":
    main()
