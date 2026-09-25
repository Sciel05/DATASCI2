"""
parity_check.py

Streaming-vs-offline parity check. Replays a slice of the labeled data
(one or more days) through the full producer -> Kafka -> Spark pipeline
and through the offline make_baseline_update_fn, then compares the two
tick by tick. They run the same function, so any difference points at what
the streaming path does differently: ordering, keying, state carried
across micro-batches, late drops, or serialization. "It runs in Docker"
only proves it doesn't crash.

Run inside the pyspark container (repo mounted at /workspace):

    python3 parity_check.py run --day 2026-09-16
    python3 parity_check.py run --day 2026-09-16 2026-09-17   # crosses an overnight gap

`run` creates two uniquely named topics, produces the slice with
data-ingestion/producer.py, runs detect_anomalies.py with small
micro-batches (--max-offsets-per-trigger) so per-ticker state has to carry
across batch boundaries, reads the output topic back, compares, and always
deletes the topics it created. Detector flags after `--` are passed to both
the Spark job and the offline run.

`slice` and `compare` do the first and last step alone, for a pipeline run
made by hand (output dumped to parity/spark_output_<days>.jsonl).

Exits non-zero if the two disagree.
"""

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import numpy as np
import pandas as pd

from detect_anomalies import make_baseline_update_fn, parse_args

HERE = Path(__file__).resolve().parent
DATA_PATH = HERE.parent / "data-ingestion" / "aapl_msft_googl_tsla_nvda_ticks_labeled.csv"
PRODUCER = HERE.parent / "data-ingestion" / "producer.py"
OUT_DIR = HERE / "parity"
TOLERANCE = 1e-9  # relative, for float fields
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
DEFAULT_SPARK_SUBMIT = "/usr/local/lib/python3.10/dist-packages/pyspark/bin/spark-submit"

# output fields compared, beyond the (ticker, timestamp) join key
FLOAT_FIELDS = ["price", "volume", "zscore", "vwap_divergence"]
EXACT_FIELDS = ["is_anomaly", "anomaly_type"]


class FakeState:
    """Minimal stand-in for pyspark.sql.streaming.state.GroupState -- see
    test_baseline.py for the same pattern."""

    def __init__(self):
        self._value = None

    @property
    def exists(self):
        return self._value is not None

    @property
    def get(self):
        return self._value

    def update(self, value):
        self._value = value


# --------------------------------------------------------------------------
# slice / offline / load


def slice_name(days):
    return "_".join(days)


def slice_path(days):
    return OUT_DIR / f"ticks_{slice_name(days)}.csv"


def spark_output_path(days):
    return OUT_DIR / f"spark_output_{slice_name(days)}.jsonl"


def write_slice(days):
    df = pd.read_csv(DATA_PATH)
    dates = pd.to_datetime(df["event_time_ms"], unit="ms").dt.strftime("%Y-%m-%d")
    missing = set(days) - set(dates)
    if missing:
        sys.exit(f"no ticks on {sorted(missing)}")
    day_df = df[dates.isin(days)]
    OUT_DIR.mkdir(exist_ok=True)
    day_df.to_csv(slice_path(days), index=False)
    print(f"wrote {len(day_df):,} ticks ({day_df['ticker'].nunique()} tickers) to {slice_path(days)}")
    return day_df


def detector_kwargs(args):
    return dict(
        window_size=args.baseline_window,
        min_samples=args.min_samples,
        z_threshold=args.zscore_threshold,
        vwap_threshold=args.vwap_threshold,
        clip_k=args.clip_k,
        wash_lookback_ms=int(args.wash_lookback_seconds * 1000),
        wash_volume_ratio=args.wash_volume_ratio,
        wash_price_range=args.wash_price_range,
        wash_min_prior=args.wash_min_prior,
        gap_reset_ms=int(args.gap_reset_minutes * 60 * 1000),
    )


def run_offline(ticks, args):
    fn = make_baseline_update_fn(**detector_kwargs(args))
    ticks = ticks.copy()
    ticks["event_time"] = pd.to_datetime(ticks["event_time_ms"], unit="ms")
    parts = []
    for ticker, g in ticks.groupby("ticker"):
        parts.append(pd.concat(list(fn((ticker,), iter([g]), FakeState())), ignore_index=True))
    out = pd.concat(parts, ignore_index=True)
    out["ts_ms"] = (out["timestamp"] - pd.Timestamp("1970-01-01")) // pd.Timedelta(milliseconds=1)
    return out


def parse_spark_rows(lines):
    rows = [json.loads(line) for line in lines if line.strip().startswith("{")]
    out = pd.DataFrame(rows)
    ts = pd.to_datetime(out["timestamp"], utc=True, format="ISO8601")
    out["ts_ms"] = (ts - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(milliseconds=1)
    for col in FLOAT_FIELDS:
        out[col] = pd.to_numeric(out.get(col), errors="coerce")
    for col in EXACT_FIELDS:
        if col not in out:
            out[col] = None
    return out


# --------------------------------------------------------------------------
# comparison


def close(a, b):
    a = np.asarray(a, float)
    b = np.asarray(b, float)
    both_nan = np.isnan(a) & np.isnan(b)
    return both_nan | (np.abs(a - b) <= TOLERANCE * np.maximum(1.0, np.abs(b)))


def compare_frames(offline, spark):
    """Returns (problems, per-field mismatch counts, mismatching rows by
    field). Both frames need `ticker`, `ts_ms` and the compared fields."""
    problems = []
    dupes = spark.duplicated(["ticker", "ts_ms"], keep=False)
    if dupes.any():
        problems.append(f"{int(dupes.sum())} duplicated (ticker, timestamp) rows in spark output")
        spark = spark.drop_duplicates(["ticker", "ts_ms"], keep="last")

    m = offline.merge(spark, on=["ticker", "ts_ms"], how="outer", suffixes=("_off", "_spk"), indicator=True)
    n_only_off = int((m["_merge"] == "left_only").sum())
    n_only_spk = int((m["_merge"] == "right_only").sum())
    if n_only_off:
        problems.append(f"{n_only_off} ticks scored offline but missing from spark output")
    if n_only_spk:
        problems.append(f"{n_only_spk} spark rows with no offline counterpart")
    both = m[m["_merge"] == "both"]

    counts, examples = {}, {}
    for name in FLOAT_FIELDS + EXACT_FIELDS:
        off, spk = both[f"{name}_off"], both[f"{name}_spk"]
        if name in FLOAT_FIELDS:
            bad = ~close(off, spk)
        elif name == "is_anomaly":
            bad = off.astype(bool).to_numpy() != spk.astype(bool).to_numpy()
        else:
            bad = off.fillna("-").astype(str).to_numpy() != spk.fillna("-").astype(str).to_numpy()
        counts[name] = int(np.sum(bad))
        if counts[name]:
            problems.append(f"{counts[name]} {name} mismatches")
            examples[name] = both.loc[np.asarray(bad), ["ticker", "ts_ms", f"{name}_off", f"{name}_spk"]].head(10)
    return problems, counts, examples, len(both)


def report(days, n_ticks, offline, spark):
    print(f"slice {slice_name(days)}: {n_ticks:,} ticks")
    print(f"offline rows: {len(offline):,}   spark rows: {len(spark):,}")
    problems, counts, examples, n_matched = compare_frames(offline, spark)
    for name, n in counts.items():
        print(f"  {name:<16} mismatches: {n}")
        if name in examples:
            print(examples[name].to_string(index=False))
    print("\nflags by type (offline vs spark):")
    for t in ("price_shock", "wash_trade"):
        print(f"  {t:<12} {int((offline['anomaly_type'] == t).sum()):>5} vs {int((spark['anomaly_type'] == t).sum()):>5}")
    if problems:
        print("\nPARITY FAILED:\n  " + "\n  ".join(problems))
        return False
    print(f"\nPARITY OK: all {n_matched:,} ticks match (floats within {TOLERANCE:g} relative)")
    return True


# --------------------------------------------------------------------------
# pipeline run (inside the pyspark container)


def kafka_admin(bootstrap):
    from confluent_kafka.admin import AdminClient

    return AdminClient({"bootstrap.servers": bootstrap})


def create_topics(admin, specs):
    from confluent_kafka.admin import NewTopic

    futures = admin.create_topics([NewTopic(name, num_partitions=p, replication_factor=1) for name, p in specs])
    for name, f in futures.items():
        f.result()
        print(f"created topic {name}")


def delete_topics(admin, names):
    futures = admin.delete_topics(list(names), operation_timeout=30)
    for name, f in futures.items():
        try:
            f.result()
            print(f"deleted topic {name}")
        except Exception as e:  # keep deleting the rest; report what failed
            print(f"WARNING: could not delete topic {name}: {e}", file=sys.stderr)


def consume_all(bootstrap, topic, idle_timeout_s=15.0):
    """Reads every message on `topic` (one partition), stopping at the end
    of the partition or after `idle_timeout_s` without messages."""
    from confluent_kafka import Consumer, KafkaError

    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"parity-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
            "enable.auto.commit": False,
        }
    )
    c.subscribe([topic])
    lines, last = [], time.time()
    try:
        while time.time() - last < idle_timeout_s:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                raise RuntimeError(msg.error())
            lines.append(msg.value().decode("utf-8"))
            last = time.time()
    finally:
        c.close()
    return lines


def cmd_run(days, detector_argv, bootstrap, spark_submit, batch_size):
    args = parse_args(detector_argv)
    write_slice(days)
    # Score offline from the same file the producer reads, parsed the same
    # way: pandas' default CSV float parser isn't correctly rounded, so a
    # write + re-read shifts some long decimals by one ulp, which is enough
    # to move running-sum z-scores by ~1e-6.
    ticks = pd.read_csv(slice_path(days))
    run_id = uuid.uuid4().hex[:8]
    in_topic, out_topic = f"parity-ticks-{run_id}", f"parity-anomalies-{run_id}"
    checkpoint = tempfile.mkdtemp(prefix="parity-ckpt-")
    admin = kafka_admin(bootstrap)
    create_topics(admin, [(in_topic, 8), (out_topic, 1)])
    try:
        subprocess.run(
            [sys.executable, str(PRODUCER), "--csv", str(slice_path(days)), "--topic", in_topic,
             "--bootstrap-servers", bootstrap, "--max-rate", "--report-every", "1000000"],
            check=True,
        )
        spark = subprocess.run(
            [spark_submit, "--packages", KAFKA_PACKAGE, str(HERE / "detect_anomalies.py"),
             "--bootstrap-servers", bootstrap, "--input-topic", in_topic, "--output-topic", out_topic,
             "--starting-offsets", "earliest", "--trigger-once",
             "--max-offsets-per-trigger", str(batch_size), "--checkpoint-dir", checkpoint,
             *detector_argv],
            capture_output=True, text=True,
        )
        for line in spark.stderr.splitlines():
            if "[detect_anomalies]" in line:
                print(line)
        if spark.returncode:
            print(spark.stderr[-4000:], file=sys.stderr)
            sys.exit(f"spark-submit failed ({spark.returncode})")
        commits = Path(checkpoint, "baseline", "commits")
        n_batches = len([p for p in commits.iterdir() if p.name.isdigit()]) if commits.exists() else 0
        print(f"spark job committed {n_batches} micro-batches (<= {batch_size} records each)")

        lines = consume_all(bootstrap, out_topic)
        spark_output_path(days).write_text("\n".join(lines) + "\n", encoding="utf-8")
    finally:
        delete_topics(admin, [in_topic, out_topic])
        shutil.rmtree(checkpoint, ignore_errors=True)

    ok = report(days, len(ticks), run_offline(ticks, args), parse_spark_rows(lines))
    sys.exit(0 if ok else 1)


def cmd_compare(days, detector_argv):
    args = parse_args(detector_argv)
    ticks = pd.read_csv(slice_path(days))
    lines = spark_output_path(days).read_text(encoding="utf-8-sig").splitlines()
    ok = report(days, len(ticks), run_offline(ticks, args), parse_spark_rows(lines))
    sys.exit(0 if ok else 1)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("run", "slice", "compare"):
        p = sub.add_parser(name)
        p.add_argument("--day", required=True, nargs="+", help="UTC date(s), e.g. 2026-09-16 2026-09-17")
        if name == "run":
            p.add_argument("--bootstrap-servers", default="kafka:9092")
            p.add_argument("--spark-submit", default=shutil.which("spark-submit") or DEFAULT_SPARK_SUBMIT)
            p.add_argument("--batch-size", type=int, default=2000, help="max Kafka records per micro-batch")
        if name != "slice":
            p.add_argument("detector_args", nargs=argparse.REMAINDER, help="-- then detect_anomalies.py flags")
    a = ap.parse_args()
    extra = getattr(a, "detector_args", [])
    extra = extra[1:] if extra[:1] == ["--"] else extra
    if a.cmd == "slice":
        write_slice(a.day)
    elif a.cmd == "compare":
        cmd_compare(a.day, extra)
    else:
        cmd_run(a.day, extra, a.bootstrap_servers, a.spark_submit, a.batch_size)


if __name__ == "__main__":
    main()
