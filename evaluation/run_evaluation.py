"""Objective 4: end-to-end evaluation of the pipeline on the Docker stack.

Every run uses its own Kafka topic and Cassandra keyspace (created from
persistence/schema.cql, so 3 replicas on the 3-node cluster) and removes
them afterwards; what it measures is read back from Cassandra, i.e. what the
pipeline actually stored, at LOCAL_QUORUM.

    batch   Replays the whole labeled dataset (original timestamps) through
            producer -> Kafka -> Spark detector -> Cassandra. Reports
            completeness, detection accuracy against the `label` column,
            and throughput (producer into Kafka; detector + sink).
    live    Runs the detector continuously while the producer replays in
            --live mode for --minutes, then reports latency per tick:
            Cassandra write time (WRITETIME) minus the tick's event time.
    probe   Writes and reads back keys in an RF=1 and an RF=3 keyspace at
            LOCAL_QUORUM; run by failover_drill.py while a node is down.

Run inside the pyspark container, from the repository root:

    docker compose -f stream-processing/docker/docker-compose.yml exec pyspark \\
        python3 evaluation/run_evaluation.py batch

Results go to evaluation/results/<command>_<run id>.json (plus the per-tick
latencies of a live run as a CSV).
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from persistence import schema_tools  # noqa: E402

DATA = ROOT / "data-ingestion" / "aapl_msft_googl_tsla_nvda_ticks_labeled.csv"
PRODUCER = ROOT / "data-ingestion" / "producer.py"
DETECTOR = ROOT / "stream-processing" / "detect_anomalies.py"
RESULTS = Path(__file__).resolve().parent / "results"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
DEFAULT_SPARK_SUBMIT = "/usr/local/lib/python3.10/dist-packages/pyspark/bin/spark-submit"
TICKERS = ("AAPL", "GOOGL", "MSFT", "NVDA", "TSLA")
PARTITIONS = 8

# inject_anomalies.py labels only an event's first tick, but a price shock
# decays over the next 5 ticks and a wash-trade burst lasts 4-9 ticks; a
# flag anywhere in that window has found the event.
EVENT_WINDOW = {"price_shock": 5, "wash_trade": 9}


# --- Kafka / Cassandra plumbing -------------------------------------------

def kafka_admin(bootstrap):
    from confluent_kafka.admin import AdminClient

    return AdminClient({"bootstrap.servers": bootstrap})


def create_topic(admin, name):
    from confluent_kafka.admin import NewTopic

    # replication factor -1: the brokers' default (1 broker: 1; docker-compose.kafka3.yml: 3)
    for f in admin.create_topics([NewTopic(name, num_partitions=PARTITIONS, replication_factor=-1)]).values():
        f.result(timeout=30)


def delete_topic(admin, name):
    for f in admin.delete_topics([name], operation_timeout=30).values():
        try:
            f.result(timeout=30)
        except Exception as e:
            print(f"WARNING: could not delete topic {name}: {e}", file=sys.stderr)


def topic_message_count(bootstrap, topic):
    """Messages in the topic: the sum of its partitions' high watermarks."""
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"eval-count-{uuid.uuid4().hex[:6]}"})
    try:
        return sum(c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in range(PARTITIONS))
    finally:
        c.close()


def spark_submit():
    return shutil.which("spark-submit") or DEFAULT_SPARK_SUBMIT


def detector_cmd(topic, checkpoint, extra=()):
    return [spark_submit(), "--packages", KAFKA_PACKAGE, str(DETECTOR),
            "--bootstrap-servers", ARGS.bootstrap_servers, "--input-topic", topic,
            "--cassandra-sink", "--starting-offsets", "earliest",
            "--checkpoint-dir", checkpoint, *extra]


def read_table(session, keyspace, table, columns):
    """Every row of a table, one partition (ticker) at a time, with the
    write time of its price cell in epoch ms."""
    rows = []
    for ticker in TICKERS:
        rows += session.execute(
            f"SELECT {columns}, writetime(price) AS wt_us FROM {keyspace}.{table} WHERE ticker = %s", (ticker,))
    out = pd.DataFrame([r._asdict() for r in rows])
    if out.empty:
        return out
    # the driver returns naive UTC datetimes, stored at millisecond precision
    out["ts_ms"] = (pd.to_datetime(out["event_time"]) - pd.Timestamp("1970-01-01")) // pd.Timedelta(milliseconds=1)
    out["write_ms"] = out["wt_us"] / 1000.0
    return out


class Run:
    """A throwaway topic + keyspace, removed on exit unless --keep."""

    def __init__(self, prefix):
        self.id = uuid.uuid4().hex[:8]
        self.topic = f"eval-{prefix}-{self.id}"
        self.keyspace = f"eval_{prefix}_{self.id}"
        self.admin = kafka_admin(ARGS.bootstrap_servers)
        self.cluster, self.session = schema_tools.connect(None, None)

    def __enter__(self):
        create_topic(self.admin, self.topic)
        schema_tools.apply_schema(self.session, self.keyspace, reset=True)
        print(f"run {self.id}: topic {self.topic}, keyspace {self.keyspace}")
        self.env = dict(os.environ, CASSANDRA_KEYSPACE=self.keyspace)
        return self

    def __exit__(self, *exc):
        if not ARGS.keep:
            delete_topic(self.admin, self.topic)
            self.session.execute(f"DROP KEYSPACE IF EXISTS {self.keyspace}")
        self.cluster.shutdown()


def save(name, result):
    RESULTS.mkdir(exist_ok=True)
    path = RESULTS / f"{name}.json"
    path.write_text(json.dumps(result, indent=2, default=str) + "\n", encoding="utf-8")
    print(f"wrote {path.relative_to(ROOT)}")
    return path


def stats(values):
    v = np.asarray(values, dtype=float)
    if not len(v):
        return {"n": 0}
    return {"n": int(len(v)), "min": round(float(v.min()), 1), "p50": round(float(np.percentile(v, 50)), 1),
            "p95": round(float(np.percentile(v, 95)), 1), "p99": round(float(np.percentile(v, 99)), 1),
            "max": round(float(v.max()), 1), "mean": round(float(v.mean()), 1)}


# --- detection accuracy ---------------------------------------------------

def ratio(a, b):
    return round(a / b, 4) if b else None


def f1(p, r):
    return round(2 * p * r / (p + r), 4) if p and r else None


def score(ticks, flags):
    """Precision/recall of the stored anomalies against the CSV labels, at
    three levels: tick (strict: the labeled tick itself), event (a flag in
    the event's window) and incident (an alert: one incident_id)."""
    ticks = ticks.sort_values(["ticker", "event_time_ms"], kind="stable").reset_index(drop=True)
    ticks["pos"] = ticks.groupby("ticker").cumcount()
    flags = flags.merge(ticks[["ticker", "event_time_ms", "pos", "label"]],
                        left_on=["ticker", "ts_ms"], right_on=["ticker", "event_time_ms"], how="left")
    unmatched = int(flags["pos"].isna().sum())
    flags = flags.dropna(subset=["pos"]).astype({"pos": int})

    # every tick position -> the event whose window covers it (-1: none)
    events = ticks[ticks["label"] != "normal"].reset_index(drop=True)
    covering = {}
    for ticker, group in ticks.groupby("ticker"):
        cover = np.full(len(group), -1)
        for eid, ev in events[events["ticker"] == ticker].iterrows():
            cover[ev["pos"]:ev["pos"] + EVENT_WINDOW[ev["label"]]] = eid
        covering[ticker] = cover
    flags["event"] = [covering[t][p] for t, p in zip(flags["ticker"], flags["pos"])]
    flags["event_label"] = flags["event"].map(events["label"]).fillna("normal")

    out = {"flags": int(len(flags)), "flags_without_matching_tick": unmatched,
           "labeled_events": {k: int(v) for k, v in events["label"].value_counts().items()}}
    for kind in ("any", "price_shock", "wash_trade"):
        f = flags if kind == "any" else flags[flags["anomaly_type"] == kind]
        ev = events if kind == "any" else events[events["label"] == kind]
        same_type = (f["event_label"] != "normal") if kind == "any" else (f["event_label"] == kind)

        tick_tp = int(((f["label"] != "normal") if kind == "any" else (f["label"] == kind)).sum())
        tick_p, tick_r = ratio(tick_tp, len(f)), ratio(tick_tp, len(ev))

        found = set(f.loc[same_type, "event"])
        ev_p, ev_r = ratio(int(same_type.sum()), len(f)), ratio(len(found & set(ev.index)), len(ev))

        inc = f.groupby("incident_id")["event_label"].agg(
            lambda s: (s != "normal").any() if kind == "any" else (s == kind).any())
        inc_p = ratio(int(inc.sum()), len(inc))

        out[kind] = {
            "tick": {"precision": tick_p, "recall": tick_r, "f1": f1(tick_p, tick_r), "flags": int(len(f))},
            "event": {"precision": ev_p, "recall": ev_r, "f1": f1(ev_p, ev_r),
                      "events_found": len(found & set(ev.index)), "events": int(len(ev))},
            "incident": {"precision": inc_p, "incidents": int(len(inc)), "true_incidents": int(inc.sum())},
        }
    return out


# --- commands -------------------------------------------------------------

def cmd_batch():
    ticks = pd.read_csv(DATA)
    with Run("batch") as run:
        t0 = time.time()
        prod = subprocess.run([sys.executable, str(PRODUCER), "--csv", str(DATA), "--topic", run.topic,
                               "--bootstrap-servers", ARGS.bootstrap_servers, "--max-rate",
                               "--report-every", "1000000"], capture_output=True, text=True, check=True)
        done = re.search(r"Sent (\d+) messages in ([\d.]+)s \(([\d.]+) msgs/sec", prod.stdout)
        produced = topic_message_count(ARGS.bootstrap_servers, run.topic)
        print(f"producer: {done.group(0) if done else prod.stdout[-300:]}; topic holds {produced}")

        checkpoint = tempfile.mkdtemp(prefix="eval-ckpt-")
        t1 = time.time()
        spark = subprocess.run(
            detector_cmd(run.topic, checkpoint,
                         ["--trigger-once", "--max-offsets-per-trigger", str(ARGS.batch_size)]),
            capture_output=True, text=True, env=run.env)
        spark_wall = time.time() - t1
        if spark.returncode:
            print(spark.stderr[-4000:], file=sys.stderr)
            sys.exit(f"spark-submit failed ({spark.returncode})")
        notes = [line for line in spark.stderr.splitlines() if "[detect_anomalies]" in line]
        print("\n".join(notes))

        raw = read_table(run.session, run.keyspace, "raw_events", "ticker, event_time, tick_id")
        anomalies = read_table(run.session, run.keyspace, "anomalies",
                               "ticker, event_time, tick_id, anomaly_type, incident_id, risk_score")

    keys_csv = set(zip(ticks["ticker"], ticks["event_time_ms"]))
    keys_raw = set(zip(raw["ticker"], raw["ts_ms"]))
    sink_span_s = (raw["write_ms"].max() - raw["write_ms"].min()) / 1000.0
    result = {
        "run_id": run.id, "finished_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dataset": {"file": DATA.name, "ticks": int(len(ticks)),
                    "labels": {k: int(v) for k, v in ticks["label"].value_counts().items()}},
        "completeness": {
            "produced_to_kafka": produced, "stored_raw_events": int(len(raw)),
            "missing_from_cassandra": len(keys_csv - keys_raw), "not_in_dataset": len(keys_raw - keys_csv),
            "stored_anomalies": int(len(anomalies)),
            "detector_notes": [n.split("] ", 1)[-1] for n in notes],
        },
        "throughput": {
            "producer_to_kafka_msgs_per_s": float(done.group(3)) if done else None,
            "producer_seconds": float(done.group(2)) if done else None,
            "max_offsets_per_trigger": ARGS.batch_size,
            "spark_job_wall_s": round(spark_wall, 1),
            "detector_and_sink_rows_per_s": round(len(raw) / sink_span_s, 1) if sink_span_s else None,
            "first_to_last_cassandra_write_s": round(sink_span_s, 1),
        },
        "accuracy": score(ticks, anomalies),
        "end_to_end_wall_s": round(time.time() - t0, 1),
    }
    save(f"batch_{run.id}", result)
    print(json.dumps({k: result[k] for k in ("completeness", "throughput")}, indent=2))
    for kind in ("any", "price_shock", "wash_trade"):
        a = result["accuracy"][kind]
        print(f"{kind:12s} tick P/R {a['tick']['precision']}/{a['tick']['recall']}  "
              f"event P/R {a['event']['precision']}/{a['event']['recall']}  "
              f"incident P {a['incident']['precision']} ({a['incident']['incidents']} incidents)")


def cmd_live():
    with Run("live") as run:
        checkpoint = tempfile.mkdtemp(prefix="eval-live-ckpt-")
        log = open(Path(checkpoint).with_suffix(".log"), "w")
        spark = subprocess.Popen(detector_cmd(run.topic, checkpoint), stdout=log, stderr=subprocess.STDOUT,
                                 env=run.env)
        print(f"detector started; {ARGS.warmup_sec}s warm-up before the producer starts", flush=True)
        time.sleep(ARGS.warmup_sec)
        if spark.poll() is not None:
            sys.exit(f"detector exited early ({spark.returncode}); log: {log.name}")

        producer_start = time.time()
        producer = subprocess.Popen(
            [sys.executable, str(PRODUCER), "--csv", str(DATA), "--topic", run.topic,
             "--bootstrap-servers", ARGS.bootstrap_servers, "--live", "--loop",
             "--speed", str(ARGS.speed), "--max-gap-sec", "2", "--report-every", "1000000"],
            stdout=subprocess.DEVNULL)
        print(f"producer started at {datetime.fromtimestamp(producer_start, timezone.utc):%H:%M:%S} UTC "
              f"(--live --speed {ARGS.speed}); running {ARGS.minutes} min", flush=True)
        deadline = producer_start + ARGS.minutes * 60
        while time.time() < deadline:
            time.sleep(10)
            if spark.poll() is not None:
                producer.terminate()
                sys.exit(f"detector died mid-run ({spark.returncode}); log: {log.name}")
        producer.terminate()
        producer.wait()
        produced = topic_message_count(ARGS.bootstrap_servers, run.topic)

        # let the detector drain what's left, then stop it
        for _ in range(ARGS.drain_sec // 5):
            time.sleep(5)
            if run.session.execute(
                    f"SELECT count(*) FROM {run.keyspace}.raw_events").one()[0] >= produced:
                break
        spark.terminate()
        try:
            spark.wait(timeout=60)
        except subprocess.TimeoutExpired:
            spark.kill()

        raw = read_table(run.session, run.keyspace, "raw_events", "ticker, event_time")
        anomalies = read_table(run.session, run.keyspace, "anomalies", "ticker, event_time, anomaly_type")

    raw["latency_ms"] = raw["write_ms"] - raw["ts_ms"]
    anomalies["latency_ms"] = anomalies["write_ms"] - anomalies["ts_ms"]
    RESULTS.mkdir(exist_ok=True)
    csv = RESULTS / f"live_{run.id}_latency.csv"
    pd.concat([raw.assign(table="raw_events"), anomalies.assign(table="anomalies")])[
        ["table", "ticker", "ts_ms", "write_ms", "latency_ms"]].sort_values("ts_ms").to_csv(csv, index=False)
    span = (raw["ts_ms"].max() - raw["ts_ms"].min()) / 1000.0
    result = {
        "run_id": run.id, "finished_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "producer_start_utc_ms": int(producer_start * 1000),
        "settings": {"minutes": ARGS.minutes, "speed": ARGS.speed, "warmup_sec": ARGS.warmup_sec,
                     "consistency": os.getenv("CASSANDRA_CONSISTENCY", "LOCAL_QUORUM")},
        "completeness": {"produced_to_kafka": produced, "stored_raw_events": int(len(raw)),
                         "missing": produced - int(len(raw)), "stored_anomalies": int(len(anomalies))},
        "rate_ticks_per_s": round(len(raw) / span, 1) if span else None,
        "latency_ms": {"raw_events": stats(raw["latency_ms"]), "anomalies": stats(anomalies["latency_ms"])},
        "latency_csv": str(csv.relative_to(ROOT)),
    }
    save(f"live_{run.id}", result)
    print(json.dumps(result, indent=2))


def cmd_probe():
    """Write and read back ARGS.keys keys in an RF=1 and an RF=3 keyspace."""
    from cassandra import ConsistencyLevel
    from cassandra.query import SimpleStatement

    cluster, session = schema_tools.connect(None, None)
    up = sum(1 for h in cluster.metadata.all_hosts() if h.is_up)
    result = {"phase": ARGS.phase, "nodes_up_seen_by_driver": up, "keyspaces": {}}
    try:
        for rf in (1, 3):
            ks = f"eval_probe_rf{rf}"
            if ARGS.phase == "setup":
                session.execute(f"DROP KEYSPACE IF EXISTS {ks}")
                session.execute(f"CREATE KEYSPACE {ks} WITH replication = "
                                f"{{'class': 'NetworkTopologyStrategy', 'datacenter1': {rf}}}")
                session.execute(f"CREATE TABLE {ks}.kv (k text PRIMARY KEY, v text)")
                continue
            if ARGS.phase == "teardown":
                session.execute(f"DROP KEYSPACE IF EXISTS {ks}")
                continue
            write_fail = read_fail = 0
            for i in range(ARGS.keys):
                key = f"{ARGS.phase}-{i}"
                try:
                    session.execute(SimpleStatement(f"INSERT INTO {ks}.kv (k, v) VALUES (%s, %s)",
                                                    consistency_level=ConsistencyLevel.LOCAL_QUORUM),
                                    (key, ARGS.phase), timeout=5)
                except Exception:
                    write_fail += 1
                    continue
                try:
                    session.execute(SimpleStatement(f"SELECT v FROM {ks}.kv WHERE k = %s",
                                                    consistency_level=ConsistencyLevel.LOCAL_QUORUM),
                                    (key,), timeout=5).one()
                except Exception:
                    read_fail += 1
            result["keyspaces"][f"rf{rf}"] = {"keys": ARGS.keys, "write_failures": write_fail,
                                              "read_failures": read_fail}
    finally:
        cluster.shutdown()
    print(json.dumps(result))


def main():
    global ARGS
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap-servers", default="kafka:9092")
    ap.add_argument("--keep", action="store_true", help="keep the run's topic and keyspace")
    sub = ap.add_subparsers(dest="command", required=True)
    b = sub.add_parser("batch")
    b.add_argument("--batch-size", type=int, default=10000, help="--max-offsets-per-trigger for the detector")
    lv = sub.add_parser("live")
    lv.add_argument("--minutes", type=float, default=5.0)
    lv.add_argument("--speed", type=float, default=100.0, help="producer --speed (100 is ~120 ticks/s)")
    lv.add_argument("--warmup-sec", type=int, default=60, help="detector start-up before producing")
    lv.add_argument("--drain-sec", type=int, default=120)
    p = sub.add_parser("probe")
    p.add_argument("--phase", required=True, help="setup | teardown | any label for a write/read round")
    p.add_argument("--keys", type=int, default=200)
    ARGS = ap.parse_args()
    {"batch": cmd_batch, "live": cmd_live, "probe": cmd_probe}[ARGS.command]()


if __name__ == "__main__":
    main()
