"""Node-failure drill for the 3-node Cassandra cluster (Objective 4).

Runs `run_evaluation.py live` in the pyspark container and, while it is
streaming, stops one Cassandra node and later starts it again:

    t = warm-up + 0          producer starts (live mode)
    t = warm-up + --down-at  `docker compose stop cassandra3`
    t = warm-up + --up-at    `docker compose start cassandra3`

During each phase it also runs `run_evaluation.py probe`, which writes and
reads keys at LOCAL_QUORUM in an RF=1 and an RF=3 keyspace, to show what the
same outage would do to a single-replica layout. At the end it splits the
live run's per-tick latencies into before / during / after the outage, using
the Docker daemon's own stop/start times (the same clock the containers use).

Run on the host, from the repository root, with the stack up:

    python evaluation/failover_drill.py

With the 3-broker stack (docker-compose.kafka3.yml) a Kafka broker can be
the node stopped instead; the probe still runs against Cassandra:

    python evaluation/failover_drill.py --kafka3 --node kafka3

Writes evaluation/results/failover_<live run id>.json.
"""

import argparse
import json
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DOCKER_DIR = ROOT / "stream-processing" / "docker"
COMPOSE = ["docker", "compose", "-f", str(DOCKER_DIR / "docker-compose.yml")]
RESULTS = ROOT / "evaluation" / "results"


def compose(*args, **kw):
    return subprocess.run([*COMPOSE, *args], capture_output=True, text=True, **kw)


def probe(phase):
    out = compose("exec", "-T", "pyspark", "python3", "evaluation/run_evaluation.py", "probe", "--phase", phase)
    lines = [line for line in out.stdout.splitlines() if line.startswith("{")]
    if out.returncode or not lines:
        print(out.stdout[-2000:], out.stderr[-2000:], file=sys.stderr)
        sys.exit(f"probe {phase} failed")
    result = json.loads(lines[-1])
    print(f"[probe {phase}] {json.dumps(result['keyspaces'])} (driver sees {result['nodes_up_seen_by_driver']} up)",
          flush=True)
    return result


def docker_time(container_id, field):
    """A container's State.StartedAt / FinishedAt as epoch ms."""
    out = subprocess.run(["docker", "inspect", "-f", "{{.State.%s}}" % field, container_id],
                         capture_output=True, text=True, check=True).stdout.strip()
    return int(pd.Timestamp(out).timestamp() * 1000)


def wait_healthy(service, timeout_s=300):
    """Until the container's healthcheck passes; a container without one
    (the Kafka brokers) counts once it has run for 30 s."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        cid = compose("ps", "-q", service).stdout.strip()
        state = subprocess.run(["docker", "inspect", "-f", "{{if .State.Health}}{{.State.Health.Status}}"
                                "{{else}}{{.State.Status}}{{end}}", cid],
                               capture_output=True, text=True).stdout.strip()
        if state == "healthy":
            return
        if state == "running" and time.time() - docker_time(cid, "StartedAt") / 1000 > 30:
            return
        time.sleep(5)
    sys.exit(f"{service} not healthy after {timeout_s}s")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--node", default="cassandra3")
    ap.add_argument("--minutes", type=float, default=6.0, help="length of the live run")
    ap.add_argument("--warmup-sec", type=int, default=60)
    ap.add_argument("--down-at", type=int, default=120, help="seconds into the live run")
    ap.add_argument("--up-at", type=int, default=240, help="seconds into the live run")
    ap.add_argument("--speed", type=float, default=100.0)
    ap.add_argument("--kafka3", action="store_true", help="the stack runs with docker-compose.kafka3.yml")
    args = ap.parse_args()
    if args.kafka3:
        COMPOSE.extend(["-f", str(DOCKER_DIR / "docker-compose.kafka3.yml")])

    probe("setup")
    phases = {"before": probe("before")}

    live_out = {}

    def run_live():
        live_out["proc"] = compose("exec", "-T", "pyspark", "python3", "evaluation/run_evaluation.py", "live",
                                   "--minutes", str(args.minutes), "--speed", str(args.speed),
                                   "--warmup-sec", str(args.warmup_sec))

    live = threading.Thread(target=run_live)
    start = time.time()
    live.start()

    time.sleep(args.warmup_sec + args.down_at)
    cid = compose("ps", "-q", args.node).stdout.strip()
    print(f"[{datetime.now():%H:%M:%S}] stopping {args.node}", flush=True)
    compose("stop", args.node, check=True)
    down_ms = docker_time(cid, "FinishedAt")
    time.sleep(15)  # let the driver notice the node is gone
    phases["outage"] = probe("outage")

    time.sleep(max(0, start + args.warmup_sec + args.up_at - time.time()))
    print(f"[{datetime.now():%H:%M:%S}] starting {args.node}", flush=True)
    compose("start", args.node, check=True)
    up_ms = docker_time(cid, "StartedAt")
    wait_healthy(args.node)
    healthy_ms = int(time.time() * 1000)
    time.sleep(15)
    phases["recovered"] = probe("recovered")

    live.join()
    probe("teardown")
    proc = live_out["proc"]
    if proc.returncode:
        print(proc.stdout[-4000:], proc.stderr[-4000:], file=sys.stderr)
        sys.exit("live run failed")
    run_id = next(line.split()[1].rstrip(":") for line in proc.stdout.splitlines() if line.startswith("run "))
    live = json.loads((RESULTS / f"live_{run_id}.json").read_text())

    lat = pd.read_csv(ROOT / live["latency_csv"])
    lat = lat[lat["table"] == "raw_events"]

    def phase_stats(frame):
        v = frame["latency_ms"]
        return {"ticks": int(len(v)), "p50": round(v.median(), 1), "p95": round(v.quantile(0.95), 1),
                "p99": round(v.quantile(0.99), 1), "max": round(v.max(), 1)} if len(v) else {"ticks": 0}

    result = {
        "node_stopped": args.node,
        "outage_seconds": round((up_ms - down_ms) / 1000, 1),
        "node_rejoined_healthy_after_start_s": round((healthy_ms - up_ms) / 1000, 1),
        "live_run": live,
        "latency_by_phase_ms": {
            "before": phase_stats(lat[lat["ts_ms"] < down_ms]),
            "during_outage": phase_stats(lat[(lat["ts_ms"] >= down_ms) & (lat["ts_ms"] < up_ms)]),
            "after_restart": phase_stats(lat[lat["ts_ms"] >= up_ms]),
        },
        "probe": {k: v["keyspaces"] for k, v in phases.items()},
    }
    RESULTS.mkdir(exist_ok=True)
    path = RESULTS / f"failover_{run_id}.json"
    path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k != "live_run"}, indent=2))
    print(f"live run completeness: {live['completeness']}")
    print(f"wrote {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
