"""Apply, reset and check the Cassandra schema in ``schema.cql``.

``schema.cql`` only uses ``CREATE ... IF NOT EXISTS``, so re-applying it
after a schema change silently keeps the old tables, and the sink fails
later on its first insert. ``verify_schema`` catches that up front, and
``--reset`` drops and recreates the keyspace.

    python -m persistence.schema_tools            # apply schema.cql
    python -m persistence.schema_tools --reset    # drop keyspace, re-apply
    python -m persistence.schema_tools --check    # only check the columns

Host, port and keyspace come from CASSANDRA_HOST / CASSANDRA_PORT /
CASSANDRA_KEYSPACE (defaults 127.0.0.1 / 9042 / surveillance), or the
matching flags. Run from the repository root.

CASSANDRA_HOST may list several contact points, comma-separated
("cassandra1,cassandra2,cassandra3" in the Docker stack), so a client can
still connect while one node is down. Every session made by ``cluster()``
reads and writes at CASSANDRA_CONSISTENCY (default LOCAL_QUORUM).

schema.cql asks for 3 replicas; on a cluster with fewer nodes (a single
local Cassandra) ``apply_schema`` creates the keyspace with as many
replicas as there are nodes, since a quorum of 3 can't be met by 1 node.
"""

import argparse
import os
import re
import sys
from pathlib import Path

SCHEMA_PATH = Path(__file__).resolve().parent / "schema.cql"
SCHEMA_KEYSPACE = "surveillance"
REPLICATION_RE = re.compile(r"'(\w+)'\s*:\s*(\d+)\s*}")

# Columns the sink writes to each table (cassandra_sink.py).
REQUIRED_COLUMNS = {
    "raw_events": {"ticker", "day", "event_time", "tick_id", "price", "volume"},
    "anomalies": {
        "ticker", "day", "event_time", "tick_id", "price", "volume", "zscore",
        "vwap_divergence", "is_anomaly", "anomaly_type", "ewma_divergence",
        "cusum_price", "cusum_volume", "risk_score", "signals", "incident_id",
    },
}


# Both tables' partition key. A primary key can't be altered, so a table
# created with the old ((ticker)) key has to be recreated (--reset).
PARTITION_KEY = {"ticker", "day"}


class SchemaMismatch(RuntimeError):
    pass


def schema_statements(keyspace=SCHEMA_KEYSPACE):
    """The statements in schema.cql, with the keyspace name replaced.
    Comments are dropped first, so a ';' inside one can't split a statement
    (schema.cql has no string literals containing '--')."""
    text = re.sub(r"--[^\n]*", "", SCHEMA_PATH.read_text(encoding="utf-8"))
    if keyspace != SCHEMA_KEYSPACE:
        text = re.sub(rf"\b{SCHEMA_KEYSPACE}\b", keyspace, text)
    return [s.strip() for s in text.split(";") if s.strip()]


def fit_replication(statement, hosts):
    """Fit schema.cql's 'datacenter1': 3 to the cluster's own datacenter
    name and node count (a factor above the node count makes every
    QUORUM request fail as unavailable)."""
    match = REPLICATION_RE.search(statement)
    datacenters = {h.datacenter for h in hosts if h.datacenter}
    if not match or len(datacenters) != 1:
        return statement
    datacenter = datacenters.pop()
    factor = min(int(match.group(2)), len(hosts))
    if (datacenter, factor) != (match.group(1), int(match.group(2))):
        print(f"schema_tools: keyspace replication {datacenter}={factor} "
              f"(schema.cql asks {match.group(1)}={match.group(2)}; "
              f"cluster has {len(hosts)} node(s))", file=sys.stderr)
    start, end = match.span(1)[0] - 1, match.span(2)[1]
    return statement[:start] + f"'{datacenter}': {factor}" + statement[end:]


def apply_schema(session, keyspace=SCHEMA_KEYSPACE, reset=False):
    if reset:
        session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
    hosts = list(session.cluster.metadata.all_hosts())
    for statement in schema_statements(keyspace):
        if "CREATE KEYSPACE" in statement.upper():
            statement = fit_replication(statement, hosts)
        if statement.upper().startswith("USE "):
            session.set_keyspace(keyspace)
        else:
            session.execute(statement)


def verify_schema(session, keyspace=SCHEMA_KEYSPACE):
    """Raise SchemaMismatch if a table the sink writes is missing columns."""
    problems = []
    for table, required in REQUIRED_COLUMNS.items():
        rows = list(session.execute(
            "SELECT column_name, kind FROM system_schema.columns "
            "WHERE keyspace_name = %s AND table_name = %s",
            (keyspace, table),
        ))
        present = {r.column_name for r in rows}
        partition_key = {r.column_name for r in rows if r.kind == "partition_key"}
        if not present:
            problems.append(f"table {keyspace}.{table} does not exist")
        elif required - present:
            problems.append(f"{keyspace}.{table} is missing {sorted(required - present)}")
        elif partition_key != PARTITION_KEY:
            problems.append(f"{keyspace}.{table} is partitioned by {sorted(partition_key)}, "
                            f"not {sorted(PARTITION_KEY)}")
    if problems:
        hint = (
            " The tables were created before the 2026-10-04 change to the "
            "((ticker, day)) partition key (see the note at the top of schema.cql)."
            if any("partitioned by" in p or "missing ['day']" in p for p in problems) else ""
        )
        raise SchemaMismatch(
            "Cassandra schema does not match persistence/schema.cql: "
            + "; ".join(problems)
            + "." + hint
            + " Recreate it (this deletes the keyspace's data) with: python -m persistence.schema_tools --reset"
        )


def contact_points(hosts=None):
    """CASSANDRA_HOST (or ``hosts``), split on commas."""
    hosts = hosts or os.getenv("CASSANDRA_HOST", "127.0.0.1")
    return [h.strip() for h in hosts.split(",") if h.strip()]


def cluster(hosts=None, port=None, request_timeout=10.0, **kwargs):
    """A Cluster on every contact point whose sessions use
    CASSANDRA_CONSISTENCY (default LOCAL_QUORUM) for reads and writes, and
    ``request_timeout`` seconds per request (the driver's default is 10)."""
    from cassandra import ConsistencyLevel
    from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile

    level = os.getenv("CASSANDRA_CONSISTENCY", "LOCAL_QUORUM").upper()
    profile = ExecutionProfile(
        consistency_level=getattr(ConsistencyLevel, level),
        request_timeout=request_timeout,
    )
    return Cluster(
        contact_points(hosts),
        port=port or int(os.getenv("CASSANDRA_PORT", "9042")),
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        **kwargs,
    )


def connect(host, port):
    c = cluster(host, port)
    return c, c.connect()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default=os.getenv("CASSANDRA_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.getenv("CASSANDRA_PORT", "9042")))
    ap.add_argument("--keyspace", default=os.getenv("CASSANDRA_KEYSPACE", SCHEMA_KEYSPACE))
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--reset", action="store_true", help="drop the keyspace (and its data) first")
    mode.add_argument("--check", action="store_true", help="only check the tables' columns")
    args = ap.parse_args()

    cluster, session = connect(args.host, args.port)
    try:
        if not args.check:
            apply_schema(session, args.keyspace, reset=args.reset)
            print(f"{'reset and applied' if args.reset else 'applied'} schema.cql to keyspace {args.keyspace}")
        verify_schema(session, args.keyspace)
        print(f"keyspace {args.keyspace} matches schema.cql")
    except SchemaMismatch as e:
        sys.exit(str(e))
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
