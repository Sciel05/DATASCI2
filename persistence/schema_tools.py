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
"""

import argparse
import os
import re
import sys
from pathlib import Path

SCHEMA_PATH = Path(__file__).resolve().parent / "schema.cql"
SCHEMA_KEYSPACE = "surveillance"

# Columns the sink writes to each table (cassandra_sink.py).
REQUIRED_COLUMNS = {
    "raw_events": {"ticker", "event_time", "tick_id", "price", "volume"},
    "anomalies": {
        "ticker", "event_time", "tick_id", "price", "volume", "zscore",
        "vwap_divergence", "is_anomaly", "anomaly_type", "ewma_divergence",
        "cusum_price", "cusum_volume", "risk_score", "signals", "incident_id",
    },
}


class SchemaMismatch(RuntimeError):
    pass


def schema_statements(keyspace=SCHEMA_KEYSPACE):
    """The statements in schema.cql, with the keyspace name replaced."""
    text = SCHEMA_PATH.read_text(encoding="utf-8")
    if keyspace != SCHEMA_KEYSPACE:
        text = re.sub(rf"\b{SCHEMA_KEYSPACE}\b", keyspace, text)
    return [s.strip() for s in text.split(";") if s.strip()]


def apply_schema(session, keyspace=SCHEMA_KEYSPACE, reset=False):
    if reset:
        session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
    for statement in schema_statements(keyspace):
        if statement.upper().startswith("USE "):
            session.set_keyspace(keyspace)
        else:
            session.execute(statement)


def verify_schema(session, keyspace=SCHEMA_KEYSPACE):
    """Raise SchemaMismatch if a table the sink writes is missing columns."""
    problems = []
    for table, required in REQUIRED_COLUMNS.items():
        rows = session.execute(
            "SELECT column_name FROM system_schema.columns "
            "WHERE keyspace_name = %s AND table_name = %s",
            (keyspace, table),
        )
        present = {r.column_name for r in rows}
        if not present:
            problems.append(f"table {keyspace}.{table} does not exist")
        elif required - present:
            problems.append(f"{keyspace}.{table} is missing {sorted(required - present)}")
    if problems:
        raise SchemaMismatch(
            "Cassandra schema does not match persistence/schema.cql: "
            + "; ".join(problems)
            + ". Recreate it with: python -m persistence.schema_tools --reset"
        )


def connect(host, port):
    from cassandra.cluster import Cluster

    cluster = Cluster([host], port=port)
    return cluster, cluster.connect()


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
