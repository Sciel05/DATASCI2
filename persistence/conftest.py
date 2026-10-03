"""Shared fixtures for the persistence tests.

Tests that touch Cassandra get a throwaway keyspace (surveillance_test_<id>)
built from schema.cql and dropped afterwards, so they never write into the
pipeline's own ``surveillance`` keyspace.

If Cassandra isn't reachable those tests are skipped, unless
REQUIRE_CASSANDRA=1 (set in the Docker stack), where a missing Cassandra is
a failure rather than a silent skip.

    pytest persistence/                      # from the repository root
"""

import os
import sys
import uuid
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_KEYSPACE = f"surveillance_test_{uuid.uuid4().hex[:8]}"

# Set before any SparkSession starts: Spark's Python workers inherit this
# environment, and the sink reads its keyspace from it on the executors and
# imports persistence.cassandra_sink by name.
os.environ["CASSANDRA_KEYSPACE"] = TEST_KEYSPACE
os.environ["PYTHONPATH"] = os.pathsep.join(
    p for p in (str(PROJECT_ROOT), os.environ.get("PYTHONPATH")) if p
)


@pytest.fixture(scope="session")
def cassandra_session():
    pytest.importorskip("cassandra", reason="cassandra-driver is not installed")
    from cassandra.cluster import Cluster, NoHostAvailable

    from persistence.schema_tools import apply_schema, verify_schema

    host = os.getenv("CASSANDRA_HOST", "127.0.0.1")
    port = int(os.getenv("CASSANDRA_PORT", "9042"))
    cluster = Cluster([host], port=port, connect_timeout=5)
    try:
        session = cluster.connect()
    except NoHostAvailable:
        cluster.shutdown()
        message = f"no Cassandra reachable at {host}:{port}"
        if os.getenv("REQUIRE_CASSANDRA") == "1":
            pytest.fail(message)
        pytest.skip(message)

    apply_schema(session, TEST_KEYSPACE, reset=True)
    verify_schema(session, TEST_KEYSPACE)
    session.set_keyspace(TEST_KEYSPACE)
    try:
        yield session
    finally:
        session.execute(f"DROP KEYSPACE IF EXISTS {TEST_KEYSPACE}")
        cluster.shutdown()


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark", reason="pyspark is not installed")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("persistence-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
