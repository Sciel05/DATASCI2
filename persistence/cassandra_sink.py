import os


def write_anomalies_to_cassandra(batch_df, batch_id):
    """
    Writes detected anomalies from a Spark micro-batch
    into Cassandra's surveillance.anomalies table.
    """

    print(f"Processing anomaly batch {batch_id}")

    anomalies_df = (
        batch_df
        .filter("is_anomaly = true")
        .withColumnRenamed("timestamp", "event_time")
        .select(
            "ticker",
            "event_time",
            "zscore",
            "vwap_divergence",
            "anomaly_type"
        )
    )

    def write_partition(rows):
        from cassandra.cluster import Cluster

        cassandra_host = os.getenv("CASSANDRA_HOST", "127.0.0.1")

        cluster = Cluster([cassandra_host], port=9042)
        session = cluster.connect("surveillance")

        insert_statement = session.prepare("""
            INSERT INTO anomalies (
                ticker,
                event_time,
                zscore,
                vwap_divergence,
                anomaly_type
            )
            VALUES (?, ?, ?, ?, ?)
        """)

        for row in rows:
            session.execute(
                insert_statement,
                (
                    row.ticker,
                    row.event_time,
                    row.zscore,
                    row.vwap_divergence,
                    row.anomaly_type,
                )
            )

        cluster.shutdown()

    anomalies_df.foreachPartition(write_partition)

    print(f"Batch {batch_id} written to Cassandra.")


def write_raw_events_to_cassandra(batch_df, batch_id):
    """
    Writes all processed market events from a Spark micro-batch
    into Cassandra's surveillance.raw_events table.
    """

    print(f"Processing raw event batch {batch_id}")

    raw_events_df = (
        batch_df
        .withColumnRenamed("timestamp", "event_time")
        .select(
            "ticker",
            "event_time",
            "price",
            "volume"
        )
    )

    def write_partition(rows):
        from cassandra.cluster import Cluster

        cassandra_host = os.getenv("CASSANDRA_HOST", "127.0.0.1")

        cluster = Cluster([cassandra_host], port=9042)
        session = cluster.connect("surveillance")

        insert_statement = session.prepare("""
            INSERT INTO raw_events (
                ticker,
                event_time,
                price,
                volume
            )
            VALUES (?, ?, ?, ?)
        """)

        for row in rows:
            session.execute(
                insert_statement,
                (
                    row.ticker,
                    row.event_time,
                    row.price,
                    row.volume,
                )
            )

        cluster.shutdown()

    raw_events_df.foreachPartition(write_partition)

    print(f"Raw event batch {batch_id} written to Cassandra.")