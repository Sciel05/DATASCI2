"""
detect_anomalies.py

Spark Structured Streaming job for Objective 2 (Real-Time Stream
Processing & Detection). Consumes the `market-ticks` Kafka topic that
data-ingestion/producer.py produces and emits an anomaly-annotated
stream: {ticker, timestamp, price, volume, zscore, vwap_divergence,
is_anomaly, anomaly_type}.

Two independent detectors feed the same output stream:

1. Statistical baseline (per-ticker rolling Z-score + VWAP divergence),
   via applyInPandasWithState -- PySpark's arbitrary-state API, the
   equivalent of Scala's flatMapGroupsWithState. Keyed by ticker, it
   keeps a trailing window of the last --baseline-window ticks and
   recomputes mean/stddev/VWAP from that window on every event, so the
   baseline tracks each asset's own recent behavior rather than a fixed
   session-wide statistic. Flags anomaly_type="price_shock".

2. Wash-trade heuristic: a separate tumbling-window aggregation
   (high volume + near-zero net price movement in a short window).
   Explicitly a simplified heuristic per the README, not a validated
   detector. Flags anomaly_type="wash_trade".

`label` is present in the Kafka payload for evaluation only (see
data-ingestion/README.md and stream-processing/README.md) and is
dropped immediately after parsing -- it is never read as a detection
input.

Usage:
    python detect_anomalies.py \
        --bootstrap-servers localhost:9092 \
        --input-topic market-ticks \
        --starting-offsets earliest

    # also mirror the output to a Kafka topic for persistence/ to consume:
    python detect_anomalies.py --output-topic market-anomalies

Run with spark-submit so the Kafka connector package resolves, e.g.:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
        detect_anomalies.py --starting-offsets earliest
"""

import argparse
import pickle

import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

TICK_SCHEMA = StructType(
    [
        StructField("ticker", StringType()),
        StructField("event_time_ms", LongType()),
        StructField("price", DoubleType()),
        StructField("volume", DoubleType()),
        StructField("side", StringType()),
        StructField("bid", DoubleType(), True),
        StructField("ask", DoubleType(), True),
        # ground truth only -- dropped right after parsing, never used
        # as a detection input (see module docstring)
        StructField("label", StringType(), True),
    ]
)

OUTPUT_SCHEMA = StructType(
    [
        StructField("ticker", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("price", DoubleType()),
        StructField("volume", DoubleType()),
        StructField("zscore", DoubleType(), True),
        StructField("vwap_divergence", DoubleType(), True),
        StructField("is_anomaly", BooleanType()),
        StructField("anomaly_type", StringType(), True),
    ]
)


# A single opaque blob rather than one StructField per piece of state: PySpark
# 4.2.0's applyInPandasWithState fails to deserialize a state schema
# containing an ArrayType field on the Python worker side (KeyError:
# 'elementType' out of pyspark.sql.types.ArrayType.fromJson -- reproduced
# against a real Kafka source, not just a theoretical concern). Pickling the
# window into BinaryType sidesteps that nested-type schema path entirely.
BASELINE_STATE_SCHEMA = StructType([StructField("blob", BinaryType())])


def make_baseline_update_fn(window_size, min_samples, z_threshold, vwap_threshold, clip_k=5.0):
    """Builds the per-ticker flatMapGroupsWithState function (PySpark:
    applyInPandasWithState). Closes over the tuned thresholds so they don't
    need to be threaded through Spark's fixed (key, pdf_iter, state) signature.
    """

    def update_baseline(key, pdf_iter, state: GroupState):
        if state.exists:
            (blob,) = state.get
            # window: list of (raw_price, clipped_price, volume) -- see the
            # bounded-influence comment below for why raw and clipped diverge.
            window, sum_price, sum_sq, sum_pv, sum_v = pickle.loads(blob)
        else:
            window = []
            sum_price = sum_sq = sum_pv = sum_v = 0.0

        rows_out = []
        for pdf in pdf_iter:
            for row in pdf.sort_values("event_time_ms").itertuples(index=False):
                n = len(window)
                if n >= min_samples:
                    mean = sum_price / n
                    variance = max(sum_sq / n - mean * mean, 0.0)
                    std = variance**0.5
                    zscore = (row.price - mean) / std if std > 1e-9 else 0.0
                    vwap = sum_pv / sum_v if sum_v > 1e-9 else None
                    vwap_div = (
                        abs(row.price - vwap) / vwap
                        if vwap is not None and vwap > 1e-9
                        else None
                    )
                    is_anomaly = bool(
                        abs(zscore) > z_threshold
                        or (vwap_div is not None and vwap_div > vwap_threshold)
                    )
                    anomaly_type = "price_shock" if is_anomaly else None

                    # Bounded-influence update for the running mean/variance:
                    # clip this tick's deviation from the *current* mean to
                    # at most clip_k standard deviations before folding it
                    # in, instead of folding in the raw price unbounded.
                    #
                    # History: an earlier version excluded flagged ticks from
                    # the window entirely, which caused an unbounded feedback
                    # loop on real data (excluding a tick shrinks the
                    # variance estimate, which makes the next tick more
                    # likely to be excluded too, compounding to a ~89% flag
                    # rate). Including ticks raw fixed that but let a single
                    # large shock blow the window's variance wide open for
                    # up to window_size ticks afterward. Clipping keeps every
                    # tick contributing (still self-correcting -- no
                    # exclusion, no lock-up) while capping how far any one
                    # tick can stretch the baseline's tolerance. VWAP is
                    # intentionally untouched (still folds in the raw price)
                    # -- this pass isolates the mean/variance fix only, per
                    # stream-processing/README.md's calibration notes.
                    if std > 1e-9:
                        deviation = row.price - mean
                        clipped_deviation = max(-clip_k * std, min(clip_k * std, deviation))
                        clipped_price = mean + clipped_deviation
                    else:
                        # no established spread yet to clip against
                        clipped_price = row.price
                else:
                    # not enough trailing history yet to trust a baseline --
                    # emit the tick un-scored rather than guess
                    zscore, vwap_div, is_anomaly, anomaly_type = None, None, False, None
                    clipped_price = row.price

                rows_out.append(
                    (
                        row.ticker,
                        row.event_time,
                        row.price,
                        row.volume,
                        zscore,
                        vwap_div,
                        is_anomaly,
                        anomaly_type,
                    )
                )

                # Roll the window forward AFTER scoring this tick, so every
                # price is tested against the baseline of ticks strictly
                # before it -- an anomalous tick never pollutes its own
                # baseline. The clipped price (not the raw price) feeds the
                # mean/variance accumulators; VWAP's accumulators still use
                # the raw price/volume.
                window.append((row.price, clipped_price, row.volume))
                sum_price += clipped_price
                sum_sq += clipped_price * clipped_price
                sum_pv += row.price * row.volume
                sum_v += row.volume
                if len(window) > window_size:
                    old_raw, old_clipped, old_vol = window.pop(0)
                    sum_price -= old_clipped
                    sum_sq -= old_clipped * old_clipped
                    sum_pv -= old_raw * old_vol
                    sum_v -= old_vol

        state.update((pickle.dumps((window, sum_price, sum_sq, sum_pv, sum_v)),))

        yield pd.DataFrame(
            rows_out,
            columns=[
                "ticker",
                "timestamp",
                "price",
                "volume",
                "zscore",
                "vwap_divergence",
                "is_anomaly",
                "anomaly_type",
            ],
        )

    return update_baseline


def build_baseline_stream(parsed, args):
    update_baseline = make_baseline_update_fn(
        window_size=args.baseline_window,
        min_samples=args.min_samples,
        z_threshold=args.zscore_threshold,
        vwap_threshold=args.vwap_threshold,
        clip_k=args.clip_k,
    )
    return (
        parsed.groupBy("ticker").applyInPandasWithState(
            update_baseline,
            outputStructType=OUTPUT_SCHEMA,
            stateStructType=BASELINE_STATE_SCHEMA,
            outputMode="update",
            timeoutConf=GroupStateTimeout.NoTimeout,
        )
    )


def build_wash_trade_stream(parsed, args):
    windowed = (
        parsed.withWatermark("event_time", args.wash_watermark)
        .groupBy(F.col("ticker"), F.window(F.col("event_time"), args.wash_window))
        .agg(
            F.sum("volume").alias("total_volume"),
            F.max("price").alias("max_price"),
            F.min("price").alias("min_price"),
            F.avg("price").alias("avg_price"),
            F.count(F.lit(1)).alias("event_count"),
        )
        .withColumn(
            "price_range_pct",
            (F.col("max_price") - F.col("min_price")) / F.col("avg_price"),
        )
        .filter(
            (F.col("total_volume") >= F.lit(args.wash_volume_threshold))
            & (F.col("price_range_pct") <= F.lit(args.wash_price_stability))
            & (F.col("event_count") >= F.lit(args.wash_min_events))
        )
    )
    return windowed.select(
        F.col("ticker"),
        F.col("window.end").alias("timestamp"),
        F.col("avg_price").alias("price"),
        F.col("total_volume").alias("volume"),
        F.lit(None).cast("double").alias("zscore"),
        F.lit(None).cast("double").alias("vwap_divergence"),
        F.lit(True).alias("is_anomaly"),
        F.lit("wash_trade").alias("anomaly_type"),
    )


def read_parsed_ticks(spark, args):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    return (
        raw.select(F.from_json(F.col("value").cast("string"), TICK_SCHEMA).alias("tick"))
        .select("tick.*")
        .drop("label")
        .withColumn("event_time", (F.col("event_time_ms") / 1000).cast("timestamp"))
    )


def build_sink(anomaly_df, args, name):
    """Wraps an {ticker, timestamp, price, volume, zscore, vwap_divergence,
    is_anomaly, anomaly_type}-shaped stream in a console or Kafka sink. `name`
    scopes the checkpoint directory -- the baseline and wash-trade detectors
    run as two independent streaming queries (Spark rejects
    applyInPandasWithState in update mode if the *same* query plan also
    contains a streaming aggregation, even in a unioned sibling branch), so
    each needs its own checkpoint.
    """
    checkpoint_dir = f"{args.checkpoint_dir.rstrip('/')}/{name}"
    if args.output_topic:
        sink = (
            anomaly_df.select(
                F.col("ticker").alias("key"),
                F.to_json(
                    F.struct(
                        "ticker",
                        "timestamp",
                        "price",
                        "volume",
                        "zscore",
                        "vwap_divergence",
                        "is_anomaly",
                        "anomaly_type",
                    )
                ).alias("value"),
            )
            .writeStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("topic", args.output_topic)
            .option("checkpointLocation", checkpoint_dir)
        )
    else:
        sink = (
            anomaly_df.writeStream.format("console")
            .option("truncate", False)
            .option("checkpointLocation", checkpoint_dir)
        )
    return sink.outputMode("update")


def build_queries(spark, args):
    parsed = read_parsed_ticks(spark, args)
    baseline_sink = build_sink(build_baseline_stream(parsed, args), args, "baseline")
    wash_trade_sink = build_sink(build_wash_trade_stream(parsed, args), args, "wash_trade")
    return [baseline_sink, wash_trade_sink]


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap-servers", default="localhost:9092")
    ap.add_argument("--input-topic", default="market-ticks")
    ap.add_argument(
        "--output-topic",
        default=None,
        help="If set, mirror the anomaly stream to this Kafka topic instead of the console",
    )
    ap.add_argument(
        "--starting-offsets",
        default="latest",
        choices=["latest", "earliest"],
        help="Use 'earliest' to replay the full labeled dataset for evaluation",
    )
    ap.add_argument("--checkpoint-dir", default="./checkpoints/detect_anomalies")

    # statistical baseline (flatMapGroupsWithState / applyInPandasWithState)
    ap.add_argument(
        "--baseline-window",
        type=int,
        default=100,
        help="Trailing tick count per ticker used for rolling mean/std/VWAP",
    )
    ap.add_argument(
        "--min-samples",
        type=int,
        default=30,
        help="Minimum trailing ticks before a baseline is trusted enough to score against",
    )
    ap.add_argument(
        "--clip-k",
        type=float,
        default=5.0,
        help="Bounded-influence cap for the mean/variance update: a tick's deviation from "
        "the current mean is clipped to at most this many standard deviations before "
        "being folded into the running mean/variance, so one large shock can't blow the "
        "window's variance wide open for the next window_size ticks. Every tick still "
        "contributes (self-correcting), unlike an earlier version that excluded flagged "
        "ticks entirely and caused a runaway feedback loop -- see README.md.",
    )
    ap.add_argument(
        "--zscore-threshold",
        type=float,
        default=3.5,
        help="Z-score is the informative signal here. A full sweep (1.5-6.0, see "
        "calibrate_thresholds.py and stream-processing/README.md) found F1 peaks at "
        "z=4.0 (precision 5.5%%/recall 8.6%%), but this defaults to 3.5 instead -- "
        "close to the peak on F1 (precision 4.2%%/recall 11.2%%) but recall-favoring, "
        "since false negatives are worse than false positives for a surveillance use "
        "case. Both precision (~4-5%%) and recall (~8-11%%) are near their ceiling "
        "across the whole sweep -- this is the plain Z-score/VWAP baseline's real "
        "detection limit on this data, not an undertuned threshold.",
    )
    ap.add_argument(
        "--vwap-threshold",
        type=float,
        default=0.01,
        help="Flag when abs(price - vwap) / vwap exceeds this fraction (default 1%%). "
        "On the labeled dataset, real per-tick VWAP divergence for *normal* ticks is "
        "already ~0.4%% at the 99th percentile (window=100) -- the originally-guessed "
        "0.3%% default flagged ~89%% of all ticks. VWAP divergence alone is a weak "
        "signal for this injected price_shock style (near-0%% recall in isolation); "
        "it's kept as a secondary OR condition alongside Z-score, not the primary one.",
    )

    # wash-trade windowed heuristic
    ap.add_argument("--wash-window", default="30 seconds")
    ap.add_argument(
        "--wash-watermark",
        default="1 minute",
        help="How late a tick may arrive before its window is closed",
    )
    ap.add_argument(
        "--wash-volume-threshold",
        type=float,
        default=20000.0,
        help="Minimum total window volume to consider (median wash_trade volume "
        "in the labeled dataset is ~44.6k vs ~4.6k for normal ticks -- tune "
        "against the labeled data per stream-processing/README.md)",
    )
    ap.add_argument(
        "--wash-price-stability",
        type=float,
        default=0.0005,
        help="Max (max_price - min_price) / avg_price within the window (default 0.05%%)",
    )
    ap.add_argument(
        "--wash-min-events",
        type=int,
        default=5,
        help="Minimum ticks in the window before the volume/stability check applies, "
        "so a quiet window can't trivially satisfy the near-zero-movement check",
    )

    ap.add_argument(
        "--trigger-once",
        action="store_true",
        help="Process available data once and stop (useful for evaluation batch runs)",
    )
    return ap.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("market-surveillance-detect-anomalies").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    sinks = build_queries(spark, args)
    if args.trigger_once:
        sinks = [s.trigger(availableNow=True) for s in sinks]
    handles = [s.start() for s in sinks]

    if args.trigger_once:
        for h in handles:
            h.awaitTermination()
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
