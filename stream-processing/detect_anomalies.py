"""
detect_anomalies.py

Spark Structured Streaming job for Objective 2 (Real-Time Stream
Processing & Detection). Consumes the `market-ticks` Kafka topic that
data-ingestion/producer.py produces and emits an anomaly-annotated
stream: {ticker, timestamp, price, volume, zscore, vwap_divergence,
is_anomaly, anomaly_type}.

Both detectors run inside one per-ticker stateful function, via
applyInPandasWithState -- PySpark's arbitrary-state API, the equivalent
of Scala's flatMapGroupsWithState -- so each keeps its trailing history
across micro-batches rather than restarting cold at every batch boundary:

1. Statistical baseline (per-ticker rolling Z-score + VWAP divergence).
   Keeps a trailing window of the last --baseline-window ticks and
   recomputes mean/stddev/VWAP from that window on every event, so the
   baseline tracks each asset's own recent behavior rather than a fixed
   session-wide statistic. Flags anomaly_type="price_shock".

2. Wash-trade heuristic (adapted from the Frank-stream-processing
   branch): a tick is flagged when the ticker's trailing
   --wash-lookback-seconds of ticks moved in price by less than
   --wash-price-range, yet this single tick's volume exceeds
   --wash-volume-ratio of that whole trailing volume -- i.e. a large
   print into a flat market (needs at least --wash-min-prior prior ticks). Explicitly a simplified heuristic per the
   README, not a validated detector. Flags anomaly_type="wash_trade"
   (price_shock takes precedence if both fire on the same tick).

Both detectors start cold after a gap longer than --gap-reset-minutes
(overnight/weekend), so the morning open isn't scored against yesterday's
close.

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

# Earlier versions of this job pickled a positional tuple; these were its
# fields, in order (the 5-field layout predates wash_hist).
_LEGACY_STATE_FIELDS = ["window", "sum_price", "sum_sq", "sum_pv", "sum_v", "wash_hist"]


def empty_state():
    """Per-ticker state. window: list of (raw_price, clipped_price, volume)
    -- see the bounded-influence comment in make_baseline_update_fn for why
    raw and clipped diverge. wash_hist: list of (event_time_ms, price,
    volume) covering the trailing wash lookback. last_ts: event_time_ms of
    the last tick processed."""
    return {
        "window": [],
        "sum_price": 0.0,
        "sum_sq": 0.0,
        "sum_pv": 0.0,
        "sum_v": 0.0,
        "wash_hist": [],
        "last_ts": None,
    }


def load_state(state):
    """Unpickles the state blob into a dict, filling fields that older
    checkpoints didn't have (a dict, so new fields don't need another
    positional layout)."""
    st = empty_state()
    if state.exists:
        (blob,) = state.get
        saved = pickle.loads(blob)
        if isinstance(saved, dict):
            st.update(saved)
        else:
            st.update(zip(_LEGACY_STATE_FIELDS, saved))
    return st


def make_baseline_update_fn(
    window_size,
    min_samples,
    z_threshold,
    vwap_threshold,
    clip_k=5.0,
    wash_lookback_ms=120_000,
    wash_volume_ratio=0.3,
    wash_price_range=0.001,
    wash_min_prior=4,
    gap_reset_ms=30 * 60 * 1000,
):
    """Builds the per-ticker flatMapGroupsWithState function (PySpark:
    applyInPandasWithState). Closes over the tuned thresholds so they don't
    need to be threaded through Spark's fixed (key, pdf_iter, state) signature.
    """

    def update_baseline(key, pdf_iter, state: GroupState):
        st = load_state(state)
        window, wash_hist = st["window"], st["wash_hist"]
        sum_price, sum_sq, sum_pv, sum_v = st["sum_price"], st["sum_sq"], st["sum_pv"], st["sum_v"]
        last_ts = st["last_ts"]

        rows_out = []
        for pdf in pdf_iter:
            for row in pdf.sort_values("event_time_ms", kind="stable").itertuples(index=False):
                if last_ts is not None and row.event_time_ms - last_ts > gap_reset_ms:
                    # Overnight/weekend (or feed outage) gap: yesterday's
                    # close is no baseline for this morning's open, so both
                    # detectors start cold and re-warm.
                    window, wash_hist = [], []
                    sum_price = sum_sq = sum_pv = sum_v = 0.0
                last_ts = row.event_time_ms

                # Wash-trade heuristic, scored against ticks strictly before
                # this one within the lookback (same-millisecond ticks are
                # excluded, matching the original rangeBetween(-lookback, -1)).
                cutoff = row.event_time_ms - wash_lookback_ms
                while wash_hist and wash_hist[0][0] < cutoff:
                    wash_hist.pop(0)
                prior = [h for h in wash_hist if h[0] < row.event_time_ms]
                is_wash_trade = False
                # wash_min_prior (carried over from the old windowed version's
                # --wash-min-events): with only 1-3 prior ticks, any ordinary
                # tick is trivially >30% of the trailing volume
                if len(prior) >= max(wash_min_prior, 1):
                    prior_volume = sum(h[2] for h in prior)
                    lo = min(h[1] for h in prior)
                    hi = max(h[1] for h in prior)
                    is_wash_trade = bool(
                        prior_volume > 0
                        and lo > 0
                        and (hi - lo) / lo < wash_price_range
                        and row.volume > wash_volume_ratio * prior_volume
                    )
                wash_hist.append((row.event_time_ms, row.price, row.volume))

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

                # independent of the baseline's warm-up; price_shock wins ties
                if is_wash_trade and not is_anomaly:
                    is_anomaly, anomaly_type = True, "wash_trade"

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

        state.update(
            (
                pickle.dumps(
                    {
                        "window": window,
                        "sum_price": sum_price,
                        "sum_sq": sum_sq,
                        "sum_pv": sum_pv,
                        "sum_v": sum_v,
                        "wash_hist": wash_hist,
                        "last_ts": last_ts,
                    }
                ),
            )
        )

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
        wash_lookback_ms=int(args.wash_lookback_seconds * 1000),
        wash_volume_ratio=args.wash_volume_ratio,
        wash_price_range=args.wash_price_range,
        wash_min_prior=args.wash_min_prior,
        gap_reset_ms=int(args.gap_reset_minutes * 60 * 1000),
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
    scopes the checkpoint directory under --checkpoint-dir.
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
    # one query: both detectors live in the same stateful function
    return [build_sink(build_baseline_stream(parsed, args), args, "baseline")]


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
        default=20,
        help="Trailing tick count per ticker used for rolling mean/std/VWAP. Short on "
        "purpose: a sweep (see README.md) found 15-25 ticks -- roughly the 2-minute "
        "span that worked on the Frank-stream-processing branch -- beats 100 ticks on "
        "both precision and recall, since a local baseline reacts to the current regime",
    )
    ap.add_argument(
        "--min-samples",
        type=int,
        default=10,
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
        default=5.0,
        help="Z-score is the informative signal here. A sweep (3.0-7.0 at "
        "--baseline-window 20, see calibrate_thresholds.py and README.md) found F1 "
        "peaks at z=5.5 (precision 19.7%%/recall 35.5%%); this defaults to 5.0 -- "
        "F1 within 0.001 of the peak (precision 18.7%%/recall 39.4%%) but "
        "recall-favoring, since false negatives are worse than false positives for "
        "a surveillance use case.",
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

    # wash-trade heuristic (runs inside the same per-ticker state function)
    ap.add_argument(
        "--wash-lookback-seconds",
        type=float,
        default=120.0,
        help="Trailing time span of prior ticks the wash-trade check compares against",
    )
    ap.add_argument(
        "--wash-volume-ratio",
        type=float,
        default=0.3,
        help="Flag a tick whose volume exceeds this fraction of the trailing lookback's "
        "total volume (while the price range stays under --wash-price-range)",
    )
    ap.add_argument(
        "--wash-price-range",
        type=float,
        default=0.001,
        help="Max (max_price - min_price) / min_price across the trailing lookback for "
        "the market to count as flat (default 0.1%%)",
    )
    ap.add_argument(
        "--wash-min-prior",
        type=int,
        default=4,
        help="Minimum prior ticks in the lookback before the wash-trade check applies, "
        "so the first few ticks after a quiet gap can't trivially exceed the volume ratio",
    )

    # stream hygiene
    ap.add_argument(
        "--gap-reset-minutes",
        type=float,
        default=30.0,
        help="Reset a ticker's baseline and wash-trade history when the next tick "
        "arrives more than this long after the previous one (overnight/weekend "
        "gaps are 17.5h+ on the labeled data; there are no intraday gaps over 5 min)",
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
