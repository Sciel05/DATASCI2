from pyspark.sql import functions as F
from pyspark.sql.window import Window

TWO_MINUTES_MS = 2 * 60 * 1000

def detect_anomalies(df):
    time_window = (
        Window.partitionBy("ticker")
        .orderBy("event_time_ms")
        .rangeBetween(-TWO_MINUTES_MS, -1)
    )

    df = df.withColumn("rolling_mean", F.avg("price").over(time_window))
    df = df.withColumn("rolling_std", F.stddev("price").over(time_window))
    df = df.withColumn(
        "zscore",
        F.when(F.col("rolling_std") > 0, (F.col("price") - F.
        col("rolling_mean")) / F.col("rolling_std")).otherwise(0.0)
    )

    df = df.withColumn("cum_pv", F.sum(F.col("price") * F.col("volume")).over(time_window))
    df = df.withColumn("cum_v", F.sum("volume").over(time_window))
    df = df.withColumn(
        "vwap",
        F.when(F.col("cum_v") > 0, F.col("cum_pv") / F.col("cum_v")).otherwise(F.col("price"))
    )
    df = df.withColumn("vwap_divergence", F.abs(F.col("price") - F.col("vwap")) / F.col("vwap"))

    df = df.withColumn("is_price_shock", F.col("zscore") > 3)

    price_range_window = time_window
    df = df.withColumn("rolling_max_price", F.max("price").over(price_range_window))
    df = df.withColumn("rolling_min_price", F.min("price").over(price_range_window))
    df = df.withColumn("rolling_volume_sum", F.sum("volume").over(price_range_window))
    df = df.withColumn(
        "price_range_pct",
        (F.col("rolling_max_price") - F.col("rolling_min_price")) / F.col("rolling_min_price")
    )
    df = df.withColumn(
        "is_wash_trade",
        (F.coalesce(F.col("rolling_volume_sum"), F.lit(0.0)) > 0) &
        (F.coalesce(F.col("price_range_pct"), F.lit(1.0)) < 0.001) &
        (F.col("volume") > F.coalesce(F.col("rolling_volume_sum"), F.lit(0.0)) * 0.3)
    )

    df = df.withColumn("is_anomaly", F.col("is_price_shock") | F.col("is_wash_trade"))
    df = df.withColumn(
        "anomaly_type",
        F.when(F.col("is_price_shock"), "price_shock")
         .when(F.col("is_wash_trade"), "wash_trade")
         .otherwise(F.lit(None).cast("string"))
    )

    return df.drop(
        "rolling_mean", "rolling_std", "cum_pv", "cum_v",
        "is_price_shock", "is_wash_trade",
        "rolling_max_price", "rolling_min_price", "rolling_volume_sum", "price_range_pct"
    )