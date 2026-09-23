from pyspark.sql import functions as F
from pyspark.sql.window import Window

def detect_anomalies(df):
    window_spec = Window.partitionBy("ticker").orderBy("event_time_ms").rowsBetween(-50, -1)

    df = df.withColumn("rolling_mean", F.avg("price").over(window_spec))
    df = df.withColumn("rolling_std", F.stddev("price").over(window_spec))
    df = df.withColumn(
        "zscore",
        F.when(F.col("rolling_std") > 0, (F.col("price") - F.col("rolling_mean")) / F.col("rolling_std")).otherwise(0.0)
    )

    cum_window = Window.partitionBy("ticker").orderBy("event_time_ms")
    df = df.withColumn("cum_pv", F.sum(F.col("price") * F.col("volume")).over(cum_window))
    df = df.withColumn("cum_v", F.sum("volume").over(cum_window))
    df = df.withColumn("vwap", F.col("cum_pv") / F.col("cum_v"))
    df = df.withColumn("vwap_divergence", F.abs(F.col("price") - F.col("vwap")) / F.col("vwap"))

    df = df.withColumn("is_anomaly", F.col("zscore") > 3)
    df = df.withColumn(
        "anomaly_type",
        F.when(F.col("is_anomaly"), "price_shock").otherwise(F.lit(None).cast("string"))
    )

    return df.drop("rolling_mean", "rolling_std", "cum_pv", "cum_v")