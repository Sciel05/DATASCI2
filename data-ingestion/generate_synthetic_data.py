"""
generate_synthetic_data.py

Generates synthetic high-frequency tick data for multiple tickers, formatted
to look like a realistic HFT feed: Poisson-process arrival times, GBM-driven
price paths, and deliberately injected anomalies so your detection pipeline
has something real to catch.

This is your "market feed" stand-in. Output is a single CSV, globally sorted
by timestamp (interleaved across tickers) - exactly how events would arrive
off a real exchange feed, and exactly what your Kafka producer should replay.

Usage:
    python generate_synthetic_data.py --tickers AAPL MSFT GOOGL TSLA NVDA \
        --duration-sec 300 --avg-rate-per-sec 20 --anomaly-rate 0.01 \
        --out ticks.csv
"""

import argparse
import numpy as np
import pandas as pd


def generate_ticker_stream(ticker, start_price, duration_sec, avg_rate_per_sec,
                            anomaly_rate, rng):
    """
    Generate one ticker's tick stream.

    Price follows a discretized geometric Brownian motion. Volume is drawn
    from a heavy-tailed distribution (lognormal) so occasional large trades
    happen naturally, separate from injected anomalies.

    Anomalies injected, each tagged for later validation of your detector:
      - "price_shock": a sudden jump/drop of 4-8 sigma relative to the
        ticker's recent volatility, decaying back over a few ticks.
      - "wash_trade": a burst of high volume with near-zero net price
        movement, matching the heuristic in your proposal.
    """
    # Poisson-process inter-arrival times -> irregular, realistic tick spacing
    n_expected = int(duration_sec * avg_rate_per_sec * 1.2)  # headroom
    inter_arrivals = rng.exponential(scale=1.0 / avg_rate_per_sec, size=n_expected)
    timestamps = np.cumsum(inter_arrivals)
    timestamps = timestamps[timestamps <= duration_sec]
    n = len(timestamps)

    # GBM price path: daily-vol-equivalent tuned down to per-tick scale
    mu, sigma = 0.0, 0.0008  # per-tick drift/vol, deliberately small
    log_returns = rng.normal(mu, sigma, size=n)
    price_path = start_price * np.exp(np.cumsum(log_returns))

    # Base volume: lognormal, heavy right tail
    volume = rng.lognormal(mean=4.0, sigma=0.8, size=n).round().clip(min=1)

    rows = []
    rolling_sigma = sigma * start_price  # rough absolute-price-scale sigma

    i = 0
    while i < n:
        is_anomaly = rng.random() < anomaly_rate
        if not is_anomaly:
            rows.append({
                "ticker": ticker,
                "timestamp": timestamps[i],
                "price": round(float(price_path[i]), 4),
                "volume": float(volume[i]),
                "side": "buy" if rng.random() > 0.5 else "sell",
                "label": "normal",
            })
            i += 1
            continue

        # Choose anomaly type
        if rng.random() < 0.5:
            # price_shock: jump several sigma, then decay over next few ticks
            shock_mag = rng.uniform(4, 8) * rolling_sigma
            direction = 1 if rng.random() > 0.5 else -1
            shocked_price = price_path[i] + direction * shock_mag
            decay_len = min(5, n - i)
            for j in range(decay_len):
                decay_factor = 1 - (j / decay_len)
                p = price_path[i + j] + direction * shock_mag * decay_factor
                rows.append({
                    "ticker": ticker,
                    "timestamp": timestamps[i + j],
                    "price": round(float(p), 4),
                    "volume": float(volume[i + j]),
                    "side": "buy" if direction > 0 else "sell",
                    "label": "price_shock" if j == 0 else "normal",
                })
            # re-anchor the price path so subsequent ticks continue from here
            price_path[i:] = price_path[i:] - (price_path[i + decay_len - 1] - shocked_price) if decay_len > 0 else price_path[i:]
            i += decay_len
        else:
            # wash_trade: burst of high volume, near-zero net price move
            burst_len = min(rng.integers(4, 10), n - i)
            anchor_price = price_path[i]
            for j in range(burst_len):
                jitter = rng.normal(0, rolling_sigma * 0.05)
                rows.append({
                    "ticker": ticker,
                    "timestamp": timestamps[i + j],
                    "price": round(float(anchor_price + jitter), 4),
                    "volume": float(rng.lognormal(mean=7.0, sigma=0.4)),
                    "side": "buy" if j % 2 == 0 else "sell",
                    "label": "wash_trade" if j == 0 else "normal",
                })
            i += burst_len

    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", nargs="+", default=["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"])
    ap.add_argument("--duration-sec", type=float, default=300,
                     help="Simulated session length in seconds")
    ap.add_argument("--avg-rate-per-sec", type=float, default=20,
                     help="Average ticks/sec PER ticker")
    ap.add_argument("--anomaly-rate", type=float, default=0.01,
                     help="Probability a given tick starts an anomaly event")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="ticks.csv")
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    start_prices = {t: rng.uniform(50, 500) for t in args.tickers}

    all_rows = []
    for ticker in args.tickers:
        all_rows.extend(generate_ticker_stream(
            ticker, start_prices[ticker], args.duration_sec,
            args.avg_rate_per_sec, args.anomaly_rate, rng,
        ))

    df = pd.DataFrame(all_rows)
    df = df.sort_values("timestamp").reset_index(drop=True)
    # timestamp column here is seconds-since-session-start (float);
    # convert to epoch millis anchored to "now" so the producer can pace realistically
    session_start_ms = int(pd.Timestamp.now(tz="UTC").timestamp() * 1000)
    df["event_time_ms"] = session_start_ms + (df["timestamp"] * 1000).astype(np.int64)
    df = df[["ticker", "event_time_ms", "price", "volume", "side", "label"]]

    df.to_csv(args.out, index=False)
    n_anom = (df["label"] != "normal").sum()
    print(f"Wrote {len(df)} ticks across {len(args.tickers)} tickers to {args.out}")
    print(f"  Injected anomalies: {n_anom} ({n_anom / len(df):.2%})")
    print(df["label"].value_counts().to_string())


if __name__ == "__main__":
    main()
